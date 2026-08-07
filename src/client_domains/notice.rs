use super::decode_ok;
use crate::async_connection::{AsyncConnection, RestorableRegistration};
use crate::codec::{PayloadDecoder, PayloadEncoder};
use crate::domains::routes::{validate_fixed_route, validate_registration_pattern};
use crate::protocol::message_type;
use crate::{FitzError, Result};
use futures_core::Stream;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio_stream::wrappers::BroadcastStream;

#[derive(Clone)]
pub struct NoticeClient {
    connection: AsyncConnection,
    subscriptions: Arc<tokio::sync::Mutex<HashMap<String, SharedNoticeSubscription>>>,
}

struct SharedNoticeSubscription {
    registration: Arc<RestorableRegistration>,
    references: usize,
}

impl NoticeClient {
    pub(crate) fn new(connection: AsyncConnection) -> Self {
        Self {
            connection,
            subscriptions: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn publish(&self, route: &str, body: &[u8]) -> Result<()> {
        validate_fixed_route(route, "notice", 3)?;
        let mut encoder = PayloadEncoder::new();
        encoder
            .put_string(route)
            .put_u32(u32::try_from(body.len()).map_err(|_| FitzError::FrameTooLarge(body.len()))?)
            .put_raw(body);
        self.connection
            .send(message_type::NOTICE_PUBLISH, encoder.finish())
            .await
    }

    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn subscribe(&self, pattern: &str) -> Result<NoticeSubscription> {
        validate_registration_pattern(pattern, "notice", 0)?;
        let receiver = self
            .connection
            .notifications(message_type::NOTICE_NOTIFY, 64);
        let mut subscriptions = self.subscriptions.lock().await;
        let registration = if let Some(shared) = subscriptions.get_mut(pattern) {
            shared.references += 1;
            Arc::clone(&shared.registration)
        } else {
            let mut encoder = PayloadEncoder::new();
            encoder.put_string(pattern);
            let payload = encoder.finish();
            let response = self
                .connection
                .request(message_type::NOTICE_SUBSCRIBE, payload.clone())
                .await?;
            let subscription_id = decode_subscription_id(&response)?;
            let registration = Arc::new(self.connection.register_restorable(
                message_type::NOTICE_SUBSCRIBE,
                payload,
                subscription_id,
                decode_subscription_id,
            ));
            subscriptions.insert(
                pattern.into(),
                SharedNoticeSubscription {
                    registration: Arc::clone(&registration),
                    references: 1,
                },
            );
            registration
        };
        drop(subscriptions);
        Ok(NoticeSubscription {
            connection: self.connection.clone(),
            subscriptions: Arc::clone(&self.subscriptions),
            pattern: pattern.into(),
            registration,
            receiver: BroadcastStream::new(receiver),
            closed: false,
        })
    }
}

fn decode_subscription_id(response: &[u8]) -> Result<u64> {
    let mut decoder = PayloadDecoder::new(response);
    match decoder.get_u8()? {
        0 => {}
        1 => {
            return Err(FitzError::Domain {
                code: decoder.get_u32()?,
                message: decoder.get_string()?,
            });
        }
        status => {
            return Err(FitzError::Protocol(format!(
                "NOTICE SUBSCRIBE returned status {status}"
            )));
        }
    }
    if decoder.remaining() == 9 && decoder.get_u8()? != 1 {
        return Err(FitzError::Protocol(
            "NOTICE SUBSCRIBE response omitted subscription id".into(),
        ));
    }
    decoder.get_u64()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoticeMessage {
    pub route: String,
    pub body: Vec<u8>,
}

pub struct NoticeSubscription {
    connection: AsyncConnection,
    subscriptions: Arc<tokio::sync::Mutex<HashMap<String, SharedNoticeSubscription>>>,
    pattern: String,
    registration: Arc<RestorableRegistration>,
    receiver: BroadcastStream<Vec<u8>>,
    closed: bool,
}

impl NoticeSubscription {
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn unsubscribe(mut self) -> Result<()> {
        self.closed = true;
        let mut subscriptions = self.subscriptions.lock().await;
        let Some(shared) = subscriptions.get_mut(&self.pattern) else {
            return Ok(());
        };
        if shared.references > 1 {
            shared.references -= 1;
            return Ok(());
        }
        let subscription_id = self.registration.wire_id();
        let mut encoder = PayloadEncoder::new();
        encoder.put_u64(subscription_id);
        decode_ok(
            &self
                .connection
                .request(message_type::NOTICE_UNSUBSCRIBE, encoder.finish())
                .await?,
        )?;
        self.registration.deactivate();
        subscriptions.remove(&self.pattern);
        Ok(())
    }
}

impl Stream for NoticeSubscription {
    type Item = Result<NoticeMessage>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.closed {
            return Poll::Ready(None);
        }
        loop {
            match Pin::new(&mut self.receiver).poll_next(context) {
                Poll::Ready(Some(Ok(payload))) => {
                    let mut decoder = PayloadDecoder::new(&payload);
                    let subscription_id = match decoder.get_u64() {
                        Ok(value) => value,
                        Err(error) => return Poll::Ready(Some(Err(error))),
                    };
                    if subscription_id != self.registration.wire_id() {
                        continue;
                    }
                    let route = match decoder.get_string() {
                        Ok(value) => value,
                        Err(error) => return Poll::Ready(Some(Err(error))),
                    };
                    let body = match decoder.get_bytes() {
                        Ok(value) => value,
                        Err(error) => return Poll::Ready(Some(Err(error))),
                    };
                    return Poll::Ready(Some(Ok(NoticeMessage { route, body })));
                }
                Poll::Ready(Some(Err(_))) => {
                    return Poll::Ready(Some(Err(FitzError::Backpressure(
                        "Notice subscription buffer is full".into(),
                    ))));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::async_connection::AsyncConnectionOptions;
    use crate::{
        ConnectionState, FitzObservability, HeartbeatOptions, ReconnectPolicy, RetryPolicy,
    };
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::watch;

    async fn read_frame(stream: &mut TcpStream) -> (u16, Vec<u8>) {
        let len = stream.read_u32().await.unwrap() as usize;
        let mut frame = vec![0; len];
        stream.read_exact(&mut frame).await.unwrap();
        let (kind, start) = crate::codec::decode_message_frame(&frame).unwrap();
        (kind, frame[start..].to_vec())
    }

    async fn write_frame(stream: &mut TcpStream, kind: u16, payload: &[u8]) {
        let frame = crate::codec::try_encode_message_frame(kind, payload).unwrap();
        stream
            .write_u32(u32::try_from(frame.len()).unwrap())
            .await
            .unwrap();
        stream.write_all(&frame).await.unwrap();
        stream.flush().await.unwrap();
    }

    #[tokio::test]
    async fn should_reference_count_duplicate_notice_subscriptions() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            assert_eq!(read_frame(&mut stream).await.0, message_type::CONNECT);
            let (subscribe, _) = read_frame(&mut stream).await;
            assert_eq!(subscribe, message_type::NOTICE_SUBSCRIBE);
            let mut response = PayloadEncoder::new();
            response.put_u8(0).put_u64(42);
            write_frame(
                &mut stream,
                message_type::NOTICE_SUBSCRIBE,
                &response.finish(),
            )
            .await;
            let (unsubscribe, _) = read_frame(&mut stream).await;
            assert_eq!(unsubscribe, message_type::NOTICE_UNSUBSCRIBE);
            write_frame(&mut stream, message_type::NOTICE_UNSUBSCRIBE, &[0]).await;
        });
        let (state, _) = watch::channel(ConnectionState::Disconnected);
        let connection = AsyncConnection::spawn(AsyncConnectionOptions {
            endpoint: format!("tcp://{address}"),
            token_provider: Arc::new(|| async { Ok(String::new()) }),
            timeout: Duration::from_secs(1),
            max_queued: 8,
            reconnect: ReconnectPolicy {
                enabled: false,
                ..ReconnectPolicy::default()
            },
            retry: RetryPolicy::default(),
            heartbeat: HeartbeatOptions::default(),
            observability: FitzObservability::default(),
            state,
        });
        connection.connect().await.unwrap();
        let client = NoticeClient::new(connection.clone());
        let first = client.subscribe("notice://realm/area/*").await.unwrap();
        let second = client.subscribe("notice://realm/area/*").await.unwrap();
        first.unsubscribe().await.unwrap();
        second.unsubscribe().await.unwrap();
        server.await.unwrap();
        connection.close().await;
    }
}
