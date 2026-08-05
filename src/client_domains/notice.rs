use super::decode_ok;
use crate::async_connection::{AsyncConnection, RestorableRegistration};
use crate::codec::{PayloadDecoder, PayloadEncoder};
use crate::domains::routes::{validate_fixed_route, validate_registration_pattern};
use crate::protocol::message_type;
use crate::{FitzError, Result};
use futures_core::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_stream::wrappers::BroadcastStream;

#[derive(Clone)]
pub struct NoticeClient {
    connection: AsyncConnection,
}

impl NoticeClient {
    pub(crate) fn new(connection: AsyncConnection) -> Self {
        Self { connection }
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
        let mut encoder = PayloadEncoder::new();
        encoder.put_string(pattern);
        let receiver = self
            .connection
            .notifications(message_type::NOTICE_NOTIFY, 64);
        let payload = encoder.finish();
        let response = self
            .connection
            .request(message_type::NOTICE_SUBSCRIBE, payload.clone())
            .await?;
        let subscription_id = decode_subscription_id(&response)?;
        let registration = self.connection.register_restorable(
            message_type::NOTICE_SUBSCRIBE,
            payload,
            subscription_id,
            decode_subscription_id,
        );
        Ok(NoticeSubscription {
            connection: self.connection.clone(),
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
    registration: RestorableRegistration,
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
        let subscription_id = self.registration.wire_id();
        self.registration.deactivate();
        let mut encoder = PayloadEncoder::new();
        encoder.put_u64(subscription_id);
        decode_ok(
            &self
                .connection
                .request(message_type::NOTICE_UNSUBSCRIBE, encoder.finish())
                .await?,
        )
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
