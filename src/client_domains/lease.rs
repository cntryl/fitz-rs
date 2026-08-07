use crate::async_connection::{AsyncConnection, RestorableRegistration};
use crate::codec::{PayloadDecoder, PayloadEncoder};
use crate::domains::routes::validate_fixed_route;
use crate::protocol::message_type;
use crate::{FitzError, Result};
use futures_core::Stream;
use futures_util::FutureExt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, Default)]
pub struct LeaseExecutionOptions {
    pub wait_seconds: u32,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LeaseAcquireOptions {
    pub wait_seconds: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum LeaseExecutionError<E> {
    #[error("lease acquisition failed: {0}")]
    Acquisition(FitzError),
    #[error("lease callback failed")]
    Callback(E),
    #[error("lease ownership lost: {0}")]
    OwnershipLost(FitzError),
    #[error("lease release failed: {0}")]
    Release(FitzError),
    #[error("lease lifecycle and callback both failed")]
    Combined { lifecycle: FitzError, callback: E },
}
#[derive(Clone)]
pub struct LeaseClient {
    connection: AsyncConnection,
    acquisition_gate: Arc<tokio::sync::Mutex<()>>,
}
impl LeaseClient {
    pub(crate) fn new(connection: AsyncConnection) -> Self {
        Self {
            connection,
            acquisition_gate: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn acquire(
        &self,
        route: &str,
        owner_id: &str,
        ttl_secs: u64,
        options: LeaseAcquireOptions,
    ) -> Result<LeaseHandle> {
        validate_fixed_route(route, "lease", 3)?;
        let _acquisition_guard = self.acquisition_gate.lock().await;
        let mut deferred = self
            .connection
            .notifications(message_type::LEASE_ACQUIRE, 16);
        let mut e = PayloadEncoder::new();
        e.put_string(route)
            .put_string(owner_id)
            .put_u64(ttl_secs)
            .put_u32(options.wait_seconds);
        let response = self
            .connection
            .request(message_type::LEASE_ACQUIRE, e.finish())
            .await?;
        let mut d = lease_success(&response, "ACQUIRE")?;
        let kind = d.get_u8()?;
        let fencing_token = if kind < 2 {
            d.get_u64()?
        } else if kind <= 3 && options.wait_seconds > 0 {
            let payload = deferred
                .recv()
                .await
                .map_err(|_| FitzError::ConnectionClosed)?;
            let mut completion = lease_success(&payload, "ACQUIRE")?;
            let completion_kind = completion.get_u8()?;
            if completion_kind > 1 {
                return Err(FitzError::Protocol(
                    "deferred ACQUIRE remained queued".into(),
                ));
            }
            completion.get_u64()?
        } else {
            return Err(FitzError::Domain {
                code: 5001,
                message: "lease acquisition queued".into(),
            });
        };
        Ok(LeaseHandle {
            connection: self.connection.clone(),
            generation: self.connection.generation(),
            route: route.into(),
            owner_id: owner_id.into(),
            fencing_token,
            released: false,
        })
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn query(&self, route: &str) -> Result<LeaseInfo> {
        validate_fixed_route(route, "lease", 3)?;
        let mut e = PayloadEncoder::new();
        e.put_string(route);
        let response = self
            .connection
            .request_replayable(message_type::LEASE_QUERY, e.finish())
            .await?;
        let mut d = lease_success(&response, "QUERY")?;
        let held = d.get_u8()? == 1;
        if held {
            Ok(LeaseInfo {
                held,
                owner_id: Some(d.get_string()?),
                ttl_remaining_secs: Some(d.get_u64()?),
                pending_waiters: d.get_u32()?,
            })
        } else {
            Ok(LeaseInfo {
                held,
                owner_id: None,
                ttl_remaining_secs: None,
                pending_waiters: d.get_u32()?,
            })
        }
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn subscribe(&self, route: &str) -> Result<LeaseSubscription> {
        validate_fixed_route(route, "lease", 3)?;
        let receiver = self
            .connection
            .notifications(message_type::LEASE_NOTIFY, 64);
        let mut e = PayloadEncoder::new();
        e.put_string(route);
        let payload = e.finish();
        let response = self
            .connection
            .request(message_type::LEASE_SUBSCRIBE, payload.clone())
            .await?;
        let subscription_id = decode_subscription_id(&response)?;
        let registration = self.connection.register_restorable(
            message_type::LEASE_SUBSCRIBE,
            payload,
            subscription_id,
            decode_subscription_id,
        );
        Ok(LeaseSubscription {
            connection: self.connection.clone(),
            route: route.into(),
            registration,
            receiver: BroadcastStream::new(receiver),
            closed: false,
        })
    }

    /// Runs a callback while the lease is owned and supervised.
    ///
    /// # Errors
    /// Returns typed acquisition, callback, ownership-loss, release, or combined failures.
    pub async fn with_lease<F, Fut, T, E>(
        &self,
        route: &str,
        owner_id: &str,
        ttl_secs: u64,
        callback: F,
    ) -> std::result::Result<T, LeaseExecutionError<E>>
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.with_lease_with_options(
            route,
            owner_id,
            ttl_secs,
            callback,
            LeaseExecutionOptions::default(),
        )
        .await
    }

    /// Runs a callback while the lease is owned with explicit acquisition behavior.
    ///
    /// # Errors
    /// Returns typed acquisition, callback, ownership-loss, release, or combined failures.
    pub async fn with_lease_with_options<F, Fut, T, E>(
        &self,
        route: &str,
        owner_id: &str,
        ttl_secs: u64,
        callback: F,
        options: LeaseExecutionOptions,
    ) -> std::result::Result<T, LeaseExecutionError<E>>
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        validate_fixed_route(route, "lease", 3).map_err(LeaseExecutionError::Acquisition)?;
        if ttl_secs == 0 || ttl_secs > u64::from(u32::MAX) / 1_000 {
            return Err(LeaseExecutionError::Acquisition(FitzError::Protocol(
                "lease TTL must be positive and schedulable".into(),
            )));
        }
        let mut handle = self
            .acquire(
                route,
                owner_id,
                ttl_secs,
                LeaseAcquireOptions {
                    wait_seconds: options.wait_seconds,
                },
            )
            .await
            .map_err(LeaseExecutionError::Acquisition)?;

        let cancellation = CancellationToken::new();
        let callback_task = AssertUnwindSafe(callback(cancellation.clone())).catch_unwind();
        tokio::pin!(callback_task);
        let renewal = tokio::time::sleep(Duration::from_secs(ttl_secs) / 3);
        tokio::pin!(renewal);
        loop {
            tokio::select! {
                biased;
                callback_result = &mut callback_task => {
                    let outcome = callback_result;
                    let release_result = handle.release().await;
                    let callback_result = match outcome {
                        Ok(result) => result,
                        Err(panic) => std::panic::resume_unwind(panic),
                    };
                    return match (callback_result, release_result) {
                        (Ok(value), Ok(())) => Ok(value),
                        (Err(error), Ok(())) => Err(LeaseExecutionError::Callback(error)),
                        (Ok(_), Err(error)) => Err(LeaseExecutionError::Release(error)),
                        (Err(callback), Err(lifecycle)) => Err(LeaseExecutionError::Combined { lifecycle, callback }),
                    };
                }
                () = &mut renewal => {
                    match handle.extend(ttl_secs).await {
                        Ok(()) => renewal.as_mut().reset(tokio::time::Instant::now() + Duration::from_secs(ttl_secs) / 3),
                        Err(error) => {
                            cancellation.cancel();
                            let outcome = callback_task.await;
                            let callback_result = match outcome {
                                Ok(result) => result,
                                Err(panic) => std::panic::resume_unwind(panic),
                            };
                            return match callback_result {
                                Ok(_) => Err(LeaseExecutionError::OwnershipLost(error)),
                                Err(callback) => Err(LeaseExecutionError::Combined { lifecycle: error, callback }),
                            };
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseInfo {
    pub held: bool,
    pub owner_id: Option<String>,
    pub ttl_remaining_secs: Option<u64>,
    pub pending_waiters: u32,
}
pub struct LeaseHandle {
    connection: AsyncConnection,
    generation: u64,
    route: String,
    owner_id: String,
    fencing_token: u64,
    released: bool,
}
impl LeaseHandle {
    fn current(&self) -> Result<()> {
        if self.released || self.connection.generation() != self.generation {
            Err(FitzError::StaleHandle)
        } else {
            Ok(())
        }
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn extend(&mut self, ttl_secs: u64) -> Result<()> {
        self.current()?;
        let mut e = PayloadEncoder::new();
        e.put_string(&self.route)
            .put_string(&self.owner_id)
            .put_u64(self.fencing_token)
            .put_u64(ttl_secs);
        let response = self
            .connection
            .request(message_type::LEASE_RENEW, e.finish())
            .await?;
        let mut d = lease_success(&response, "RENEW")?;
        self.fencing_token = d.get_u64()?;
        Ok(())
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn release(mut self) -> Result<()> {
        self.current()?;
        let mut e = PayloadEncoder::new();
        e.put_string(&self.route)
            .put_string(&self.owner_id)
            .put_u64(self.fencing_token);
        lease_success(
            &self
                .connection
                .request(message_type::LEASE_RELEASE, e.finish())
                .await?,
            "RELEASE",
        )?;
        self.released = true;
        Ok(())
    }
}

impl Drop for LeaseHandle {
    fn drop(&mut self) {
        if self.released || self.connection.generation() != self.generation {
            return;
        }
        self.released = true;
        let connection = self.connection.clone();
        let route = self.route.clone();
        let owner_id = self.owner_id.clone();
        let fencing_token = self.fencing_token;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let mut e = PayloadEncoder::new();
                e.put_string(&route)
                    .put_string(&owner_id)
                    .put_u64(fencing_token);
                let _ = connection
                    .request(message_type::LEASE_RELEASE, e.finish())
                    .await;
            });
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseChangeNotification {
    pub route: String,
}
pub struct LeaseSubscription {
    connection: AsyncConnection,
    route: String,
    registration: RestorableRegistration,
    receiver: BroadcastStream<Vec<u8>>,
    closed: bool,
}
impl LeaseSubscription {
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn unsubscribe(mut self) -> Result<()> {
        self.closed = true;
        self.registration.deactivate();
        let mut e = PayloadEncoder::new();
        e.put_string(&self.route);
        lease_success(
            &self
                .connection
                .request(message_type::LEASE_UNSUBSCRIBE, e.finish())
                .await?,
            "UNSUBSCRIBE",
        )?;
        Ok(())
    }
}
impl Stream for LeaseSubscription {
    type Item = Result<LeaseChangeNotification>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.closed {
            return Poll::Ready(None);
        }
        loop {
            match Pin::new(&mut self.receiver).poll_next(cx) {
                Poll::Ready(Some(Ok(payload))) => {
                    let mut d = PayloadDecoder::new(&payload);
                    let id = match d.get_u64() {
                        Ok(v) => v,
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    };
                    if id != self.registration.wire_id() {
                        continue;
                    }
                    return Poll::Ready(Some(
                        d.get_string()
                            .map(|route| LeaseChangeNotification { route }),
                    ));
                }
                Poll::Ready(Some(Err(_))) => {
                    return Poll::Ready(Some(Err(FitzError::Backpressure(
                        "Lease subscription buffer is full".into(),
                    ))));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

fn decode_subscription_id(response: &[u8]) -> Result<u64> {
    let mut decoder = lease_success(response, "SUBSCRIBE")?;
    let subscription_id = decoder.get_u64()?;
    if !decoder.is_empty() {
        return Err(FitzError::Protocol(
            "lease SUBSCRIBE response has trailing bytes".into(),
        ));
    }
    Ok(subscription_id)
}
fn lease_success<'a>(response: &'a [u8], operation: &str) -> Result<PayloadDecoder<'a>> {
    let mut d = PayloadDecoder::new(response);
    match d.get_u8()? {
        0 => Ok(d),
        1 => {
            let code = d.get_u32()?;
            let message = d.get_string()?;
            if !d.is_empty() {
                return Err(FitzError::Protocol(format!(
                    "Lease {operation} error response has trailing bytes"
                )));
            }
            Err(FitzError::Domain { code, message })
        }
        v => Err(FitzError::Protocol(format!(
            "Lease {operation} returned status {v}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::async_connection::AsyncConnectionOptions;
    use crate::{
        ConnectionState, FitzObservability, HeartbeatOptions, ReconnectPolicy, RetryPolicy,
    };
    use std::sync::Arc;
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

    #[test]
    fn should_preserve_domain_code_given_typed_error_when_decoding_lease_response() {
        // Arrange: build the server's canonical typed Lease error envelope.
        let mut response = PayloadEncoder::new();
        response
            .put_u8(1)
            .put_u32(5001)
            .put_string("HeldByOther: worker-1");

        // Act: decode the response through the shared Lease status parser.
        let Err(error) = lease_success(&response.finish(), "ACQUIRE") else {
            panic!("typed Lease error decoded as success");
        };

        // Assert: retain the numeric domain code used by retry and callers.
        assert!(matches!(error, FitzError::Domain { code: 5001, .. }));
    }

    #[tokio::test]
    async fn should_complete_deferred_acquire_and_release_when_handle_dropped() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            assert_eq!(read_frame(&mut stream).await.0, message_type::CONNECT);
            let (kind, payload) = read_frame(&mut stream).await;
            assert_eq!(kind, message_type::LEASE_ACQUIRE);
            assert_eq!(
                u32::from_be_bytes(payload[payload.len() - 4..].try_into().unwrap()),
                7
            );
            let mut queued = PayloadEncoder::new();
            queued.put_u8(0).put_u8(2).put_u64(0);
            write_frame(&mut stream, message_type::LEASE_ACQUIRE, &queued.finish()).await;
            let mut acquired = PayloadEncoder::new();
            acquired.put_u8(0).put_u8(0).put_u64(42);
            write_frame(&mut stream, message_type::LEASE_ACQUIRE, &acquired.finish()).await;
            assert_eq!(read_frame(&mut stream).await.0, message_type::LEASE_RELEASE);
            write_frame(&mut stream, message_type::LEASE_RELEASE, &[0]).await;
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
        let client = LeaseClient::new(connection.clone());
        let handle = client
            .acquire(
                "lease://realm/area/resource",
                "worker-1",
                30,
                LeaseAcquireOptions { wait_seconds: 7 },
            )
            .await
            .unwrap();
        drop(handle);
        server.await.unwrap();
        connection.close().await;
    }
}
