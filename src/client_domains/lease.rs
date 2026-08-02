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
use std::task::{Context, Poll};
use std::time::Duration;
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, Default)]
pub struct LeaseExecutionOptions {
    pub wait_for_availability: bool,
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
}
impl LeaseClient {
    pub(crate) fn new(connection: AsyncConnection) -> Self {
        Self { connection }
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn acquire(&self, route: &str, owner_id: &str, ttl_secs: u64) -> Result<LeaseHandle> {
        validate_fixed_route(route, "lease", 3)?;
        let mut e = PayloadEncoder::new();
        e.put_string(route).put_string(owner_id).put_u64(ttl_secs);
        let response = self
            .connection
            .request(message_type::LEASE_ACQUIRE, e.finish())
            .await?;
        let mut d = success(&response, "ACQUIRE")?;
        let kind = d.get_u8()?;
        if kind >= 2 {
            return Err(FitzError::Domain {
                code: 5001,
                message: "lease acquisition queued".into(),
            });
        }
        Ok(LeaseHandle {
            connection: self.connection.clone(),
            generation: self.connection.generation(),
            route: route.into(),
            owner_id: owner_id.into(),
            fencing_token: d.get_u64()?,
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
        let mut d = success(&response, "QUERY")?;
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
        let mut delay = Duration::from_millis(50);
        let mut handle = loop {
            match self.acquire(route, owner_id, ttl_secs).await {
                Ok(handle) => break handle,
                Err(error) if options.wait_for_availability && is_contention(&error) => {
                    tokio::time::sleep(delay).await;
                    delay = delay.saturating_mul(2).min(Duration::from_secs(1));
                }
                Err(error) => return Err(LeaseExecutionError::Acquisition(error)),
            }
        };

        let cancellation = CancellationToken::new();
        let callback_task =
            tokio::spawn(AssertUnwindSafe(callback(cancellation.clone())).catch_unwind());
        tokio::pin!(callback_task);
        let renewal = tokio::time::sleep(Duration::from_secs(ttl_secs) / 3);
        tokio::pin!(renewal);
        loop {
            tokio::select! {
                biased;
                callback_result = &mut callback_task => {
                    let outcome = callback_result.map_err(|error| LeaseExecutionError::Acquisition(FitzError::Connection(error.to_string())))?;
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
                            let outcome = callback_task.await.map_err(|join| LeaseExecutionError::OwnershipLost(FitzError::Connection(join.to_string())))?;
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

fn is_contention(error: &FitzError) -> bool {
    matches!(error, FitzError::Domain { code: 5001, .. })
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
        let mut d = success(&response, "RENEW")?;
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
        success(
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
        success(
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
    success(response, "SUBSCRIBE")?.get_u64()
}
fn success<'a>(response: &'a [u8], operation: &str) -> Result<PayloadDecoder<'a>> {
    let mut d = PayloadDecoder::new(response);
    match d.get_u8()? {
        0 => Ok(d),
        1 => Err(FitzError::Domain {
            code: d.get_u32()?,
            message: d.get_string()?,
        }),
        v => Err(FitzError::Protocol(format!(
            "Lease {operation} returned status {v}"
        ))),
    }
}
