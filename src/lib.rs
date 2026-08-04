//! Fitz Rust Client Library
//!
//! An async Tokio client library for the Fitz event streaming and orchestration platform.
//! Supports both TCP and WebSocket transports with reconnect and restoration.
//!
//! # Quick Start
//!
//! ```ignore
//! use cntryl_fitz::Client;
//!
//! // Connect with an opaque token; the client never parses or stores auth secrets.
//! let client = Client::anonymous("tcp://127.0.0.1:4091").build()?;
//! client.connect().await?;
//!
//! // Routes are opaque strings — the client never parses them.
//! let tx = client.kv()?.begin("kv://my-realm/app/users", TransactionMode::ReadWrite, KvDurability::Buffered).await?;
//! tx.put(b"user:1", b"alice").await?;
//! tx.commit().await?;
//!
//! // Leases
//! let grant = client.lease()?.acquire("lease://my-realm/locks/leader", "node-1", 30).await?;
//! ```

mod async_connection;
pub mod client_domains;
mod codec;
mod domains;
mod error;
mod observability;
mod protocol;

pub use error::error_code;
pub use error::{FitzError, FitzErrorKind, Result};
pub use observability::{
    FitzAttributes, FitzLifecycleEvent, FitzLogger, FitzMeter, FitzObservability, FitzSpan,
    FitzTracer,
};
pub use protocol::TransactionMode;

/// Durability requested for a KV transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum KvDurability {
    Buffered = 0,
    Sync = 1,
}

use async_connection::{AsyncConnection, AsyncConnectionOptions};
use async_trait::async_trait;
use parking_lot::Mutex;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// Options shared by every domain operation.
#[derive(Debug, Clone)]
pub struct OperationOptions {
    pub timeout: Duration,
    pub cancellation: CancellationToken,
}

impl Default for OperationOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            cancellation: CancellationToken::new(),
        }
    }
}

impl OperationOptions {
    /// Runs one domain operation with this call's timeout and cancellation token.
    ///
    /// # Errors
    /// Returns [`FitzError::Timeout`] when the deadline expires or
    /// [`FitzError::Canceled`] when the cancellation token is triggered.
    pub async fn run<T, F>(&self, operation: F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        tokio::select! {
            () = self.cancellation.cancelled() => Err(FitzError::Canceled),
            result = tokio::time::timeout(self.timeout, operation) => {
                result.map_err(|_| FitzError::Timeout)?
            }
        }
    }
}

/// Observable client lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Authenticating,
    Authenticated,
    Reconnecting,
    Closed,
}

/// Exponential reconnect configuration.
#[derive(Debug, Clone)]
pub struct ReconnectPolicy {
    pub enabled: bool,
    pub base_delay: Duration,
    pub multiplier: f64,
    pub maximum_delay: Duration,
    pub maximum_attempts: usize,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            base_delay: Duration::from_millis(100),
            multiplier: 2.0,
            maximum_delay: Duration::from_secs(30),
            maximum_attempts: 0,
        }
    }
}

/// Bounded readiness wait for [`Client::connect_when_ready`].
#[derive(Debug, Clone)]
pub struct ConnectWhenReadyOptions {
    pub timeout: Duration,
    pub initial_delay: Duration,
    pub maximum_delay: Duration,
    pub cancellation: CancellationToken,
}

impl Default for ConnectWhenReadyOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            initial_delay: Duration::from_millis(250),
            maximum_delay: Duration::from_secs(2),
            cancellation: CancellationToken::new(),
        }
    }
}

/// Retry policy for the SDK's narrow replay-safe operation allowlist.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub enabled: bool,
    pub maximum_attempts: usize,
    pub base_delay: Duration,
    pub maximum_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            maximum_attempts: 3,
            base_delay: Duration::from_millis(100),
            maximum_delay: Duration::from_secs(1),
        }
    }
}

/// Idle transport liveness configuration.
#[derive(Debug, Clone)]
pub struct HeartbeatOptions {
    pub enabled: bool,
    pub idle_interval: Duration,
    pub timeout: Duration,
}

impl Default for HeartbeatOptions {
    fn default() -> Self {
        Self {
            enabled: true,
            idle_interval: Duration::from_secs(10),
            timeout: Duration::from_secs(30),
        }
    }
}

/// Supplies an opaque token for every initial connection and reconnect.
#[async_trait]
pub trait TokenProvider: Send + Sync + 'static {
    async fn token(&self) -> Result<String>;
}

#[async_trait]
impl<F, Fut> TokenProvider for F
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<String>> + Send,
{
    async fn token(&self) -> Result<String> {
        (self)().await
    }
}

struct AnonymousTokenProvider;

#[async_trait]
impl TokenProvider for AnonymousTokenProvider {
    async fn token(&self) -> Result<String> {
        Ok(String::new())
    }
}

/// Builder for the async-first [`Client`].
pub struct ClientBuilder {
    endpoint: String,
    token_provider: Arc<dyn TokenProvider>,
    timeout: Duration,
    max_in_flight: usize,
    reconnect: ReconnectPolicy,
    retry: RetryPolicy,
    heartbeat: HeartbeatOptions,
    observability: FitzObservability,
}

impl ClientBuilder {
    #[must_use]
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
    #[must_use]
    pub fn max_in_flight_requests(mut self, limit: usize) -> Self {
        self.max_in_flight = limit.max(1);
        self
    }
    #[must_use]
    pub fn reconnect_policy(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect = policy;
        self
    }
    #[must_use]
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry = policy;
        self
    }
    #[must_use]
    pub fn heartbeat_options(mut self, options: HeartbeatOptions) -> Self {
        self.heartbeat = options;
        self
    }
    #[must_use]
    pub fn observability(mut self, observability: FitzObservability) -> Self {
        self.observability = observability;
        self
    }
    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn build(self) -> Result<Client> {
        let (state_tx, _) = watch::channel(ConnectionState::Disconnected);
        Ok(Client {
            inner: Arc::new(ClientInner {
                endpoint: self.endpoint,
                token_provider: self.token_provider,
                timeout: self.timeout,
                max_in_flight: self.max_in_flight,
                reconnect: self.reconnect,
                retry: self.retry,
                heartbeat: self.heartbeat,
                observability: self.observability,
                state_tx,
                connection: Mutex::new(None),
            }),
        })
    }
}

struct ClientInner {
    endpoint: String,
    token_provider: Arc<dyn TokenProvider>,
    timeout: Duration,
    max_in_flight: usize,
    reconnect: ReconnectPolicy,
    retry: RetryPolicy,
    heartbeat: HeartbeatOptions,
    observability: FitzObservability,
    state_tx: watch::Sender<ConnectionState>,
    connection: Mutex<Option<AsyncConnection>>,
}

/// Async Fitz client entry point.
#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

impl Client {
    #[must_use]
    pub fn builder(
        endpoint: impl Into<String>,
        token_provider: impl TokenProvider,
    ) -> ClientBuilder {
        ClientBuilder {
            endpoint: endpoint.into(),
            token_provider: Arc::new(token_provider),
            timeout: Duration::from_secs(30),
            max_in_flight: 256,
            reconnect: ReconnectPolicy::default(),
            retry: RetryPolicy::default(),
            heartbeat: HeartbeatOptions::default(),
            observability: FitzObservability::default(),
        }
    }
    #[must_use]
    pub fn anonymous(endpoint: impl Into<String>) -> ClientBuilder {
        ClientBuilder {
            endpoint: endpoint.into(),
            token_provider: Arc::new(AnonymousTokenProvider),
            timeout: Duration::from_secs(30),
            max_in_flight: 256,
            reconnect: ReconnectPolicy::default(),
            retry: RetryPolicy::default(),
            heartbeat: HeartbeatOptions::default(),
            observability: FitzObservability::default(),
        }
    }
    #[must_use]
    pub fn state(&self) -> ConnectionState {
        *self.inner.state_tx.borrow()
    }
    #[must_use]
    pub fn subscribe_state(&self) -> watch::Receiver<ConnectionState> {
        self.inner.state_tx.subscribe()
    }
    /// Returns a KV domain handle.
    ///
    /// # Errors
    /// Returns [`FitzError::ConnectionClosed`] until [`Self::connect`] succeeds.
    pub fn kv(&self) -> Result<client_domains::kv::KvClient> {
        Ok(client_domains::kv::KvClient::new(self.connection()?))
    }
    /// Returns a Notice domain handle.
    ///
    /// # Errors
    /// Returns [`FitzError::ConnectionClosed`] until [`Self::connect`] succeeds.
    pub fn notice(&self) -> Result<client_domains::notice::NoticeClient> {
        Ok(client_domains::notice::NoticeClient::new(
            self.connection()?,
        ))
    }
    /// Returns a Queue domain handle.
    ///
    /// # Errors
    /// Returns [`FitzError::ConnectionClosed`] until [`Self::connect`] succeeds.
    pub fn queue(&self) -> Result<client_domains::queue::QueueClient> {
        Ok(client_domains::queue::QueueClient::new(self.connection()?))
    }
    /// Returns a Schedule domain handle.
    ///
    /// # Errors
    /// Returns [`FitzError::ConnectionClosed`] until [`Self::connect`] succeeds.
    pub fn schedule(&self) -> Result<client_domains::schedule::ScheduleClient> {
        Ok(client_domains::schedule::ScheduleClient::new(
            self.connection()?,
        ))
    }
    /// Returns a Lease domain handle.
    ///
    /// # Errors
    /// Returns [`FitzError::ConnectionClosed`] until [`Self::connect`] succeeds.
    pub fn lease(&self) -> Result<client_domains::lease::LeaseClient> {
        Ok(client_domains::lease::LeaseClient::new(self.connection()?))
    }
    /// Returns an RPC domain handle.
    ///
    /// # Errors
    /// Returns [`FitzError::ConnectionClosed`] until [`Self::connect`] succeeds.
    pub fn rpc(&self) -> Result<client_domains::rpc::RpcClient> {
        Ok(client_domains::rpc::RpcClient::new(self.connection()?))
    }
    /// Returns a Stream domain handle.
    ///
    /// # Errors
    /// Returns [`FitzError::ConnectionClosed`] until [`Self::connect`] succeeds.
    pub fn stream(&self) -> Result<client_domains::stream::StreamClient> {
        Ok(client_domains::stream::StreamClient::new(
            self.connection()?,
        ))
    }
    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub async fn connect(&self) -> Result<()> {
        if self.state() == ConnectionState::Closed {
            return Err(FitzError::Closed);
        }
        self.inner
            .state_tx
            .send_replace(ConnectionState::Connecting);
        let connection = {
            let mut slot = self.inner.connection.lock();
            slot.get_or_insert_with(|| {
                AsyncConnection::spawn(AsyncConnectionOptions {
                    endpoint: self.inner.endpoint.clone(),
                    token_provider: Arc::clone(&self.inner.token_provider),
                    timeout: self.inner.timeout,
                    max_queued: self.inner.max_in_flight,
                    reconnect: self.inner.reconnect.clone(),
                    retry: self.inner.retry.clone(),
                    heartbeat: self.inner.heartbeat.clone(),
                    observability: self.inner.observability.clone(),
                    state: self.inner.state_tx.clone(),
                })
            })
            .clone()
        };
        connection.connect().await?;
        tracing::info!(endpoint = %self.inner.endpoint, "fitz client authenticated");
        Ok(())
    }
    /// Waits for startup readiness with bounded exponential backoff.
    ///
    /// # Errors
    /// Returns immediately for authentication failures, cancellation, closure, or timeout.
    pub async fn connect_when_ready(&self, options: ConnectWhenReadyOptions) -> Result<()> {
        let deadline = tokio::time::Instant::now() + options.timeout;
        let mut delay = options.initial_delay;
        loop {
            tokio::select! {
                () = options.cancellation.cancelled() => return Err(FitzError::Canceled),
                result = self.connect() => match result {
                    Ok(()) => return Ok(()),
                    Err(error) if error.is_auth_failure() || matches!(error, FitzError::Closed) => return Err(error),
                    Err(_) => {}
                }
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(FitzError::Timeout);
            }
            tokio::select! {
                () = options.cancellation.cancelled() => return Err(FitzError::Canceled),
                () = tokio::time::sleep(delay.min(remaining)) => {}
            }
            delay = delay.saturating_mul(2).min(options.maximum_delay);
        }
    }
    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub async fn close(&self) -> Result<()> {
        let connection = self.inner.connection.lock().take();
        if let Some(connection) = connection {
            connection.close().await;
        }
        self.inner.state_tx.send_replace(ConnectionState::Closed);
        Ok(())
    }
    #[must_use]
    pub fn reconnect_policy(&self) -> &ReconnectPolicy {
        &self.inner.reconnect
    }

    fn connection(&self) -> Result<AsyncConnection> {
        self.inner
            .connection
            .lock()
            .as_ref()
            .cloned()
            .ok_or(FitzError::ConnectionClosed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn should_bound_individual_operation_given_operation_timeout() {
        let options = OperationOptions {
            timeout: Duration::from_millis(1),
            cancellation: CancellationToken::new(),
        };

        let result = options
            .run(async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok(())
            })
            .await;

        assert!(matches!(result, Err(FitzError::Timeout)));
    }

    #[tokio::test]
    async fn should_cancel_individual_operation_given_cancellation_token() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let options = OperationOptions {
            timeout: Duration::from_secs(1),
            cancellation,
        };

        let result = options.run(std::future::pending::<Result<()>>()).await;

        assert!(matches!(result, Err(FitzError::Canceled)));
    }
}
