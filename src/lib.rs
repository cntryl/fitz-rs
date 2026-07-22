//! Fitz Rust Client Library
//!
//! A synchronous client library for the Fitz event streaming and orchestration platform.
//! Supports both TCP and WebSocket transports.
//!
//! # Quick Start
//!
//! ```ignore
//! use cntryl::FitzClient;
//!
//! // Connect with an opaque token; the client never parses or stores auth secrets.
//! let client = FitzClient::connect_tcp("127.0.0.1", 4091, "opaque-token")?;
//!
//! // Routes are opaque strings — the client never parses them.
//! let tx = client.kv().begin("kv://my-realm/app/users", TransactionMode::ReadWrite)?;
//! tx.put(b"user:1", b"alice")?;
//! tx.commit()?;
//!
//! // Leases
//! let grant = client.lease().acquire("lease://my-realm/locks/leader", "node-1", 30)?;
//! ```

#[cfg(test)]
mod auth;
mod codec;
mod connection;
pub mod domains;
mod error;
mod protocol;
mod transport;

pub use error::{FitzError, FitzErrorKind, Result};
pub use protocol::TransactionMode;

use async_trait::async_trait;
use connection::{FitzConnection, SharedConnection};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex as AsyncMutex, watch};
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
            maximum_attempts: 10,
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
                state_tx,
                connection: AsyncMutex::new(None),
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
    state_tx: watch::Sender<ConnectionState>,
    connection: AsyncMutex<Option<FitzClient>>,
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
        let token = self.inner.token_provider.token().await?;
        self.inner.state_tx.send_replace(ConnectionState::Connected);
        self.inner
            .state_tx
            .send_replace(ConnectionState::Authenticating);
        let endpoint = self.inner.endpoint.clone();
        let timeout = self.inner.timeout;
        let max_in_flight = self.inner.max_in_flight;
        let client = tokio::task::spawn_blocking(move || {
            connect_endpoint(&endpoint, &token, timeout, max_in_flight)
        })
        .await
        .map_err(|error| FitzError::Connection(error.to_string()))??;
        *self.inner.connection.lock().await = Some(client);
        self.inner
            .state_tx
            .send_replace(ConnectionState::Authenticated);
        tracing::info!(endpoint = %self.inner.endpoint, "fitz client authenticated");
        Ok(())
    }
    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub async fn close(&self) -> Result<()> {
        let connection = self.inner.connection.lock().await.take();
        if let Some(connection) = connection {
            tokio::task::spawn_blocking(move || connection.close())
                .await
                .map_err(|error| FitzError::Connection(error.to_string()))??;
        }
        self.inner.state_tx.send_replace(ConnectionState::Closed);
        Ok(())
    }
    #[must_use]
    pub fn reconnect_policy(&self) -> &ReconnectPolicy {
        &self.inner.reconnect
    }
}

fn connect_endpoint(
    endpoint: &str,
    token: &str,
    timeout: Duration,
    max_in_flight: usize,
) -> Result<FitzClient> {
    let builder = FitzClient::builder(token)
        .with_timeout(timeout)
        .with_max_in_flight_requests(max_in_flight);
    if let Some(address) = endpoint.strip_prefix("tcp://") {
        let (host, port) = address
            .rsplit_once(':')
            .ok_or_else(|| FitzError::Connection("TCP endpoint must include a port".into()))?;
        return builder.connect_tcp(
            host,
            port.parse()
                .map_err(|_| FitzError::Connection("invalid TCP port".into()))?,
        );
    }
    if endpoint.starts_with("ws://") || endpoint.starts_with("wss://") {
        return builder.connect_ws(endpoint);
    }
    Err(FitzError::Connection(
        "endpoint must use tcp://, ws://, or wss://".into(),
    ))
}

enum ClientAuth {
    Token(String),
    Anonymous,
}

/// Builder for creating Fitz clients with flexible configuration.
pub struct FitzClientBuilder {
    auth: ClientAuth,
    timeout: Duration,
    max_in_flight_requests: usize,
}

impl FitzClientBuilder {
    #[must_use]
    pub fn new(token: &str) -> Self {
        Self {
            auth: ClientAuth::Token(token.to_string()),
            timeout: Duration::from_secs(30),
            max_in_flight_requests: 256,
        }
    }

    #[must_use]
    pub fn anonymous() -> Self {
        Self {
            auth: ClientAuth::Anonymous,
            timeout: Duration::from_secs(30),
            max_in_flight_requests: 256,
        }
    }

    /// Set connection timeout.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the maximum number of in-flight requests allowed on this client.
    #[must_use]
    pub fn with_max_in_flight_requests(mut self, max_in_flight_requests: usize) -> Self {
        self.max_in_flight_requests = max_in_flight_requests.max(1);
        self
    }

    /// Connect via TCP.
    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn connect_tcp(self, host: &str, port: u16) -> Result<FitzClient> {
        let conn = FitzConnection::connect_tcp(host, port)?;
        self.finish(conn)
    }

    /// Connect via WebSocket.
    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn connect_ws(self, url: &str) -> Result<FitzClient> {
        let conn = FitzConnection::connect_ws(url)?;
        self.finish(conn)
    }

    fn finish(self, mut conn: FitzConnection) -> Result<FitzClient> {
        conn.set_timeout(self.timeout)?;
        let shared = SharedConnection::new(conn, self.max_in_flight_requests);

        // Send the caller-provided token directly. Token generation is owned by
        // the application or tests, not the SDK.
        let token = match self.auth {
            ClientAuth::Token(token) => token,
            ClientAuth::Anonymous => String::new(),
        };
        shared.send_only(protocol::message_type::CONNECT, token.as_bytes())?;

        Ok(FitzClient { connection: shared })
    }
}

/// Main Fitz client — the single entry point for all domains.
///
/// Create one per connection. Call `.kv()`, `.lease()`, etc. to get
/// domain-specific handles (they share the underlying connection).
///
/// The client is intentionally realm-agnostic — realm context lives in
/// the auth token sent during CONNECT, and route strings carry the addressing.
pub struct FitzClient {
    connection: SharedConnection,
}

impl FitzClient {
    /// Create a builder.
    #[must_use]
    pub fn builder(token: &str) -> FitzClientBuilder {
        FitzClientBuilder::new(token)
    }

    /// Create a builder for anonymous connections.
    #[must_use]
    pub fn builder_anonymous() -> FitzClientBuilder {
        FitzClientBuilder::anonymous()
    }

    /// Convenient helper: connect via TCP.
    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn connect_tcp(host: &str, port: u16, token: &str) -> Result<Self> {
        FitzClient::builder(token).connect_tcp(host, port)
    }

    /// Convenient helper: connect via TCP without a JWT.
    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn connect_tcp_anonymous(host: &str, port: u16) -> Result<Self> {
        FitzClient::builder_anonymous().connect_tcp(host, port)
    }

    /// Convenient helper: connect via WebSocket.
    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn connect_ws(url: &str, token: &str) -> Result<Self> {
        FitzClient::builder(token).connect_ws(url)
    }

    /// Convenient helper: connect via WebSocket without a JWT.
    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn connect_ws_anonymous(url: &str) -> Result<Self> {
        FitzClient::builder_anonymous().connect_ws(url)
    }

    /// Get a KV domain client.
    #[must_use]
    pub fn kv(&self) -> domains::kv::KvClient {
        domains::kv::KvClient::new(self.connection.clone())
    }

    /// Get a Lease domain client.
    #[must_use]
    pub fn lease(&self) -> domains::lease::LeaseClient {
        domains::lease::LeaseClient::new(self.connection.clone())
    }

    /// Get a Queue domain client.
    #[must_use]
    pub fn queue(&self) -> domains::queue::QueueClient {
        domains::queue::QueueClient::new(self.connection.clone())
    }

    /// Get a Notice domain client.
    #[must_use]
    pub fn notice(&self) -> domains::notice::NoticeClient {
        domains::notice::NoticeClient::new(self.connection.clone())
    }

    /// Get a Schedule domain client.
    #[must_use]
    pub fn schedule(&self) -> domains::schedule::ScheduleClient {
        domains::schedule::ScheduleClient::new(self.connection.clone())
    }

    /// Get an RPC domain client.
    #[must_use]
    pub fn rpc(&self) -> domains::rpc::RpcClient {
        domains::rpc::RpcClient::new(self.connection.clone())
    }

    /// Get a Stream domain client.
    #[must_use]
    pub fn stream(&self) -> domains::stream::StreamClient {
        domains::stream::StreamClient::new(self.connection.clone())
    }

    /// Close the connection.
    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn close(&self) -> Result<()> {
        self.connection.close()
    }

    /// Update the default transport timeout used by subsequent requests.
    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn set_timeout(&self, timeout: Duration) -> Result<()> {
        self.connection.set_timeout(timeout)
    }

    /// Return the current default transport timeout.
    #[must_use]
    pub fn timeout(&self) -> Option<Duration> {
        self.connection.timeout()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{decode_message_frame, encode_message_frame};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::thread;

    fn read_length_prefixed_frame(stream: &mut std::net::TcpStream) -> Vec<u8> {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).unwrap();
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut frame = vec![0u8; len];
        stream.read_exact(&mut frame).unwrap();
        frame
    }

    #[test]
    fn should_create_client_builder() {
        let _builder = FitzClient::builder("secret");
    }

    #[test]
    fn should_create_anonymous_builder() {
        let _builder = FitzClient::builder_anonymous();
    }

    #[test]
    fn should_update_client_timeout_after_connect() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            read_length_prefixed_frame(&mut socket);
            thread::sleep(Duration::from_millis(50));
        });

        let client = FitzClient::builder("secret")
            .with_timeout(Duration::from_millis(250))
            .connect_tcp("127.0.0.1", port)
            .unwrap();

        assert_eq!(client.timeout(), Some(Duration::from_millis(250)));

        client.set_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(client.timeout(), Some(Duration::from_secs(1)));

        client.close().unwrap();
        server.join().unwrap();
    }

    #[test]
    fn should_bound_concurrent_outbound_requests_given_max_one_when_second_request_starts() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        let (first_request_seen_tx, first_request_seen_rx) = mpsc::channel();
        let (allow_first_response_tx, allow_first_response_rx) = mpsc::channel();
        let (second_request_seen_tx, second_request_seen_rx) = mpsc::channel();

        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            read_length_prefixed_frame(&mut socket);

            let first_frame = read_length_prefixed_frame(&mut socket);
            let (first_msg_type, _) = decode_message_frame(&first_frame).unwrap();
            first_request_seen_tx.send(()).unwrap();

            allow_first_response_rx.recv().unwrap();
            let mut first_payload = vec![0u8];
            first_payload.extend_from_slice(&1u64.to_be_bytes());
            write_length_prefixed_frame(
                &mut socket,
                &encode_message_frame(first_msg_type, &first_payload),
            );

            let second_frame = read_length_prefixed_frame(&mut socket);
            let (second_msg_type, _) = decode_message_frame(&second_frame).unwrap();
            second_request_seen_tx.send(()).unwrap();

            let mut second_payload = vec![0u8];
            second_payload.extend_from_slice(&2u64.to_be_bytes());
            write_length_prefixed_frame(
                &mut socket,
                &encode_message_frame(second_msg_type, &second_payload),
            );
        });

        let client = Arc::new(
            FitzClient::builder("secret")
                .with_max_in_flight_requests(1)
                .connect_tcp("127.0.0.1", port)
                .unwrap(),
        );

        let first_client = Arc::clone(&client);
        let first = thread::spawn(move || {
            let _tx = first_client
                .kv()
                .begin("kv://my-realm/app/users", TransactionMode::ReadWrite)
                .unwrap();
        });

        first_request_seen_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();

        let second_client = Arc::clone(&client);
        let second = thread::spawn(move || {
            let _tx = second_client
                .kv()
                .begin("kv://my-realm/app/users-2", TransactionMode::ReadWrite)
                .unwrap();
        });

        assert!(
            second_request_seen_rx
                .recv_timeout(Duration::from_millis(100))
                .is_err()
        );

        allow_first_response_tx.send(()).unwrap();

        first.join().unwrap();
        second.join().unwrap();
        server.join().unwrap();
    }

    #[test]
    fn should_not_cross_route_same_type_responses_between_concurrent_requests() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (first_request_seen_tx, first_request_seen_rx) = mpsc::channel();
        let (second_request_seen_tx, second_request_seen_rx) = mpsc::channel();

        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();

            // CONNECT frame
            read_length_prefixed_frame(&mut socket);

            // First request (no explicit sync for payload assertions)
            let first_request = read_length_prefixed_frame(&mut socket);
            let (first_msg_type, _) = decode_message_frame(&first_request).unwrap();
            first_request_seen_tx.send(()).unwrap();

            // Second request before responding to exercise request queueing.
            let second_request = read_length_prefixed_frame(&mut socket);
            let (second_msg_type, _) = decode_message_frame(&second_request).unwrap();
            second_request_seen_tx.send(()).unwrap();

            let mut first_resp = vec![0xA];
            first_resp[0] ^= 0;
            write_length_prefixed_frame(
                &mut socket,
                &encode_message_frame(second_msg_type, &first_resp),
            );

            let mut second_resp = vec![0xB];
            second_resp[0] ^= 0;
            write_length_prefixed_frame(
                &mut socket,
                &encode_message_frame(first_msg_type, &second_resp),
            );
        });

        let client = FitzClient::builder("secret")
            .with_max_in_flight_requests(2)
            .connect_tcp("127.0.0.1", port)
            .unwrap();

        let first_client = Arc::new(client);
        let first_sender = Arc::clone(&first_client);
        let first = thread::spawn(move || {
            first_sender
                .connection
                .send_request(100, &[0x11])
                .expect("first request failed")
        });

        first_request_seen_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();

        let second_client = Arc::clone(&first_client);
        let second = thread::spawn(move || {
            second_client
                .connection
                .send_request(100, &[0x22])
                .expect("second request failed")
        });

        second_request_seen_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();

        let first_response = first.join().unwrap();
        let second_response = second.join().unwrap();
        server.join().unwrap();

        assert_eq!(first_response, vec![0xA]);
        assert_eq!(second_response, vec![0xB]);
    }

    #[test]
    fn should_drop_stale_response_after_request_timeout_when_requests_are_queued() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();

            // CONNECT frame
            read_length_prefixed_frame(&mut socket);

            // First request
            read_length_prefixed_frame(&mut socket);

            // Second request sent after first request times out and releases the permit
            read_length_prefixed_frame(&mut socket);

            // Stale response for the timed-out first request.
            thread::sleep(Duration::from_millis(50));
            write_length_prefixed_frame(&mut socket, &encode_message_frame(100, &[0xAA]));

            // Response intended for the second request.
            thread::sleep(Duration::from_millis(10));
            write_length_prefixed_frame(&mut socket, &encode_message_frame(100, &[0xBB]));
        });

        let client = FitzClient::builder("secret")
            .with_timeout(Duration::from_millis(200))
            .with_max_in_flight_requests(1)
            .connect_tcp("127.0.0.1", port)
            .unwrap();

        let first = thread::spawn({
            let conn = client.connection.clone();
            move || {
                conn.send_request_with_timeout(100, &[0x11], Duration::from_millis(20))
                    .expect_err("first request should timeout")
            }
        });

        thread::sleep(Duration::from_millis(10));

        let second = thread::spawn({
            let conn = client.connection.clone();
            move || conn.send_request(100, &[0x22])
        });

        let first_err = first.join().unwrap();
        assert!(matches!(first_err, FitzError::Timeout));

        let second_response = second.join().unwrap().unwrap();
        assert_eq!(second_response, vec![0xBB]);

        server.join().unwrap();
    }

    fn write_length_prefixed_frame(stream: &mut std::net::TcpStream, frame: &[u8]) {
        let len = u32::try_from(frame.len()).unwrap();
        stream.write_all(&len.to_be_bytes()).unwrap();
        stream.write_all(frame).unwrap();
        stream.flush().unwrap();
    }
}
