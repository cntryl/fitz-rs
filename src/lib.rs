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
//! // Connect — realm lives in the auth handshake, not on the client struct.
//! let client = FitzClient::connect_tcp("127.0.0.1", 4091, "my-realm", "secret")?;
//!
//! // Routes are opaque strings — the client never parses them.
//! let tx = client.kv().begin("kv://my-realm/app/users", TransactionMode::ReadWrite)?;
//! tx.put(b"user:1", b"alice")?;
//! tx.commit()?;
//!
//! // Leases
//! let grant = client.lease().acquire("lease://my-realm/locks/leader", "node-1", 30)?;
//! ```

pub mod auth;
pub mod codec;
pub mod connection;
pub mod domains;
pub mod error;
pub mod protocol;
pub mod transport;

pub use auth::TestTokenGenerator;
pub use error::{FitzError, FitzErrorKind, Result};
pub use protocol::TransactionMode;

use connection::{FitzConnection, SharedConnection};
use std::time::Duration;

enum ClientAuth {
    Jwt(String),
    Anonymous,
}

/// Builder for creating Fitz clients with flexible configuration.
///
/// `realm` is carried in the CONNECT auth handshake and is not stored
/// on the client or passed to domain methods.
pub struct FitzClientBuilder {
    realm: String,
    auth: ClientAuth,
    timeout: Duration,
}

impl FitzClientBuilder {
    pub fn new(realm: &str, secret: &str) -> Self {
        Self {
            realm: realm.to_string(),
            auth: ClientAuth::Jwt(secret.to_string()),
            timeout: Duration::from_secs(30),
        }
    }

    pub fn anonymous(realm: &str) -> Self {
        Self {
            realm: realm.to_string(),
            auth: ClientAuth::Anonymous,
            timeout: Duration::from_secs(30),
        }
    }

    /// Set connection timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Connect via TCP.
    pub fn connect_tcp(self, host: &str, port: u16) -> Result<FitzClient> {
        let conn = FitzConnection::connect_tcp(host, port)?;
        self.finish(conn)
    }

    /// Connect via WebSocket.
    pub fn connect_ws(self, url: &str) -> Result<FitzClient> {
        let conn = FitzConnection::connect_ws(url)?;
        self.finish(conn)
    }

    fn finish(self, mut conn: FitzConnection) -> Result<FitzClient> {
        conn.set_timeout(self.timeout)?;
        let shared = SharedConnection::new(conn);

        // Generate JWT and send CONNECT frame.
        // Per wire protocol: silence means success, server closes on invalid CONNECT.
        let token = match self.auth {
            ClientAuth::Jwt(secret) => {
                TestTokenGenerator::new(&secret).generate(&self.realm, "fitz-client")?
            }
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
/// the JWT sent during CONNECT, and route strings carry the addressing.
pub struct FitzClient {
    connection: SharedConnection,
}

impl FitzClient {
    /// Create a builder.
    pub fn builder(realm: &str, secret: &str) -> FitzClientBuilder {
        FitzClientBuilder::new(realm, secret)
    }

    /// Create a builder for anonymous connections.
    pub fn builder_anonymous(realm: &str) -> FitzClientBuilder {
        FitzClientBuilder::anonymous(realm)
    }

    /// Convenient helper: connect via TCP.
    pub fn connect_tcp(host: &str, port: u16, realm: &str, secret: &str) -> Result<Self> {
        FitzClient::builder(realm, secret).connect_tcp(host, port)
    }

    /// Convenient helper: connect via TCP without a JWT.
    pub fn connect_tcp_anonymous(host: &str, port: u16, realm: &str) -> Result<Self> {
        FitzClient::builder_anonymous(realm).connect_tcp(host, port)
    }

    /// Convenient helper: connect via WebSocket.
    pub fn connect_ws(url: &str, realm: &str, secret: &str) -> Result<Self> {
        FitzClient::builder(realm, secret).connect_ws(url)
    }

    /// Convenient helper: connect via WebSocket without a JWT.
    pub fn connect_ws_anonymous(url: &str, realm: &str) -> Result<Self> {
        FitzClient::builder_anonymous(realm).connect_ws(url)
    }

    /// Get a KV domain client.
    pub fn kv(&self) -> domains::kv::KvClient {
        domains::kv::KvClient::new(self.connection.clone())
    }

    /// Get a Lease domain client.
    pub fn lease(&self) -> domains::lease::LeaseClient {
        domains::lease::LeaseClient::new(self.connection.clone())
    }

    /// Get a Queue domain client.
    pub fn queue(&self) -> domains::queue::QueueClient {
        domains::queue::QueueClient::new(self.connection.clone())
    }

    /// Get a Notice domain client.
    pub fn notice(&self) -> domains::notice::NoticeClient {
        domains::notice::NoticeClient::new(self.connection.clone())
    }

    /// Get a Schedule domain client.
    pub fn schedule(&self) -> domains::schedule::ScheduleClient {
        domains::schedule::ScheduleClient::new(self.connection.clone())
    }

    /// Get an RPC domain client.
    pub fn rpc(&self) -> domains::rpc::RpcClient {
        domains::rpc::RpcClient::new(self.connection.clone())
    }

    /// Get a Stream domain client.
    pub fn stream(&self) -> domains::stream::StreamClient {
        domains::stream::StreamClient::new(self.connection.clone())
    }

    /// Close the connection.
    pub fn close(&self) -> Result<()> {
        self.connection.close()
    }

    /// Update the default transport timeout used by subsequent requests.
    pub fn set_timeout(&self, timeout: Duration) -> Result<()> {
        self.connection.set_timeout(timeout)
    }

    /// Return the current default transport timeout.
    pub fn timeout(&self) -> Option<Duration> {
        self.connection.timeout()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::net::TcpListener;
    use std::thread;

    fn read_length_prefixed_frame(stream: &mut std::net::TcpStream) {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).unwrap();
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut frame = vec![0u8; len];
        stream.read_exact(&mut frame).unwrap();
    }

    #[test]
    fn should_create_client_builder() {
        let builder = FitzClient::builder("test-realm", "secret");
        assert_eq!(builder.realm, "test-realm");
    }

    #[test]
    fn should_create_anonymous_builder() {
        let builder = FitzClient::builder_anonymous("test-realm");
        assert_eq!(builder.realm, "test-realm");
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

        let client = FitzClient::builder("test-realm", "secret")
            .with_timeout(Duration::from_millis(250))
            .connect_tcp("127.0.0.1", port)
            .unwrap();

        assert_eq!(client.timeout(), Some(Duration::from_millis(250)));

        client.set_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(client.timeout(), Some(Duration::from_secs(1)));

        client.close().unwrap();
        server.join().unwrap();
    }
}
