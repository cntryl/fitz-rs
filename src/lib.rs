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

mod auth;
mod codec;
mod connection;
pub mod domains;
mod error;
mod protocol;
mod transport;

pub use error::{FitzError, FitzErrorKind, Result};
pub use protocol::TransactionMode;

use connection::{FitzConnection, SharedConnection};
use std::time::Duration;

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
    pub fn new(token: &str) -> Self {
        Self {
            auth: ClientAuth::Token(token.to_string()),
            timeout: Duration::from_secs(30),
            max_in_flight_requests: 256,
        }
    }

    pub fn anonymous() -> Self {
        Self {
            auth: ClientAuth::Anonymous,
            timeout: Duration::from_secs(30),
            max_in_flight_requests: 256,
        }
    }

    /// Set connection timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the maximum number of in-flight requests allowed on this client.
    pub fn with_max_in_flight_requests(mut self, max_in_flight_requests: usize) -> Self {
        self.max_in_flight_requests = max_in_flight_requests.max(1);
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
    pub fn builder(token: &str) -> FitzClientBuilder {
        FitzClientBuilder::new(token)
    }

    /// Create a builder for anonymous connections.
    pub fn builder_anonymous() -> FitzClientBuilder {
        FitzClientBuilder::anonymous()
    }

    /// Convenient helper: connect via TCP.
    pub fn connect_tcp(host: &str, port: u16, token: &str) -> Result<Self> {
        FitzClient::builder(token).connect_tcp(host, port)
    }

    /// Convenient helper: connect via TCP without a JWT.
    pub fn connect_tcp_anonymous(host: &str, port: u16) -> Result<Self> {
        FitzClient::builder_anonymous().connect_tcp(host, port)
    }

    /// Convenient helper: connect via WebSocket.
    pub fn connect_ws(url: &str, token: &str) -> Result<Self> {
        FitzClient::builder(token).connect_ws(url)
    }

    /// Convenient helper: connect via WebSocket without a JWT.
    pub fn connect_ws_anonymous(url: &str) -> Result<Self> {
        FitzClient::builder_anonymous().connect_ws(url)
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
    use crate::codec::{decode_message_frame, encode_message_frame};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::sync::Arc;
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

        first_request_seen_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let second_client = Arc::clone(&client);
        let second = thread::spawn(move || {
            let _tx = second_client
                .kv()
                .begin("kv://my-realm/app/users-2", TransactionMode::ReadWrite)
                .unwrap();
        });

        assert!(second_request_seen_rx.recv_timeout(Duration::from_millis(100)).is_err());

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

        first_request_seen_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let second_client = Arc::clone(&first_client);
        let second = thread::spawn(move || {
            second_client
                .connection
                .send_request(100, &[0x22])
                .expect("second request failed")
        });

        second_request_seen_rx.recv_timeout(Duration::from_secs(1)).unwrap();

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
        let len = frame.len() as u32;
        stream.write_all(&len.to_be_bytes()).unwrap();
        stream.write_all(frame).unwrap();
        stream.flush().unwrap();
    }
}
