//! Connection management
//!
//! Provides a unified connection abstraction over TCP and WebSocket transports,
//! plus a thread-safe `SharedConnection` wrapper that eliminates the
//! encode→send→recv→decode boilerplate from every domain method.

use crate::codec::{decode_message_frame, try_encode_message_frame};
use crate::error::{FitzError, Result};
use crate::protocol::message_type;
use crate::transport::{AnyTransport, Transport};
use parking_lot::{Mutex, MutexGuard};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionState {
    Open,
    Closed,
}

pub struct FitzConnection {
    transport: AnyTransport,
    state: ConnectionState,
    timeout: Option<Duration>,
}

impl FitzConnection {
    /// Connect via TCP
    pub fn connect_tcp(host: &str, port: u16) -> Result<Self> {
        let transport = AnyTransport::Tcp(
            crate::transport::tcp::TcpTransport::connect(host, port)
                .map_err(|e| FitzError::Connection(e.to_string()))?,
        );
        Ok(Self {
            transport,
            state: ConnectionState::Open,
            timeout: None,
        })
    }

    /// Connect via WebSocket
    pub fn connect_ws(url: &str) -> Result<Self> {
        let transport = AnyTransport::WebSocket(Box::new(
            crate::transport::websocket::WebSocketTransport::connect(url)
                .map_err(|e| FitzError::Connection(e.to_string()))?,
        ));
        Ok(Self {
            transport,
            state: ConnectionState::Open,
            timeout: None,
        })
    }

    pub fn send_frame(&mut self, frame: &[u8]) -> Result<()> {
        self.ensure_open()?;
        self.transport
            .send_frame(frame)
            .map_err(map_transport_error)
    }

    pub fn recv_frame(&mut self) -> Result<Vec<u8>> {
        self.ensure_open()?;
        self.transport.recv_frame().map_err(map_transport_error)
    }

    pub fn set_timeout(&mut self, timeout: Duration) -> Result<()> {
        self.ensure_open()?;
        self.transport
            .set_timeouts(Some(timeout), Some(timeout))
            .map_err(map_transport_error)?;
        self.timeout = Some(timeout);
        Ok(())
    }

    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    pub fn close(&mut self) -> Result<()> {
        if matches!(self.state, ConnectionState::Closed) {
            return Ok(());
        }

        self.state = ConnectionState::Closed;
        self.transport.close().map_err(map_transport_error)
    }

    fn ensure_open(&self) -> Result<()> {
        if matches!(self.state, ConnectionState::Closed) {
            Err(FitzError::ConnectionClosed)
        } else {
            Ok(())
        }
    }
}

/// Thread-safe connection handle shared across domain clients.
///
/// Provides `send_request` which encapsulates the entire
/// encode→send→recv→strip-header cycle in one call, so domain
/// methods never touch framing.
#[derive(Clone)]
pub struct SharedConnection {
    inner: Arc<Mutex<FitzConnection>>,
    deferred_frames: Arc<Mutex<VecDeque<(u16, Vec<u8>)>>>,
}

impl SharedConnection {
    pub fn new(conn: FitzConnection) -> Self {
        Self {
            inner: Arc::new(Mutex::new(conn)),
            deferred_frames: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Send a typed request and return the raw response payload (TLV header stripped).
    ///
    /// This is the single point of encode→send→recv→decode for the entire client.
    /// Domain methods call this instead of manually locking, framing, and stripping.
    pub fn send_request(&self, msg_type: u16, payload: &[u8]) -> Result<Vec<u8>> {
        let frame = try_encode_message_frame(msg_type, payload)?;
        {
            let mut conn = self.lock();
            conn.send_frame(&frame)?;
        }

        let (_, resp_payload) =
            self.recv_message_matching(|received_type, _| !is_server_notification(received_type))?;
        Ok(resp_payload)
    }

    /// Send a typed request with a temporary transport timeout override.
    ///
    /// The original timeout is restored before returning, even when the request
    /// fails while waiting for a response.
    pub fn send_request_with_timeout(
        &self,
        msg_type: u16,
        payload: &[u8],
        timeout: Duration,
    ) -> Result<Vec<u8>> {
        let previous_timeout = {
            let mut conn = self.lock();
            let previous_timeout = conn.timeout();
            if previous_timeout != Some(timeout) {
                conn.set_timeout(timeout)?;
            }
            previous_timeout
        };

        let result = self.send_request(msg_type, payload);
        self.restore_timeout(previous_timeout)?;
        result
    }

    /// Send a frame with no response expected (fire-and-forget, e.g. CONNECT).
    pub fn send_only(&self, msg_type: u16, payload: &[u8]) -> Result<()> {
        let frame = try_encode_message_frame(msg_type, payload)?;
        let mut conn = self.lock();
        conn.send_frame(&frame)
    }

    pub fn close(&self) -> Result<()> {
        let mut conn = self.lock();
        conn.close()
    }

    pub fn set_timeout(&self, timeout: Duration) -> Result<()> {
        let mut conn = self.lock();
        conn.set_timeout(timeout)
    }

    pub fn timeout(&self) -> Option<Duration> {
        self.lock().timeout()
    }

    fn restore_timeout(&self, timeout: Option<Duration>) -> Result<()> {
        if let Some(timeout) = timeout {
            let mut conn = self.lock();
            conn.set_timeout(timeout)?;
        }
        Ok(())
    }

    pub(crate) fn recv_message_matching<F>(&self, mut matcher: F) -> Result<(u16, Vec<u8>)>
    where
        F: FnMut(u16, &[u8]) -> bool,
    {
        if let Some(frame) = self.take_deferred_matching(&mut matcher) {
            return Ok(frame);
        }

        loop {
            let frame = self.read_next_message()?;
            if matcher(frame.0, &frame.1) {
                return Ok(frame);
            }
            self.deferred_frames.lock().push_back(frame);
        }
    }

    fn lock(&self) -> MutexGuard<'_, FitzConnection> {
        self.inner.lock()
    }

    fn read_next_message(&self) -> Result<(u16, Vec<u8>)> {
        let frame = {
            let mut conn = self.lock();
            conn.recv_frame()?
        };

        let (msg_type, payload_start) = decode_message_frame(&frame)?;
        Ok((msg_type, frame[payload_start..].to_vec()))
    }

    fn take_deferred_matching<F>(&self, matcher: &mut F) -> Option<(u16, Vec<u8>)>
    where
        F: FnMut(u16, &[u8]) -> bool,
    {
        let mut deferred = self.deferred_frames.lock();
        let mut kept = VecDeque::with_capacity(deferred.len());
        let mut matched = None;

        while let Some(frame) = deferred.pop_front() {
            if matched.is_none() && matcher(frame.0, &frame.1) {
                matched = Some(frame);
            } else {
                kept.push_back(frame);
            }
        }

        *deferred = kept;
        matched
    }
}

/// Strip the TLV header from a response frame and return just the payload bytes.
#[cfg(test)]
fn strip_tlv_header(frame: &[u8]) -> Result<Vec<u8>> {
    if frame.is_empty() {
        return Ok(Vec::new());
    }
    let (_msg_type, payload_start) = decode_message_frame(frame)?;
    Ok(frame[payload_start..].to_vec())
}

fn is_server_notification(msg_type: u16) -> bool {
    matches!(
        msg_type,
        message_type::RPC_REQUEST
            | message_type::RPC_RESPONSE
            | message_type::RPC_ACK
            | message_type::QUEUE_NOTIFY
            | message_type::NOTICE_NOTIFY
            | message_type::STREAM_NOTIFY
            | message_type::SCHEDULE_NOTIFY
    )
}

fn map_transport_error(err: std::io::Error) -> FitzError {
    use std::io::ErrorKind;

    match err.kind() {
        ErrorKind::TimedOut => FitzError::Timeout,
        ErrorKind::ConnectionAborted
        | ErrorKind::ConnectionReset
        | ErrorKind::BrokenPipe
        | ErrorKind::UnexpectedEof
        | ErrorKind::NotConnected => FitzError::ConnectionClosed,
        _ => FitzError::Transport(err.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::TransactionMode;
    use crate::FitzClient;
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
    fn should_strip_payload_from_valid_frame() {
        let frame = vec![100, 0, 2, b'o', b'k'];
        let payload = strip_tlv_header(&frame).unwrap();
        assert_eq!(payload, b"ok");
    }

    #[test]
    fn should_reject_frame_with_trailing_bytes_when_stripping_header() {
        let frame = vec![100, 0, 1, b'o', b'k'];
        let err = strip_tlv_header(&frame).unwrap_err();
        assert!(err.to_string().contains("Frame length does not match"));
    }

    #[test]
    fn should_apply_builder_timeout_to_request_path() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();

            // CONNECT frame
            read_length_prefixed_frame(&mut socket);
            // First request frame; keep the socket open but never respond.
            read_length_prefixed_frame(&mut socket);

            thread::sleep(Duration::from_millis(250));
        });

        let client = FitzClient::builder("secret")
            .with_timeout(Duration::from_millis(50))
            .connect_tcp("127.0.0.1", port)
            .unwrap();

        let err = match client
            .kv()
            .begin("kv://test-realm/app/users", TransactionMode::ReadWrite)
        {
            Ok(_) => panic!("request unexpectedly succeeded"),
            Err(err) => err,
        };

        assert!(matches!(err, FitzError::Timeout));

        server.join().unwrap();
    }

    #[test]
    fn should_reject_request_after_close() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();

            // CONNECT frame
            read_length_prefixed_frame(&mut socket);
            // Leave the connection open until the client closes it.
            thread::sleep(Duration::from_millis(50));
        });

        let client = FitzClient::builder("secret")
            .connect_tcp("127.0.0.1", port)
            .unwrap();

        client.close().unwrap();

        let err = match client
            .kv()
            .begin("kv://test-realm/app/users", TransactionMode::ReadWrite)
        {
            Ok(_) => panic!("request unexpectedly succeeded after close"),
            Err(err) => err,
        };

        assert!(matches!(err, FitzError::ConnectionClosed));

        server.join().unwrap();
    }

    #[test]
    fn should_restore_timeout_after_scoped_request_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();

            // First request times out because the server stays silent longer than the
            // scoped timeout.
            read_length_prefixed_frame(&mut socket);
            thread::sleep(Duration::from_millis(150));
        });

        let mut conn = FitzConnection::connect_tcp("127.0.0.1", port).unwrap();
        conn.set_timeout(Duration::from_secs(1)).unwrap();
        let shared = SharedConnection::new(conn);

        let err = shared
            .send_request_with_timeout(41, b"slow", Duration::from_millis(50))
            .unwrap_err();
        assert!(matches!(err, FitzError::Timeout));
        assert_eq!(shared.lock().timeout(), Some(Duration::from_secs(1)));

        server.join().unwrap();
    }
}
