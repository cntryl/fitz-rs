//! Connection management
//!
//! Provides a unified connection abstraction over TCP and WebSocket transports,
//! plus a thread-safe `SharedConnection` wrapper that eliminates the
//! encode→send→recv→decode boilerplate from every domain method.

use crate::codec::{decode_message_frame, try_encode_message_frame};
use crate::error::{FitzError, Result};
use crate::protocol::message_type;
use crate::transport::{AnyTransport, Transport};
use parking_lot::{Condvar, Mutex, MutexGuard};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, mpsc};
use std::time::Duration;

type DeferredFrame = (u16, Vec<u8>);
type DeferredFrames = Arc<Mutex<VecDeque<DeferredFrame>>>;
type ResponseSender = mpsc::Sender<Result<Vec<u8>>>;
type PendingResponses = Arc<Mutex<HashMap<u16, VecDeque<ResponseSender>>>>;

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
    fn try_clone_tcp(&self) -> Option<Self> {
        let AnyTransport::Tcp(transport) = &self.transport else {
            return None;
        };
        Some(Self {
            transport: AnyTransport::Tcp(transport.try_clone().ok()?),
            state: self.state,
            timeout: self.timeout,
        })
    }
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
            .map_err(|error| map_transport_error(&error))
    }

    pub fn recv_frame(&mut self) -> Result<Vec<u8>> {
        self.ensure_open()?;
        self.transport
            .recv_frame()
            .map_err(|error| map_transport_error(&error))
    }

    pub fn set_timeout(&mut self, timeout: Duration) -> Result<()> {
        self.ensure_open()?;
        self.transport
            .set_timeouts(Some(timeout), Some(timeout))
            .map_err(|error| map_transport_error(&error))?;
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
        self.transport
            .close()
            .map_err(|error| map_transport_error(&error))
    }

    fn ensure_open(&self) -> Result<()> {
        if matches!(self.state, ConnectionState::Closed) {
            Err(FitzError::ConnectionClosed)
        } else {
            Ok(())
        }
    }
}

struct RequestGate {
    active: Mutex<usize>,
    max_in_flight_requests: usize,
    wakeup: Condvar,
}

impl RequestGate {
    fn new(max_in_flight_requests: usize) -> Self {
        Self {
            active: Mutex::new(0),
            max_in_flight_requests: max_in_flight_requests.max(1),
            wakeup: Condvar::new(),
        }
    }

    fn acquire(self: &Arc<Self>) -> RequestPermit {
        let mut active = self.active.lock();
        while *active >= self.max_in_flight_requests {
            self.wakeup.wait(&mut active);
        }

        *active += 1;
        RequestPermit {
            gate: Arc::clone(self),
        }
    }

    fn release(&self) {
        let mut active = self.active.lock();
        if *active > 0 {
            *active -= 1;
        }
        self.wakeup.notify_one();
    }
}

struct RequestPermit {
    gate: Arc<RequestGate>,
}

impl Drop for RequestPermit {
    fn drop(&mut self) {
        self.gate.release();
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
    reader_inner: Arc<Mutex<FitzConnection>>,
    deferred_frames: DeferredFrames,
    request_gate: Arc<RequestGate>,
    pending: PendingResponses,
    reader: Arc<Mutex<()>>,
}

impl SharedConnection {
    pub fn new(conn: FitzConnection, max_in_flight_requests: usize) -> Self {
        let reader_conn = conn.try_clone_tcp();
        let inner = Arc::new(Mutex::new(conn));
        Self {
            reader_inner: reader_conn
                .map_or_else(|| Arc::clone(&inner), |conn| Arc::new(Mutex::new(conn))),
            inner,
            deferred_frames: Arc::new(Mutex::new(VecDeque::new())),
            request_gate: Arc::new(RequestGate::new(max_in_flight_requests)),
            pending: Arc::new(Mutex::new(HashMap::new())),
            reader: Arc::new(Mutex::new(())),
        }
    }

    /// Send a typed request and return the raw response payload (TLV header stripped).
    ///
    /// This is the single point of encode→send→recv→decode for the entire client.
    /// Domain methods call this instead of manually locking, framing, and stripping.
    pub fn send_request(&self, msg_type: u16, payload: &[u8]) -> Result<Vec<u8>> {
        let _permit = self.request_gate.acquire();
        let frame = try_encode_message_frame(msg_type, payload)?;
        let (tx, rx) = mpsc::channel();
        self.pending
            .lock()
            .entry(msg_type)
            .or_default()
            .push_back(tx);
        {
            let mut conn = self.lock();
            if let Err(error) = conn.send_frame(&frame) {
                let _ = self
                    .pending
                    .lock()
                    .get_mut(&msg_type)
                    .and_then(VecDeque::pop_back);
                return Err(error);
            }
        }
        // Give already-admitted writers a chance to enqueue before one caller
        // becomes the reader. The reader never holds the write lock while it
        // dispatches a decoded response.
        std::thread::yield_now();
        self.wait_for_response(&rx)
    }

    fn wait_for_response(&self, rx: &mpsc::Receiver<Result<Vec<u8>>>) -> Result<Vec<u8>> {
        loop {
            match rx.try_recv() {
                Ok(result) => return result,
                Err(mpsc::TryRecvError::Disconnected) => return Err(FitzError::ConnectionClosed),
                Err(mpsc::TryRecvError::Empty) => {}
            }

            if let Some(_reader) = self.reader.try_lock() {
                let (message_type, payload) = self.read_next_message()?;
                if is_server_notification(message_type) {
                    self.deferred_frames
                        .lock()
                        .push_back((message_type, payload));
                    continue;
                }
                let waiter = self
                    .pending
                    .lock()
                    .get_mut(&message_type)
                    .and_then(VecDeque::pop_front);
                if let Some(waiter) = waiter {
                    // A failed send is an intentional timeout/cancellation tombstone.
                    let _ = waiter.send(Ok(payload));
                } else {
                    tracing::warn!(
                        message_type,
                        "discarding response without a pending request"
                    );
                }
            } else {
                std::thread::yield_now();
            }
        }
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
        let _permit = self.request_gate.acquire();
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
        loop {
            if let Some(frame) = self.take_deferred_matching(&mut matcher) {
                return Ok(frame);
            }

            let _reader = self.reader.lock();
            if let Some(frame) = self.take_deferred_matching(&mut matcher) {
                return Ok(frame);
            }

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
            let mut conn = self.reader_inner.lock();
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
        let mut matching_frame = None;

        while let Some(frame) = deferred.pop_front() {
            if matching_frame.is_none() && matcher(frame.0, &frame.1) {
                matching_frame = Some(frame);
            } else {
                kept.push_back(frame);
            }
        }

        *deferred = kept;
        matching_frame
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
            | message_type::QUEUE_NOTIFY
            | message_type::LEASE_NOTIFY
            | message_type::NOTICE_NOTIFY
            | message_type::STREAM_NOTIFY
            | message_type::SCHEDULE_NOTIFY
    )
}

fn map_transport_error(err: &std::io::Error) -> FitzError {
    use std::io::ErrorKind;

    match err.kind() {
        ErrorKind::TimedOut | ErrorKind::WouldBlock => FitzError::Timeout,
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
    use crate::FitzClient;
    use crate::protocol::TransactionMode;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    fn read_length_prefixed_frame(stream: &mut std::net::TcpStream) {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).unwrap();
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut frame = vec![0u8; len];
        stream.read_exact(&mut frame).unwrap();
    }

    fn write_length_prefixed_frame(stream: &mut std::net::TcpStream, frame: &[u8]) {
        let len = u32::try_from(frame.len()).unwrap();
        stream.write_all(&len.to_be_bytes()).unwrap();
        stream.write_all(frame).unwrap();
        stream.flush().unwrap();
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
        // Arrange
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

        let Err(err) = client
            .kv()
            .begin("kv://test-realm/app/users", TransactionMode::ReadWrite)
        else {
            panic!("request unexpectedly succeeded");
            // Act
        };

        // Assert
        assert!(matches!(err, FitzError::Timeout));

        server.join().unwrap();
    }

    #[test]
    fn should_reject_request_after_close() {
        // Arrange
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

        let Err(err) = client
            .kv()
            .begin("kv://test-realm/app/users", TransactionMode::ReadWrite)
        else {
            panic!("request unexpectedly succeeded after close");
            // Act
        };

        // Assert
        assert!(matches!(err, FitzError::ConnectionClosed));

        server.join().unwrap();
    }

    #[test]
    fn should_route_out_of_order_matching_frames_to_concurrent_readers() {
        // Arrange
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (release_tx, release_rx) = mpsc::channel();

        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            release_rx.recv().unwrap();
            write_length_prefixed_frame(
                &mut socket,
                &try_encode_message_frame(message_type::RPC_RESPONSE, &[2]).unwrap(),
            );
            write_length_prefixed_frame(
                &mut socket,
                &try_encode_message_frame(message_type::RPC_RESPONSE, &[1]).unwrap(),
            );
        });

        let connection =
            SharedConnection::new(FitzConnection::connect_tcp("127.0.0.1", port).unwrap(), 2);
        let first_connection = connection.clone();
        let first = thread::spawn(move || {
            first_connection
                .recv_message_matching(|msg_type, payload| {
                    msg_type == message_type::RPC_RESPONSE && payload == [1]
                })
                .unwrap()
        });

        thread::sleep(Duration::from_millis(25));

        let second_connection = connection.clone();
        let second = thread::spawn(move || {
            second_connection
                .recv_message_matching(|msg_type, payload| {
                    msg_type == message_type::RPC_RESPONSE && payload == [2]
                })
                .unwrap()
        });

        thread::sleep(Duration::from_millis(25));

        // Act
        release_tx.send(()).unwrap();

        // Assert
        assert_eq!(first.join().unwrap().1, vec![1]);
        assert_eq!(second.join().unwrap().1, vec![2]);
        server.join().unwrap();
    }

    #[test]
    fn should_restore_timeout_after_scoped_request_timeout() {
        // Arrange
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
        let shared = SharedConnection::new(conn, 256);

        // Act
        let err = shared
            .send_request_with_timeout(41, b"slow", Duration::from_millis(50))
            .unwrap_err();
        // Assert
        assert!(matches!(err, FitzError::Timeout));
        assert_eq!(shared.lock().timeout(), Some(Duration::from_secs(1)));

        server.join().unwrap();
    }
}
