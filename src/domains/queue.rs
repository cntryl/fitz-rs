//! Queue domain client.

use crate::codec::{PayloadDecoder, PayloadEncoder};
use crate::connection::SharedConnection;
use crate::domains::routes::{
    validate_fixed_route, validate_registration_pattern, validate_selector_route,
};
use crate::error::{FitzError, Result};
use crate::protocol::message_type;
use std::time::Duration;

#[derive(Clone)]
/// Reserved queue item with helper methods for lease extension and completion.
pub struct QueueItem {
    pub route: String,
    id: u64,
    token: u64,
    pub body: Vec<u8>,
    conn: SharedConnection,
}

impl QueueItem {
    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn extend(&self, lease_seconds: u64) -> Result<()> {
        validate_fixed_route(&self.route, "queue", 3)?;

        let mut enc = PayloadEncoder::new();
        enc.put_string(&self.route);
        enc.put_u64(self.id);
        enc.put_u64(self.token);
        enc.put_u64(lease_seconds);

        let resp = self
            .conn
            .send_request(message_type::QUEUE_EXTEND, &enc.finish())?;

        decode_empty_ok_response("EXTEND", &resp)
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn complete(&self) -> Result<()> {
        validate_fixed_route(&self.route, "queue", 3)?;

        let mut enc = PayloadEncoder::new();
        enc.put_string(&self.route);
        enc.put_u64(self.id);
        enc.put_u64(self.token);

        let resp = self
            .conn
            .send_request(message_type::QUEUE_COMPLETE, &enc.finish())?;

        decode_empty_ok_response("COMPLETE", &resp)
    }
}

/// Queue domain client for enqueue/reserve and availability subscriptions.
pub struct QueueClient {
    conn: SharedConnection,
}

impl QueueClient {
    #[must_use]
    pub fn new(conn: SharedConnection) -> Self {
        Self { conn }
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn enqueue(&self, route: &str, body: &[u8], delay_ms: Option<u64>) -> Result<u64> {
        validate_fixed_route(route, "queue", 3)?;

        let mut enc = PayloadEncoder::new();
        enc.put_string(route);
        enc.put_bytes(body);

        let delay_seconds = delay_ms.unwrap_or(0) / 1000;
        enc.put_u8(u8::from(delay_seconds > 0));
        if delay_seconds > 0 {
            enc.put_u64(delay_seconds);
        }

        let resp = self
            .conn
            .send_request(message_type::QUEUE_ENQUEUE, &enc.finish())?;

        decode_enqueue_response(&resp)
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn reserve(
        &self,
        route: &str,
        lease_seconds: u64,
        batch_size: Option<u32>,
        wait_seconds: Option<u64>,
    ) -> Result<Vec<QueueItem>> {
        self.reserve_with_timeout(route, lease_seconds, batch_size, wait_seconds, None)
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn reserve_with_timeout(
        &self,
        route: &str,
        lease_seconds: u64,
        batch_size: Option<u32>,
        wait_seconds: Option<u64>,
        timeout: Option<Duration>,
    ) -> Result<Vec<QueueItem>> {
        validate_selector_route(route, "queue", 3, false)?;

        let mut enc = PayloadEncoder::new();
        enc.put_string(route);
        enc.put_u64(lease_seconds);

        let normalized_batch = batch_size.unwrap_or(1).max(1);
        enc.put_u8(1);
        enc.put_u32(normalized_batch);

        if let Some(wait) = wait_seconds.filter(|value| *value > 0) {
            enc.put_u8(1);
            enc.put_u64(wait);
        }

        let payload = enc.finish();
        let resp = match timeout {
            Some(timeout) => self.conn.send_request_with_timeout(
                message_type::QUEUE_RESERVE,
                &payload,
                timeout,
            )?,
            None => self
                .conn
                .send_request(message_type::QUEUE_RESERVE, &payload)?,
        };

        decode_reserve_response(route, &resp, &self.conn)
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn extend(&self, route: &str, id: u64, token: u64, lease_seconds: u64) -> Result<()> {
        validate_fixed_route(route, "queue", 3)?;

        let mut enc = PayloadEncoder::new();
        enc.put_string(route);
        enc.put_u64(id);
        enc.put_u64(token);
        enc.put_u64(lease_seconds);

        let resp = self
            .conn
            .send_request(message_type::QUEUE_EXTEND, &enc.finish())?;

        decode_empty_ok_response("EXTEND", &resp)
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn complete(&self, route: &str, id: u64, token: u64) -> Result<()> {
        validate_fixed_route(route, "queue", 3)?;

        let mut enc = PayloadEncoder::new();
        enc.put_string(route);
        enc.put_u64(id);
        enc.put_u64(token);

        let resp = self
            .conn
            .send_request(message_type::QUEUE_COMPLETE, &enc.finish())?;

        decode_empty_ok_response("COMPLETE", &resp)
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn subscribe(&self, pattern: &str) -> Result<QueueSubscription> {
        validate_registration_pattern(pattern, "queue", 3)?;

        let mut enc = PayloadEncoder::new();
        enc.put_string(pattern);

        let resp = self
            .conn
            .send_request(message_type::QUEUE_SUBSCRIBE, &enc.finish())?;

        let subscription_id = decode_subscription_response("SUBSCRIBE", &resp)?;
        Ok(QueueSubscription {
            conn: self.conn.clone(),
            pattern: pattern.to_string(),
            subscription_id,
        })
    }
}

/// Active queue availability subscription handle.
pub struct QueueSubscription {
    conn: SharedConnection,
    pattern: String,
    subscription_id: u64,
}

impl QueueSubscription {
    #[must_use]
    pub fn subscription_id(&self) -> u64 {
        self.subscription_id
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn next(&self) -> Result<QueueNotification> {
        let (_, payload) = self.conn.recv_message_matching(|msg_type, payload| {
            msg_type == message_type::QUEUE_NOTIFY
                && decode_notify_subscription_id(payload)
                    .is_ok_and(|sub_id| sub_id == self.subscription_id)
        })?;

        decode_queue_notify(&payload)
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn unsubscribe(&self) -> Result<()> {
        let mut enc = PayloadEncoder::new();
        enc.put_string(&self.pattern);

        let resp = self
            .conn
            .send_request(message_type::QUEUE_UNSUBSCRIBE, &enc.finish())?;

        decode_empty_ok_response("UNSUBSCRIBE", &resp)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Queue availability notification delivered to subscribers.
pub struct QueueNotification {
    pub route: String,
    pub ready_messages: u64,
    pub delayed_messages: u64,
    pub inflight_messages: u64,
}

fn decode_enqueue_response(buf: &[u8]) -> Result<u64> {
    let mut dec = PayloadDecoder::new(buf);
    let status = dec.get_u8()?;
    if status != 0 {
        return Err(decode_queue_error("ENQUEUE", &mut dec));
    }

    if dec.is_empty() { Ok(0) } else { dec.get_u64() }
}

fn decode_reserve_response(
    route: &str,
    buf: &[u8],
    conn: &SharedConnection,
) -> Result<Vec<QueueItem>> {
    let mut dec = PayloadDecoder::new(buf);
    let status = dec.get_u8()?;
    if status != 0 {
        return Err(decode_queue_error("RESERVE", &mut dec));
    }

    if dec.is_empty() {
        return Ok(Vec::new());
    }

    let count = dec.get_u32()? as usize;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        items.push(QueueItem {
            route: route.to_string(),
            id: dec.get_u64()?,
            token: dec.get_u64()?,
            body: dec.get_bytes()?,
            conn: conn.clone(),
        });
    }

    Ok(items)
}

fn decode_empty_ok_response(operation: &str, buf: &[u8]) -> Result<()> {
    let mut dec = PayloadDecoder::new(buf);
    let status = dec.get_u8()?;
    if status == 0 {
        return Ok(());
    }

    Err(decode_queue_error(operation, &mut dec))
}

fn decode_subscription_response(operation: &str, buf: &[u8]) -> Result<u64> {
    let mut dec = PayloadDecoder::new(buf);
    let status = dec.get_u8()?;
    if status != 0 {
        return Err(decode_queue_error(operation, &mut dec));
    }

    if dec.remaining() != 8 {
        return Err(FitzError::Protocol(format!(
            "{operation} response missing subscription id"
        )));
    }
    dec.get_u64()
}

fn decode_notify_subscription_id(payload: &[u8]) -> Result<u64> {
    let mut dec = PayloadDecoder::new(payload);
    dec.get_u64()
}

fn decode_queue_notify(payload: &[u8]) -> Result<QueueNotification> {
    let mut dec = PayloadDecoder::new(payload);
    let _subscription_id = dec.get_u64()?;
    let route = dec.get_string()?;
    let ready_messages = dec.get_u64()?;
    let delayed_messages = dec.get_u64()?;
    let inflight_messages = dec.get_u64()?;
    if !dec.is_empty() {
        return Err(FitzError::Protocol(
            "QUEUE NOTIFY payload has trailing bytes".to_string(),
        ));
    }
    Ok(QueueNotification {
        route,
        ready_messages,
        delayed_messages,
        inflight_messages,
    })
}

fn decode_queue_error(operation: &str, dec: &mut PayloadDecoder<'_>) -> FitzError {
    let Ok(code) = dec.get_u32() else {
        return FitzError::Protocol(format!("{operation} failed with malformed error payload"));
    };
    let Ok(message) = dec.get_string() else {
        return FitzError::Protocol(format!("{operation} failed with malformed error payload"));
    };
    if !dec.is_empty() {
        return FitzError::Protocol(format!("{operation} failed with malformed error payload"));
    }

    FitzError::Domain {
        code,
        message: format!("{operation} failed: {message}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::{FitzConnection, SharedConnection};
    use std::io::Read;
    use std::net::TcpListener;
    use std::thread;

    #[test]
    fn should_decode_enqueue_response() {
        // Arrange
        let mut buf = vec![0];
        buf.extend_from_slice(&7u64.to_be_bytes());
        // Act
        let id = decode_enqueue_response(&buf).unwrap();
        // Assert
        assert_eq!(id, 7);
    }

    #[test]
    fn should_decode_queue_error_code_response() {
        // Arrange
        let mut buf = vec![1];
        buf.extend_from_slice(&3u32.to_be_bytes());
        buf.extend_from_slice(&20u32.to_be_bytes());
        buf.extend_from_slice(b"queue item not found");

        // Act
        let error = decode_empty_ok_response("COMPLETE", &buf).unwrap_err();

        // Assert
        assert!(matches!(error, FitzError::Domain { code: 3, .. }));
    }

    #[test]
    fn should_reject_legacy_queue_error_code_response() {
        // Arrange
        let buf = [1, 3];

        // Act
        let error = decode_empty_ok_response("COMPLETE", &buf).unwrap_err();

        // Assert
        assert!(matches!(error, FitzError::Protocol(_)));
    }

    #[test]
    fn should_preserve_typed_subscription_error_given_broker_error_envelope() {
        // Arrange
        let mut buf = vec![1];
        buf.extend_from_slice(&4010u32.to_be_bytes());
        buf.extend_from_slice(&15u32.to_be_bytes());
        buf.extend_from_slice(b"invalid pattern");

        // Act
        let error = decode_subscription_response("SUBSCRIBE", &buf).unwrap_err();

        // Assert
        assert!(matches!(error, FitzError::Domain { code: 4010, .. }));
    }

    #[test]
    fn should_decode_queue_subscription_response() {
        // Arrange
        let mut buf = vec![0];
        buf.extend_from_slice(&42u64.to_be_bytes());
        // Act
        let sub_id = decode_subscription_response("SUBSCRIBE", &buf).unwrap();
        // Assert
        assert_eq!(sub_id, 42);
    }

    #[test]
    fn should_decode_queue_notify_payload() {
        // Arrange
        let mut buf = Vec::new();
        buf.extend_from_slice(&7u64.to_be_bytes());
        buf.extend_from_slice(&(20u32).to_be_bytes());
        buf.extend_from_slice(b"queue://realm/area/x");
        buf.extend_from_slice(&3u64.to_be_bytes());
        buf.extend_from_slice(&2u64.to_be_bytes());
        buf.extend_from_slice(&1u64.to_be_bytes());

        // Act
        let notification = decode_queue_notify(&buf).unwrap();
        // Assert
        assert_eq!(notification.route, "queue://realm/area/x");
        assert_eq!(notification.ready_messages, 3);
        assert_eq!(notification.delayed_messages, 2);
        assert_eq!(notification.inflight_messages, 1);
    }

    fn read_length_prefixed_frame(stream: &mut std::net::TcpStream) {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).unwrap();
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut frame = vec![0u8; len];
        stream.read_exact(&mut frame).unwrap();
    }

    #[test]
    fn should_timeout_queue_reserve_with_scoped_timeout() {
        // Arrange
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            read_length_prefixed_frame(&mut socket);
            thread::sleep(Duration::from_millis(150));
        });

        let mut conn = FitzConnection::connect_tcp("127.0.0.1", port).unwrap();
        conn.set_timeout(Duration::from_secs(1)).unwrap();
        let client = QueueClient::new(SharedConnection::new(conn, 256));

        let Err(err) = client.reserve_with_timeout(
            "queue://test-realm/app/jobs",
            30,
            Some(1),
            Some(60),
            Some(Duration::from_millis(50)),
        ) else {
            panic!("reserve unexpectedly succeeded");
            // Act
        };

        // Assert
        assert!(matches!(err, FitzError::Timeout));

        server.join().unwrap();
    }
}
