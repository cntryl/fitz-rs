//! Queue domain client.

use crate::codec::{PayloadDecoder, PayloadEncoder};
use crate::connection::SharedConnection;
use crate::domains::routes::{validate_fixed_route, validate_selector_route};
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
    pub fn new(conn: SharedConnection) -> Self {
        Self { conn }
    }

    pub fn enqueue(&self, route: &str, body: &[u8], delay_ms: Option<u64>) -> Result<u64> {
        validate_fixed_route(route, "queue", 3)?;

        let mut enc = PayloadEncoder::new();
        enc.put_string(route);
        enc.put_bytes(body);

        let delay_seconds = delay_ms.unwrap_or(0) / 1000;
        enc.put_u8((delay_seconds > 0) as u8);
        if delay_seconds > 0 {
            enc.put_u64(delay_seconds);
        }

        let resp = self
            .conn
            .send_request(message_type::QUEUE_ENQUEUE, &enc.finish())?;

        decode_enqueue_response(&resp)
    }

    pub fn reserve(
        &self,
        route: &str,
        lease_seconds: u64,
        batch_size: Option<u32>,
        wait_seconds: Option<u64>,
    ) -> Result<Vec<QueueItem>> {
        self.reserve_with_timeout(route, lease_seconds, batch_size, wait_seconds, None)
    }

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

        match wait_seconds.filter(|value| *value > 0) {
            Some(wait) => {
                enc.put_u8(1);
                enc.put_u64(wait);
            }
            None => {
                enc.put_u8(0);
            }
        }

        let payload = enc.finish();
        let resp = match timeout {
            Some(timeout) => self
                .conn
                .send_request_with_timeout(message_type::QUEUE_RESERVE, &payload, timeout)?,
            None => self.conn.send_request(message_type::QUEUE_RESERVE, &payload)?,
        };

        decode_reserve_response(route, &resp, self.conn.clone())
    }

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

    pub fn subscribe(&self, pattern: &str) -> Result<QueueSubscription> {
        validate_selector_route(pattern, "queue", 3, true)?;

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
    pub fn subscription_id(&self) -> u64 {
        self.subscription_id
    }

    pub fn next(&self) -> Result<QueueNotification> {
        let (_, payload) = self.conn.recv_message_matching(|msg_type, payload| {
            msg_type == message_type::QUEUE_NOTIFY
                && decode_notify_subscription_id(payload)
                    .map(|sub_id| sub_id == self.subscription_id)
                    .unwrap_or(false)
        })?;

        decode_queue_notify(&payload)
    }

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
    pub payload: Vec<u8>,
}

fn decode_enqueue_response(buf: &[u8]) -> Result<u64> {
    let mut dec = PayloadDecoder::new(buf);
    let status = dec.get_u8()?;
    if status != 0 {
        return Err(decode_queue_error("ENQUEUE", &mut dec));
    }

    if dec.is_empty() {
        Ok(0)
    } else {
        dec.get_u64()
    }
}

fn decode_reserve_response(
    route: &str,
    buf: &[u8],
    conn: SharedConnection,
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

    let has_subscription_id = if dec.is_empty() { 0 } else { dec.get_u8()? };
    if has_subscription_id != 1 {
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
    let payload = dec.get_bytes()?;
    Ok(QueueNotification { route, payload })
}

fn decode_queue_error(operation: &str, dec: &mut PayloadDecoder<'_>) -> FitzError {
    if dec.remaining() == 1 {
        let code = dec.get_u8().unwrap_or_default();
        let message = match code {
            1 => "invalid lease token",
            2 => "queue lease expired",
            3 => "queue item not found",
            4 => "queue not found",
            _ => "unknown queue error",
        };
        return FitzError::DomainError(format!("{operation} failed: {message}"));
    }

    match dec.get_string() {
        Ok(message) => FitzError::DomainError(format!("{operation} failed: {message}")),
        Err(_) => FitzError::Protocol(format!("{operation} failed with malformed error payload")),
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
        let mut buf = vec![0];
        buf.extend_from_slice(&7u64.to_be_bytes());
        let id = decode_enqueue_response(&buf).unwrap();
        assert_eq!(id, 7);
    }

    #[test]
    fn should_decode_queue_error_code_response() {
        let err = decode_empty_ok_response("COMPLETE", &[1, 3]).unwrap_err();
        assert!(err.to_string().contains("queue item not found"));
    }

    #[test]
    fn should_decode_queue_subscription_response() {
        let mut buf = vec![0, 1];
        buf.extend_from_slice(&42u64.to_be_bytes());
        let sub_id = decode_subscription_response("SUBSCRIBE", &buf).unwrap();
        assert_eq!(sub_id, 42);
    }

    #[test]
    fn should_decode_queue_notify_payload() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&7u64.to_be_bytes());
        buf.extend_from_slice(&(20u32).to_be_bytes());
        buf.extend_from_slice(b"queue://realm/area/x");
        buf.extend_from_slice(&(4u32).to_be_bytes());
        buf.extend_from_slice(b"fire");

        let notification = decode_queue_notify(&buf).unwrap();
        assert_eq!(notification.route, "queue://realm/area/x");
        assert_eq!(notification.payload, b"fire");
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
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            read_length_prefixed_frame(&mut socket);
            thread::sleep(Duration::from_millis(150));
        });

        let mut conn = FitzConnection::connect_tcp("127.0.0.1", port).unwrap();
        conn.set_timeout(Duration::from_secs(1)).unwrap();
        let client = QueueClient::new(SharedConnection::new(conn));

        let err = match client.reserve_with_timeout(
            "queue://test-realm/app/jobs",
            30,
            Some(1),
            Some(60),
            Some(Duration::from_millis(50)),
        ) {
            Ok(_) => panic!("reserve unexpectedly succeeded"),
            Err(err) => err,
        };

        assert!(matches!(err, FitzError::Timeout));

        server.join().unwrap();
    }
}
