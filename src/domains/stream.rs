//! Stream domain client.

use crate::codec::{PayloadDecoder, PayloadEncoder};
use crate::connection::SharedConnection;
use crate::domains::routes::{validate_fixed_route, validate_selector_route};
use crate::error::{FitzError, Result};
use crate::protocol::message_type;
use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamCommitMode {
    Buffered = 0,
    Sync = 1,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamRecord {
    pub offset: u64,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamMetadata {
    pub first_offset: u64,
    pub last_offset: u64,
    pub record_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamCommitNotification {
    pub route: String,
    pub event: String,
    pub first_resource_offset: u64,
    pub last_resource_offset: u64,
    pub first_area_offset: u64,
    pub last_area_offset: u64,
    pub first_realm_offset: u64,
    pub last_realm_offset: u64,
    pub batch_size: u64,
    pub payload: Vec<u8>,
}

pub struct StreamClient {
    conn: SharedConnection,
}

impl StreamClient {
    pub fn new(conn: SharedConnection) -> Self {
        Self { conn }
    }

    pub fn begin(
        &self,
        route: &str,
        ingest_metadata: Option<&[u8]>,
    ) -> Result<StreamSession> {
        validate_fixed_route(route, "stream", 3)?;

        let mut enc = PayloadEncoder::new();
        enc.put_string(route);
        match ingest_metadata.filter(|metadata| !metadata.is_empty()) {
            Some(metadata) => {
                enc.put_u8(1);
                enc.put_bytes(metadata);
            }
            None => {
                enc.put_u8(0);
            }
        }

        let resp = self
            .conn
            .send_request(message_type::STREAM_BEGIN, &enc.finish())?;
        let decoded = decode_stream_response("BEGIN", &resp)?;
        let session_id = decoded
            .session_id
            .ok_or_else(|| FitzError::Protocol("BEGIN response missing session id".to_string()))?;

        Ok(StreamSession {
            conn: self.conn.clone(),
            session_id,
            active: true,
        })
    }

    pub fn read(
        &self,
        route: &str,
        start_offset: u64,
        limit: u64,
        max_bytes: Option<u64>,
    ) -> Result<Vec<StreamRecord>> {
        validate_selector_route(route, "stream", 3, true)?;

        let mut enc = PayloadEncoder::new();
        enc.put_string(route);
        enc.put_u64(start_offset);
        enc.put_u64(limit);
        match max_bytes {
            Some(value) => {
                enc.put_u8(1);
                enc.put_u64(value);
            }
            None => {
                enc.put_u8(0);
            }
        }

        let resp = self
            .conn
            .send_request(message_type::STREAM_READ, &enc.finish())?;
        let decoded = decode_stream_response("READ", &resp)?;
        parse_stream_records(&decoded.data)
    }

    pub fn peek(&self, route: &str) -> Result<Option<StreamRecord>> {
        validate_fixed_route(route, "stream", 3)?;

        let mut enc = PayloadEncoder::new();
        enc.put_string(route);

        let resp = self
            .conn
            .send_request(message_type::STREAM_LAST, &enc.finish())?;
        let decoded = decode_stream_response("LAST", &resp)?;
        parse_stream_record(&decoded.data)
    }

    pub fn metadata(&self, route: &str) -> Result<StreamMetadata> {
        validate_fixed_route(route, "stream", 3)?;

        let mut enc = PayloadEncoder::new();
        enc.put_string(route);

        let resp = self
            .conn
            .send_request(message_type::STREAM_GET_METADATA, &enc.finish())?;
        let decoded = decode_stream_response("METADATA", &resp)?;

        if decoded.data.is_empty() {
            return Ok(StreamMetadata {
                first_offset: 0,
                last_offset: 0,
                record_count: 0,
            });
        }

        let mut dec = PayloadDecoder::new(&decoded.data);
        Ok(StreamMetadata {
            first_offset: dec.get_u64()?,
            last_offset: dec.get_u64()?,
            record_count: dec.get_u64()?,
        })
    }

    pub fn subscribe(&self, pattern: &str) -> Result<StreamSubscription> {
        validate_selector_route(pattern, "stream", 3, true)?;

        let mut enc = PayloadEncoder::new();
        enc.put_string(pattern);

        let resp = self
            .conn
            .send_request(message_type::STREAM_SUBSCRIBE, &enc.finish())?;
        let decoded = decode_stream_response("SUBSCRIBE", &resp)?;
        let subscription_id = decoded.session_id.ok_or_else(|| {
            FitzError::Protocol("SUBSCRIBE response missing subscription id".to_string())
        })?;

        Ok(StreamSubscription {
            conn: self.conn.clone(),
            pattern: pattern.to_string(),
            subscription_id,
        })
    }
}

pub struct StreamSession {
    conn: SharedConnection,
    session_id: u64,
    active: bool,
}

impl StreamSession {
    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    pub fn append(
        &mut self,
        expected_offset: u64,
        body: &[u8],
        metadata: Option<&[u8]>,
    ) -> Result<Option<u64>> {
        self.ensure_active("APPEND")?;

        let mut enc = PayloadEncoder::new();
        enc.put_u64(self.session_id);
        enc.put_u64(expected_offset);
        enc.put_bytes(body);
        match metadata.filter(|value| !value.is_empty()) {
            Some(value) => {
                enc.put_u8(1);
                enc.put_bytes(value);
            }
            None => {
                enc.put_u8(0);
            }
        }

        let resp = self
            .conn
            .send_request(message_type::STREAM_APPEND, &enc.finish())?;
        let decoded = decode_stream_response("APPEND", &resp)?;

        if decoded.data.len() < 8 {
            return Ok(None);
        }

        let mut dec = PayloadDecoder::new(&decoded.data);
        Ok(Some(dec.get_u64()?))
    }

    pub fn commit(mut self, mode: StreamCommitMode) -> Result<()> {
        self.ensure_active("COMMIT")?;

        let mut enc = PayloadEncoder::new();
        enc.put_u64(self.session_id);
        enc.put_u8(mode as u8);

        let resp = self
            .conn
            .send_request(message_type::STREAM_COMMIT, &enc.finish())?;
        decode_stream_response("COMMIT", &resp)?;
        self.active = false;
        Ok(())
    }

    pub fn rollback(mut self) -> Result<()> {
        self.ensure_active("ROLLBACK")?;

        let mut enc = PayloadEncoder::new();
        enc.put_u64(self.session_id);

        let resp = self
            .conn
            .send_request(message_type::STREAM_ROLLBACK, &enc.finish())?;
        decode_stream_response("ROLLBACK", &resp)?;
        self.active = false;
        Ok(())
    }

    fn ensure_active(&self, operation: &str) -> Result<()> {
        if self.active {
            return Ok(());
        }

        Err(FitzError::Protocol(format!(
            "{operation} cannot be used after session finalization"
        )))
    }
}

pub struct StreamSubscription {
    conn: SharedConnection,
    pattern: String,
    subscription_id: u64,
}

impl StreamSubscription {
    pub fn subscription_id(&self) -> u64 {
        self.subscription_id
    }

    pub fn next(&self) -> Result<StreamCommitNotification> {
        let (_, payload) = self.conn.recv_message_matching(|msg_type, payload| {
            msg_type == message_type::STREAM_NOTIFY
                && decode_stream_notify_subscription_id(payload)
                    .map(|subscription_id| subscription_id == self.subscription_id)
                    .unwrap_or(false)
        })?;

        decode_stream_notify(&payload)
    }

    pub fn unsubscribe(&self) -> Result<()> {
        let mut enc = PayloadEncoder::new();
        enc.put_string(&self.pattern);

        let resp = self
            .conn
            .send_request(message_type::STREAM_UNSUBSCRIBE, &enc.finish())?;
        decode_stream_response("UNSUBSCRIBE", &resp)?;
        Ok(())
    }
}

#[derive(Debug)]
struct StreamResponsePayload {
    session_id: Option<u64>,
    data: Vec<u8>,
}

fn decode_stream_response(operation: &str, buf: &[u8]) -> Result<StreamResponsePayload> {
    let mut dec = PayloadDecoder::new(buf);
    let status = dec.get_u8()?;
    match status {
        0 => {
            if dec.is_empty() {
                return Ok(StreamResponsePayload {
                    session_id: None,
                    data: Vec::new(),
                });
            }

            let has_session_id = dec.get_u8()?;
            let session_id = match has_session_id {
                0 => None,
                1 => Some(dec.get_u64()?),
                other => {
                    return Err(FitzError::Protocol(format!(
                        "{operation} response has invalid optional-id flag: {other}"
                    )));
                }
            };

            let data = if dec.is_empty() {
                Vec::new()
            } else {
                dec.get_bytes()?
            };

            Ok(StreamResponsePayload { session_id, data })
        }
        1 => {
            let message = dec.get_string()?;
            Err(FitzError::DomainError(format!(
                "{operation} failed: {message}"
            )))
        }
        other => Err(FitzError::Protocol(format!(
            "{operation} failed with unknown status byte: {other}"
        ))),
    }
}

fn parse_stream_record(buf: &[u8]) -> Result<Option<StreamRecord>> {
    if buf.is_empty() {
        return Ok(None);
    }

    let mut dec = PayloadDecoder::new(buf);
    Ok(Some(StreamRecord {
        offset: dec.get_u64()?,
        body: dec.get_bytes()?,
    }))
}

fn parse_stream_records(buf: &[u8]) -> Result<Vec<StreamRecord>> {
    if buf.is_empty() {
        return Ok(Vec::new());
    }

    if let Ok(records) = try_parse_count_prefixed_records(buf) {
        return Ok(records);
    }

    parse_flat_records(buf)
}

fn try_parse_count_prefixed_records(buf: &[u8]) -> Result<Vec<StreamRecord>> {
    let mut dec = PayloadDecoder::new(buf);
    let count = dec.get_u32()? as usize;
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        records.push(StreamRecord {
            offset: dec.get_u64()?,
            body: dec.get_bytes()?,
        });
    }

    if !dec.is_empty() {
        return Err(FitzError::Protocol(
            "READ response has trailing bytes".to_string(),
        ));
    }

    Ok(records)
}

fn parse_flat_records(buf: &[u8]) -> Result<Vec<StreamRecord>> {
    let mut dec = PayloadDecoder::new(buf);
    let mut records = Vec::new();
    while !dec.is_empty() {
        records.push(StreamRecord {
            offset: dec.get_u64()?,
            body: dec.get_bytes()?,
        });
    }
    Ok(records)
}

fn decode_stream_notify_subscription_id(payload: &[u8]) -> Result<u64> {
    let mut dec = PayloadDecoder::new(payload);
    dec.get_u64()
}

fn decode_stream_notify(payload: &[u8]) -> Result<StreamCommitNotification> {
    let mut dec = PayloadDecoder::new(payload);
    let _subscription_id = dec.get_u64()?;
    let route = dec.get_string()?;
    let body = dec.get_bytes()?;

    let parsed = serde_json::from_slice::<DecodedStreamNotifyPayload>(&body).unwrap_or_default();

    Ok(StreamCommitNotification {
        route,
        event: parsed.event,
        first_resource_offset: parsed.first_resource_offset,
        last_resource_offset: parsed.last_resource_offset,
        first_area_offset: parsed.first_area_offset,
        last_area_offset: parsed.last_area_offset,
        first_realm_offset: parsed.first_realm_offset,
        last_realm_offset: parsed.last_realm_offset,
        batch_size: parsed.batch_size,
        payload: body,
    })
}

#[derive(Debug, Default, Deserialize)]
struct DecodedStreamNotifyPayload {
    #[serde(default)]
    event: String,
    #[serde(default)]
    first_resource_offset: u64,
    #[serde(default)]
    last_resource_offset: u64,
    #[serde(default)]
    first_area_offset: u64,
    #[serde(default)]
    last_area_offset: u64,
    #[serde(default)]
    first_realm_offset: u64,
    #[serde(default)]
    last_realm_offset: u64,
    #[serde(default)]
    batch_size: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_decode_begin_response_with_session_id() {
        let mut buf = vec![0, 1];
        buf.extend_from_slice(&9u64.to_be_bytes());

        let decoded = decode_stream_response("BEGIN", &buf).unwrap();
        assert_eq!(decoded.session_id, Some(9));
        assert!(decoded.data.is_empty());
    }

    #[test]
    fn should_decode_stream_append_offset_payload() {
        let mut buf = vec![0, 0];
        buf.extend_from_slice(&(8u32).to_be_bytes());
        buf.extend_from_slice(&17u64.to_be_bytes());

        let decoded = decode_stream_response("APPEND", &buf).unwrap();
        let mut dec = PayloadDecoder::new(&decoded.data);
        assert_eq!(dec.get_u64().unwrap(), 17);
    }

    #[test]
    fn should_parse_count_prefixed_stream_records() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(2u32).to_be_bytes());
        buf.extend_from_slice(&1u64.to_be_bytes());
        buf.extend_from_slice(&(1u32).to_be_bytes());
        buf.extend_from_slice(b"a");
        buf.extend_from_slice(&2u64.to_be_bytes());
        buf.extend_from_slice(&(1u32).to_be_bytes());
        buf.extend_from_slice(b"b");

        let records = parse_stream_records(&buf).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].offset, 1);
        assert_eq!(records[1].body, b"b");
    }

    #[test]
    fn should_parse_flat_stream_records_when_count_prefix_is_invalid() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u64.to_be_bytes());
        buf.extend_from_slice(&(1u32).to_be_bytes());
        buf.extend_from_slice(b"a");
        buf.extend_from_slice(&2u64.to_be_bytes());
        buf.extend_from_slice(&(1u32).to_be_bytes());
        buf.extend_from_slice(b"b");

        let records = parse_stream_records(&buf).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].body, b"a");
        assert_eq!(records[1].offset, 2);
    }

    #[test]
    fn should_decode_stream_notify_payload() {
        let payload = br#"{"event":"committed","first_resource_offset":4,"last_resource_offset":5,"first_area_offset":7,"last_area_offset":8,"first_realm_offset":9,"last_realm_offset":10,"batch_size":2}"#;

        let mut buf = Vec::new();
        buf.extend_from_slice(&42u64.to_be_bytes());
        buf.extend_from_slice(&(21u32).to_be_bytes());
        buf.extend_from_slice(b"stream://realm/area/x");
        buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(payload);

        let notification = decode_stream_notify(&buf).unwrap();
        assert_eq!(notification.route, "stream://realm/area/x");
        assert_eq!(notification.event, "committed");
        assert_eq!(notification.last_resource_offset, 5);
        assert_eq!(notification.batch_size, 2);
    }

    #[test]
    fn should_decode_stream_error_response() {
        let mut buf = vec![1];
        buf.extend_from_slice(&(4u32).to_be_bytes());
        buf.extend_from_slice(b"nope");

        let err = decode_stream_response("READ", &buf).unwrap_err();
        assert!(err.to_string().contains("nope"));
    }
}
