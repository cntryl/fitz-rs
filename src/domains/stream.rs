//! Stream domain client.

use bincode::serialize;
use crate::codec::{PayloadDecoder, PayloadEncoder};
use crate::connection::SharedConnection;
use crate::domains::routes::{validate_fixed_route, validate_selector_route};
use crate::error::{FitzError, Result};
use crate::protocol::message_type;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamCommitMode {
    Buffered = 0,
    Sync = 1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StreamDiscriminator(pub String);

impl StreamDiscriminator {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for StreamDiscriminator {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<String> for StreamDiscriminator {
    fn from(value: String) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamFilterClause {
    Equals(String),
    NotEquals(String),
    StartsWith(String),
    AnyOf(Vec<String>),
}

impl StreamFilterClause {
    fn matches(&self, discriminator: &str) -> bool {
        match self {
            Self::Equals(value) => discriminator == value,
            Self::NotEquals(value) => discriminator != value,
            Self::StartsWith(prefix) => discriminator.starts_with(prefix),
            Self::AnyOf(values) => values.iter().any(|value| value == discriminator),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct StreamFilterSet {
    pub clauses: Vec<StreamFilterClause>,
}

impl StreamFilterSet {
    pub fn is_empty(&self) -> bool {
        self.clauses.is_empty()
    }

    pub fn matches(&self, discriminator: Option<&str>) -> bool {
        let discriminator = discriminator.unwrap_or("");
        self.clauses.iter().all(|clause| clause.matches(discriminator))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamRecord {
    pub offset: u64,
    pub body: Vec<u8>,
    pub area_offset: Option<u64>,
    pub realm_offset: Option<u64>,
    pub metadata: Option<Vec<u8>>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamMetadata {
    pub first_offset: u64,
    pub last_offset: u64,
    pub record_count: u64,
    pub max_batch_events: u64,
    pub max_batch_bytes: u64,
    pub ttl_seconds: Option<u64>,
    pub area_watermark: u64,
    pub realm_watermark: u64,
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
        filter: Option<&StreamFilterSet>,
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

        match filter.filter(|filter| !filter.is_empty()) {
            Some(filter) => {
                enc.put_u8(1);
                let filter_bytes = serialize(filter).map_err(|error| {
                    FitzError::Protocol(format!("failed to encode stream filter: {error}"))
                })?;
                enc.put_bytes(&filter_bytes);
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
                max_batch_events: 0,
                max_batch_bytes: 0,
                ttl_seconds: None,
                area_watermark: 0,
                realm_watermark: 0,
            });
        }

        decode_stream_metadata(&decoded.data)
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
        discriminator: Option<&StreamDiscriminator>,
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

        match discriminator.filter(|value| !value.as_str().is_empty()) {
            Some(value) => {
                enc.put_u8(1);
                enc.put_string(value.as_str());
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
    Ok(Some(decode_stream_record(&mut dec)?))
}

fn parse_stream_records(buf: &[u8]) -> Result<Vec<StreamRecord>> {
    if buf.is_empty() {
        return Ok(Vec::new());
    }

    let mut dec = PayloadDecoder::new(buf);
    let count = dec.get_u32()? as usize;
    let mut records = Vec::with_capacity(count);

    for _ in 0..count {
        records.push(decode_stream_record(&mut dec)?);
    }

    let _last_resource_offset = dec.get_u64()?;
    let _last_area_offset = decode_optional_u64(&mut dec)?;
    let _last_realm_offset = decode_optional_u64(&mut dec)?;
    let _has_more = dec.get_u8()?;

    if !dec.is_empty() {
        return Err(FitzError::Protocol("READ response has trailing bytes".to_string()));
    }

    Ok(records)
}

fn decode_stream_metadata(buf: &[u8]) -> Result<StreamMetadata> {
    let mut dec = PayloadDecoder::new(buf);
    Ok(StreamMetadata {
        first_offset: decode_optional_u64(&mut dec)?.unwrap_or(0),
        last_offset: decode_optional_u64(&mut dec)?.unwrap_or(0),
        record_count: dec.get_u64()?,
        max_batch_events: dec.get_u64()?,
        max_batch_bytes: dec.get_u64()?,
        ttl_seconds: decode_optional_u64(&mut dec)?,
        area_watermark: dec.get_u64()?,
        realm_watermark: dec.get_u64()?,
    })
}

fn decode_stream_record(dec: &mut PayloadDecoder<'_>) -> Result<StreamRecord> {
    Ok(StreamRecord {
        offset: dec.get_u64()?,
        area_offset: decode_optional_u64(dec)?,
        realm_offset: decode_optional_u64(dec)?,
        body: dec.get_bytes()?,
        metadata: decode_optional_bytes(dec)?,
        timestamp: dec.get_u64()?,
    })
}

fn decode_optional_u64(dec: &mut PayloadDecoder<'_>) -> Result<Option<u64>> {
    match dec.get_u8()? {
        0 => Ok(None),
        1 => Ok(Some(dec.get_u64()?)),
        other => Err(FitzError::Protocol(format!(
            "invalid optional u64 flag: {other}"
        ))),
    }
}

fn decode_optional_bytes(dec: &mut PayloadDecoder<'_>) -> Result<Option<Vec<u8>>> {
    match dec.get_u8()? {
        0 => Ok(None),
        1 => Ok(Some(dec.get_bytes()?)),
        other => Err(FitzError::Protocol(format!(
            "invalid optional bytes flag: {other}"
        ))),
    }
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
        buf.push(1);
        buf.extend_from_slice(&10u64.to_be_bytes());
        buf.push(1);
        buf.extend_from_slice(&20u64.to_be_bytes());
        buf.extend_from_slice(&(1u32).to_be_bytes());
        buf.extend_from_slice(b"a");
        buf.push(1);
        buf.extend_from_slice(&(2u32).to_be_bytes());
        buf.extend_from_slice(b"m1");
        buf.extend_from_slice(&111u64.to_be_bytes());
        buf.extend_from_slice(&2u64.to_be_bytes());
        buf.push(0);
        buf.push(1);
        buf.extend_from_slice(&21u64.to_be_bytes());
        buf.extend_from_slice(&(1u32).to_be_bytes());
        buf.extend_from_slice(b"b");
        buf.push(0);
        buf.extend_from_slice(&222u64.to_be_bytes());
        buf.extend_from_slice(&2u64.to_be_bytes());
        buf.push(0);
        buf.push(0);
        buf.push(0);

        let records = parse_stream_records(&buf).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].offset, 1);
        assert_eq!(records[0].area_offset, Some(10));
        assert_eq!(records[0].realm_offset, Some(20));
        assert_eq!(records[0].metadata.as_deref(), Some(&b"m1"[..]));
        assert_eq!(records[0].timestamp, 111);
        assert_eq!(records[1].body, b"b");
        assert_eq!(records[1].offset, 2);
        assert_eq!(records[1].timestamp, 222);
    }

    #[test]
    fn should_parse_full_stream_record() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u64.to_be_bytes());
        buf.push(1);
        buf.extend_from_slice(&2u64.to_be_bytes());
        buf.push(1);
        buf.extend_from_slice(&3u64.to_be_bytes());
        buf.extend_from_slice(&(4u32).to_be_bytes());
        buf.extend_from_slice(b"body");
        buf.push(1);
        buf.extend_from_slice(&(4u32).to_be_bytes());
        buf.extend_from_slice(b"meta");
        buf.extend_from_slice(&5u64.to_be_bytes());

        let record = parse_stream_record(&buf).unwrap().unwrap();
        assert_eq!(record.offset, 1);
        assert_eq!(record.area_offset, Some(2));
        assert_eq!(record.realm_offset, Some(3));
        assert_eq!(record.body, b"body");
        assert_eq!(record.metadata.as_deref(), Some(&b"meta"[..]));
        assert_eq!(record.timestamp, 5);
    }

    #[test]
    fn should_decode_stream_metadata_payload() {
        let mut buf = Vec::new();
        buf.push(1);
        buf.extend_from_slice(&4u64.to_be_bytes());
        buf.push(1);
        buf.extend_from_slice(&9u64.to_be_bytes());
        buf.extend_from_slice(&2u64.to_be_bytes());
        buf.extend_from_slice(&100u64.to_be_bytes());
        buf.extend_from_slice(&200u64.to_be_bytes());
        buf.push(1);
        buf.extend_from_slice(&300u64.to_be_bytes());
        buf.extend_from_slice(&7u64.to_be_bytes());
        buf.extend_from_slice(&8u64.to_be_bytes());

        let metadata = decode_stream_metadata(&buf).unwrap();
        assert_eq!(metadata.first_offset, 4);
        assert_eq!(metadata.last_offset, 9);
        assert_eq!(metadata.record_count, 2);
        assert_eq!(metadata.max_batch_events, 100);
        assert_eq!(metadata.max_batch_bytes, 200);
        assert_eq!(metadata.ttl_seconds, Some(300));
        assert_eq!(metadata.area_watermark, 7);
        assert_eq!(metadata.realm_watermark, 8);
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

    #[test]
    fn should_roundtrip_stream_filter_set_with_bincode() {
        let filter = StreamFilterSet {
            clauses: vec![
                StreamFilterClause::Equals("proj.alpha".to_string()),
                StreamFilterClause::NotEquals("audit.beta".to_string()),
                StreamFilterClause::StartsWith("proj.".to_string()),
                StreamFilterClause::AnyOf(vec!["proj.alpha".to_string(), "proj.gamma".to_string()]),
            ],
        };

        let encoded = serialize(&filter).unwrap();
        let decoded: StreamFilterSet = bincode::deserialize(&encoded).unwrap();

        assert_eq!(decoded, filter);
    }

    #[test]
    fn should_match_missing_discriminator_as_empty_string() {
        let filter = StreamFilterSet {
            clauses: vec![StreamFilterClause::Equals(String::new())],
        };

        assert!(filter.matches(None));
        assert!(!filter.matches(Some("proj.alpha")));
    }
}
