//! Stream domain client.

use crate::codec::{PayloadDecoder, PayloadEncoder};
use crate::connection::SharedConnection;
use crate::domains::routes::{
    validate_fixed_route, validate_registration_pattern, validate_selector_route,
};
use crate::error::{FitzError, Result};
use crate::protocol::message_type;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StreamCommitMode {
    Buffered = 0,
    Sync = 1,
}

/// Opaque stream discriminator used by server-side filtering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StreamDiscriminator(pub String);

impl StreamDiscriminator {
    #[must_use]
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
#[non_exhaustive]
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

/// Set of discriminator filter clauses applied to stream reads.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct StreamFilterSet {
    pub clauses: Vec<StreamFilterClause>,
}

impl StreamFilterSet {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.clauses.is_empty()
    }

    #[must_use]
    pub fn matches(&self, discriminator: Option<&str>) -> bool {
        let discriminator = discriminator.unwrap_or("");
        self.clauses
            .iter()
            .all(|clause| clause.matches(discriminator))
    }
}

/// Stream record payload returned by read and peek operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamRecord {
    pub offset: u64,
    pub body: Vec<u8>,
    pub area_offset: Option<u64>,
    pub realm_offset: Option<u64>,
    pub metadata: Option<Vec<u8>>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum StreamFilteredReason {
    ServerFilter,
    Permission,
    Projection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum StreamReadItem {
    Event(StreamRecord),
    Filtered {
        offset: u64,
        reason: Option<StreamFilteredReason>,
    },
    FilteredRange {
        from_offset: u64,
        to_offset: u64,
        reason: Option<StreamFilteredReason>,
    },
}

/// Cursor information describing the current stream read position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamReadCursor {
    pub last_resource_offset: u64,
    pub last_area_offset: Option<u64>,
    pub last_realm_offset: Option<u64>,
    pub has_more: bool,
}

/// Page of stream read items with cursor continuation state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamReadPage {
    pub items: Vec<StreamReadItem>,
    pub cursor: StreamReadCursor,
}

/// Stream metadata summary returned by the metadata API.
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

/// Notification payload emitted for committed stream batches.
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

/// Stream domain client for session lifecycle, reads, and subscriptions.
pub struct StreamClient {
    conn: SharedConnection,
}

impl StreamClient {
    #[must_use]
    pub fn new(conn: SharedConnection) -> Self {
        Self { conn }
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn begin(&self, route: &str, ingest_metadata: Option<&[u8]>) -> Result<StreamSession> {
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

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
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
                let filter_bytes = encode_stream_filter_set(filter)?;
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
        Ok(flatten_stream_read_items(
            &parse_stream_read_page(&decoded.data)?.items,
        ))
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn read_page(
        &self,
        route: &str,
        start_offset: u64,
        limit: u64,
        max_bytes: Option<u64>,
        filter: Option<&StreamFilterSet>,
    ) -> Result<StreamReadPage> {
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
                let filter_bytes = encode_stream_filter_set(filter)?;
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
        parse_stream_read_page(&decoded.data)
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
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

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
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

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn subscribe(&self, pattern: &str) -> Result<StreamSubscription> {
        validate_registration_pattern(pattern, "stream", 3)?;

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

fn encode_stream_filter_set(filter: &StreamFilterSet) -> Result<Vec<u8>> {
    let mut enc = PayloadEncoder::new();
    enc.put_u8(0);
    enc.put_u8(0xF1);
    enc.put_u32(
        u32::try_from(filter.clauses.len())
            .map_err(|_| FitzError::Protocol("too many stream filter clauses".into()))?,
    );
    for clause in &filter.clauses {
        match clause {
            StreamFilterClause::Equals(value) => {
                enc.put_u8(0);
                enc.put_string(value);
            }
            StreamFilterClause::NotEquals(value) => {
                enc.put_u8(1);
                enc.put_string(value);
            }
            StreamFilterClause::StartsWith(value) => {
                enc.put_u8(2);
                enc.put_string(value);
            }
            StreamFilterClause::AnyOf(values) => {
                enc.put_u8(3);
                enc.put_u32(
                    u32::try_from(values.len())
                        .map_err(|_| FitzError::Protocol("too many AnyOf values".into()))?,
                );
                for value in values {
                    enc.put_string(value);
                }
            }
        }
    }
    Ok(enc.finish())
}

pub struct StreamSession {
    conn: SharedConnection,
    session_id: u64,
    active: bool,
}

impl StreamSession {
    #[must_use]
    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
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

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
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

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
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
    #[must_use]
    pub fn subscription_id(&self) -> u64 {
        self.subscription_id
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn next(&self) -> Result<StreamCommitNotification> {
        let (_, payload) = self.conn.recv_message_matching(|msg_type, payload| {
            msg_type == message_type::STREAM_NOTIFY
                && decode_stream_notify_subscription_id(payload)
                    .is_ok_and(|subscription_id| subscription_id == self.subscription_id)
        })?;

        decode_stream_notify(&payload)
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
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
            let code = dec.get_u32()?;
            let message = dec.get_string()?;
            Err(FitzError::Domain {
                code,
                message: format!("{operation} failed: {message}"),
            })
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

fn parse_stream_read_page(buf: &[u8]) -> Result<StreamReadPage> {
    if buf.is_empty() {
        return Ok(StreamReadPage {
            items: Vec::new(),
            cursor: StreamReadCursor {
                last_resource_offset: 0,
                last_area_offset: None,
                last_realm_offset: None,
                has_more: false,
            },
        });
    }

    let mut dec = PayloadDecoder::new(buf);
    let count = dec.get_u32()? as usize;
    let mut items = Vec::with_capacity(count);

    for _ in 0..count {
        items.push(decode_stream_read_item(&mut dec)?);
    }

    let cursor = StreamReadCursor {
        last_resource_offset: dec.get_u64()?,
        last_area_offset: decode_optional_u64(&mut dec)?,
        last_realm_offset: decode_optional_u64(&mut dec)?,
        has_more: decode_bool_u8(&mut dec, "stream read cursor has_more")?,
    };

    if !dec.is_empty() {
        return Err(FitzError::Protocol(
            "READ response has trailing bytes".to_string(),
        ));
    }

    Ok(StreamReadPage { items, cursor })
}

fn flatten_stream_read_items(items: &[StreamReadItem]) -> Vec<StreamRecord> {
    items
        .iter()
        .filter_map(|item| match item {
            StreamReadItem::Event(record) => Some(record.clone()),
            StreamReadItem::Filtered { .. } | StreamReadItem::FilteredRange { .. } => None,
        })
        .collect()
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

fn decode_stream_read_item(dec: &mut PayloadDecoder<'_>) -> Result<StreamReadItem> {
    match dec.get_u8()? {
        0 => Ok(StreamReadItem::Event(decode_stream_record(dec)?)),
        1 => Ok(StreamReadItem::Filtered {
            offset: dec.get_u64()?,
            reason: decode_stream_filtered_reason(dec)?,
        }),
        2 => Ok(StreamReadItem::FilteredRange {
            from_offset: dec.get_u64()?,
            to_offset: dec.get_u64()?,
            reason: decode_stream_filtered_reason(dec)?,
        }),
        other => Err(FitzError::Protocol(format!(
            "unknown stream read item tag: {other}"
        ))),
    }
}

fn decode_stream_filtered_reason(
    dec: &mut PayloadDecoder<'_>,
) -> Result<Option<StreamFilteredReason>> {
    match dec.get_u8()? {
        0 => Ok(None),
        1 => Ok(Some(StreamFilteredReason::ServerFilter)),
        2 => Ok(Some(StreamFilteredReason::Permission)),
        3 => Ok(Some(StreamFilteredReason::Projection)),
        other => Err(FitzError::Protocol(format!(
            "invalid stream filtered reason tag: {other}"
        ))),
    }
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

fn decode_bool_u8(dec: &mut PayloadDecoder<'_>, field: &str) -> Result<bool> {
    match dec.get_u8()? {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(FitzError::Protocol(format!(
            "invalid boolean flag for {field}: {other}"
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
        // Arrange
        let mut buf = vec![0, 1];
        buf.extend_from_slice(&9u64.to_be_bytes());

        // Act
        let decoded = decode_stream_response("BEGIN", &buf).unwrap();
        // Assert
        assert_eq!(decoded.session_id, Some(9));
        assert!(decoded.data.is_empty());
    }

    #[test]
    fn should_decode_stream_append_offset_payload() {
        // Arrange
        let mut buf = vec![0, 0];
        buf.extend_from_slice(&(8u32).to_be_bytes());
        buf.extend_from_slice(&17u64.to_be_bytes());

        let decoded = decode_stream_response("APPEND", &buf).unwrap();
        // Act
        let mut dec = PayloadDecoder::new(&decoded.data);
        // Assert
        assert_eq!(dec.get_u64().unwrap(), 17);
    }

    #[test]
    fn should_parse_count_prefixed_stream_records() {
        // Arrange
        let mut buf = Vec::new();
        buf.extend_from_slice(&(2u32).to_be_bytes());
        buf.push(0);
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
        buf.push(0);
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

        let page = parse_stream_read_page(&buf).unwrap();
        // Act
        let records = flatten_stream_read_items(&page.items);
        // Assert
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].offset, 1);
        assert_eq!(records[0].area_offset, Some(10));
        assert_eq!(records[0].realm_offset, Some(20));
        assert_eq!(records[0].metadata.as_deref(), Some(&b"m1"[..]));
        assert_eq!(records[0].timestamp, 111);
        assert_eq!(records[1].body, b"b");
        assert_eq!(records[1].offset, 2);
        assert_eq!(records[1].timestamp, 222);
        assert_eq!(page.cursor.last_resource_offset, 2);
        assert_eq!(page.cursor.last_area_offset, None);
        assert_eq!(page.cursor.last_realm_offset, None);
        assert!(!page.cursor.has_more);
    }

    #[test]
    fn should_parse_filtered_stream_read_page() {
        // Arrange
        let mut buf = Vec::new();
        buf.extend_from_slice(&(3u32).to_be_bytes());
        buf.push(0);
        buf.extend_from_slice(&41u64.to_be_bytes());
        buf.push(1);
        buf.extend_from_slice(&51u64.to_be_bytes());
        buf.push(0);
        buf.extend_from_slice(&(1u32).to_be_bytes());
        buf.extend_from_slice(b"a");
        buf.push(0);
        buf.extend_from_slice(&111u64.to_be_bytes());
        buf.push(1);
        buf.extend_from_slice(&42u64.to_be_bytes());
        buf.push(1);
        buf.push(2);
        buf.extend_from_slice(&43u64.to_be_bytes());
        buf.extend_from_slice(&45u64.to_be_bytes());
        buf.push(2);
        buf.extend_from_slice(&45u64.to_be_bytes());
        buf.push(1);
        buf.extend_from_slice(&52u64.to_be_bytes());
        buf.push(0);
        buf.push(1);

        // Act
        let page = parse_stream_read_page(&buf).unwrap();

        // Assert
        assert_eq!(page.cursor.last_resource_offset, 45);
        assert_eq!(page.cursor.last_area_offset, Some(52));
        assert_eq!(page.cursor.last_realm_offset, None);
        assert!(page.cursor.has_more);
        assert_eq!(page.items.len(), 3);
        assert!(matches!(page.items[0], StreamReadItem::Event(_)));
        assert_eq!(
            page.items[1],
            StreamReadItem::Filtered {
                offset: 42,
                reason: Some(StreamFilteredReason::ServerFilter),
            }
        );
        assert_eq!(
            page.items[2],
            StreamReadItem::FilteredRange {
                from_offset: 43,
                to_offset: 45,
                reason: Some(StreamFilteredReason::Permission),
            }
        );
        let records = flatten_stream_read_items(&page.items);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].offset, 41);
    }

    #[test]
    fn should_parse_full_stream_record() {
        // Arrange
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

        // Act
        let record = parse_stream_record(&buf).unwrap().unwrap();
        // Assert
        assert_eq!(record.offset, 1);
        assert_eq!(record.area_offset, Some(2));
        assert_eq!(record.realm_offset, Some(3));
        assert_eq!(record.body, b"body");
        assert_eq!(record.metadata.as_deref(), Some(&b"meta"[..]));
        assert_eq!(record.timestamp, 5);
    }

    #[test]
    fn should_decode_stream_metadata_payload() {
        // Arrange
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

        // Act
        let metadata = decode_stream_metadata(&buf).unwrap();
        // Assert
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
        // Arrange
        let payload = br#"{"event":"committed","first_resource_offset":4,"last_resource_offset":5,"first_area_offset":7,"last_area_offset":8,"first_realm_offset":9,"last_realm_offset":10,"batch_size":2}"#;

        let mut buf = Vec::new();
        buf.extend_from_slice(&42u64.to_be_bytes());
        buf.extend_from_slice(&(21u32).to_be_bytes());
        buf.extend_from_slice(b"stream://realm/area/x");
        buf.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
        buf.extend_from_slice(payload);

        // Act
        let notification = decode_stream_notify(&buf).unwrap();
        // Assert
        assert_eq!(notification.route, "stream://realm/area/x");
        assert_eq!(notification.event, "committed");
        assert_eq!(notification.last_resource_offset, 5);
        assert_eq!(notification.batch_size, 2);
    }

    #[test]
    fn should_decode_stream_error_response() {
        // Arrange
        let mut buf = vec![1];
        buf.extend_from_slice(&2010u32.to_be_bytes());
        buf.extend_from_slice(&(4u32).to_be_bytes());
        buf.extend_from_slice(b"nope");

        // Act
        let err = decode_stream_response("READ", &buf).unwrap_err();
        // Assert
        assert!(err.to_string().contains("nope"));
        assert!(matches!(err, FitzError::Domain { code: 2010, .. }));
    }

    #[test]
    fn should_encode_canonical_stream_filter_set() {
        // Arrange
        let filter = StreamFilterSet {
            clauses: vec![
                StreamFilterClause::Equals("proj.alpha".to_string()),
                StreamFilterClause::NotEquals("audit.beta".to_string()),
                StreamFilterClause::StartsWith("proj.".to_string()),
                StreamFilterClause::AnyOf(vec!["proj.alpha".to_string(), "proj.gamma".to_string()]),
            ],
        };

        // Act
        let encoded = encode_stream_filter_set(&filter).unwrap();
        // Assert
        assert_eq!(&encoded[..2], &[0, 0xF1]);
        assert_eq!(&encoded[2..6], &4_u32.to_be_bytes());
    }

    #[test]
    fn should_match_missing_discriminator_as_empty_string() {
        // Arrange
        let filter = StreamFilterSet {
            clauses: vec![StreamFilterClause::Equals(String::new())],
        };

        // Act
        let matches_missing = filter.matches(None);
        let matches_present = filter.matches(Some("proj.alpha"));

        // Assert
        assert!(matches_missing);
        assert!(!matches_present);
    }
}
