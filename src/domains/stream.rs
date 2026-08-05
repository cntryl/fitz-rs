//! Stream domain client.

use crate::codec::{PayloadDecoder, PayloadEncoder};
use crate::domains::routes::validate_response_fixed_route;
use crate::error::{FitzError, Result};
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
    pub route: String,
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
        route: String,
        offset: u64,
        reason: Option<StreamFilteredReason>,
    },
    FilteredRange {
        route: String,
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
    pub current_realm: Option<String>,
    pub last_global_offset: Option<u64>,
    pub cursor_fingerprint: Option<u64>,
    pub captured_watermark: Option<u64>,
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

pub(crate) fn encode_stream_filter_set(filter: &StreamFilterSet) -> Result<Vec<u8>> {
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

#[derive(Debug)]
pub(crate) struct StreamResponsePayload {
    pub(crate) session_id: Option<u64>,
    pub(crate) data: Vec<u8>,
}

pub(crate) fn decode_stream_response(operation: &str, buf: &[u8]) -> Result<StreamResponsePayload> {
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

pub(crate) fn parse_stream_last_response(buf: &[u8]) -> Result<Option<StreamRecord>> {
    if buf.is_empty() {
        return Ok(None);
    }

    let mut dec = PayloadDecoder::new(buf);
    let route = dec.get_string()?;
    validate_response_fixed_route(&route, "stream", 3, "LAST")?;
    let record = decode_stream_record(&mut dec, &route)?;
    if !dec.is_empty() {
        return Err(FitzError::Protocol(
            "LAST response has trailing bytes".to_string(),
        ));
    }
    Ok(Some(record))
}

#[cfg(test)]
pub(crate) fn parse_stream_read_page(buf: &[u8]) -> Result<StreamReadPage> {
    parse_stream_read_page_with_scope(buf, false)
}

pub(crate) fn parse_stream_read_page_with_scope(
    buf: &[u8],
    global: bool,
) -> Result<StreamReadPage> {
    if buf.is_empty() {
        return Ok(StreamReadPage {
            items: Vec::new(),
            cursor: StreamReadCursor {
                last_resource_offset: 0,
                last_area_offset: None,
                last_realm_offset: None,
                current_realm: None,
                last_global_offset: None,
                cursor_fingerprint: None,
                captured_watermark: None,
                has_more: false,
            },
        });
    }

    let mut dec = PayloadDecoder::new(buf);
    let count = dec.get_u32()? as usize;
    let mut items = Vec::with_capacity(count);

    for _ in 0..count {
        let route = dec.get_string()?;
        validate_response_fixed_route(&route, "stream", 3, "READ")?;
        items.push(decode_stream_read_item(&mut dec, route)?);
    }

    let cursor = StreamReadCursor {
        last_resource_offset: dec.get_u64()?,
        last_area_offset: decode_optional_u64(&mut dec)?,
        last_realm_offset: decode_optional_u64(&mut dec)?,
        current_realm: decode_optional_string(&mut dec)?,
        last_global_offset: if global {
            decode_optional_u64(&mut dec)?
        } else {
            None
        },
        has_more: decode_bool_u8(&mut dec, "stream read cursor has_more")?,
        cursor_fingerprint: if global {
            decode_optional_u64(&mut dec)?
        } else {
            None
        },
        captured_watermark: if global {
            decode_optional_u64(&mut dec)?
        } else {
            None
        },
    };

    if !dec.is_empty() {
        return Err(FitzError::Protocol(
            "READ response has trailing bytes".to_string(),
        ));
    }

    Ok(StreamReadPage { items, cursor })
}

pub(crate) fn flatten_stream_read_items(items: &[StreamReadItem]) -> Vec<StreamRecord> {
    items
        .iter()
        .filter_map(|item| match item {
            StreamReadItem::Event(record) => Some(record.clone()),
            StreamReadItem::Filtered { .. } | StreamReadItem::FilteredRange { .. } => None,
        })
        .collect()
}

pub(crate) fn decode_stream_metadata(buf: &[u8]) -> Result<StreamMetadata> {
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

fn decode_stream_record(dec: &mut PayloadDecoder<'_>, route: &str) -> Result<StreamRecord> {
    Ok(StreamRecord {
        route: route.to_string(),
        offset: dec.get_u64()?,
        area_offset: decode_optional_u64(dec)?,
        realm_offset: decode_optional_u64(dec)?,
        body: dec.get_bytes()?,
        metadata: decode_optional_bytes(dec)?,
        timestamp: dec.get_u64()?,
    })
}

fn decode_stream_read_item(dec: &mut PayloadDecoder<'_>, route: String) -> Result<StreamReadItem> {
    match dec.get_u8()? {
        0 => Ok(StreamReadItem::Event(decode_stream_record(dec, &route)?)),
        1 => Ok(StreamReadItem::Filtered {
            route,
            offset: dec.get_u64()?,
            reason: decode_stream_filtered_reason(dec)?,
        }),
        2 => Ok(StreamReadItem::FilteredRange {
            route,
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

fn decode_optional_string(dec: &mut PayloadDecoder<'_>) -> Result<Option<String>> {
    match dec.get_u8()? {
        0 => Ok(None),
        1 => dec.get_string().map(Some),
        other => Err(FitzError::Protocol(format!(
            "invalid optional string flag: {other}"
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

pub(crate) fn decode_stream_notify(payload: &[u8]) -> Result<StreamCommitNotification> {
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
#[path = "stream_tests.rs"]
mod tests;
