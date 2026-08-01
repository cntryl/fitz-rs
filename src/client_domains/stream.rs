use crate::async_connection::{AsyncConnection, RestorableRegistration};
use crate::codec::{PayloadDecoder, PayloadEncoder};
use crate::domains::routes::{
    validate_fixed_route, validate_registration_pattern, validate_selector_route,
};
pub use crate::domains::stream::{
    StreamCommitMode, StreamCommitNotification, StreamDiscriminator, StreamFilterClause,
    StreamFilterSet, StreamMetadata, StreamReadPage, StreamRecord,
};
use crate::domains::stream::{
    decode_stream_metadata, decode_stream_notify, decode_stream_response, encode_stream_filter_set,
    flatten_stream_read_items, parse_stream_read_page, parse_stream_record,
};
use crate::protocol::message_type;
use crate::{FitzError, Result};
use futures_core::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_stream::wrappers::BroadcastStream;
#[derive(Clone)]
pub struct StreamClient {
    connection: AsyncConnection,
}
impl StreamClient {
    pub(crate) fn new(connection: AsyncConnection) -> Self {
        Self { connection }
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn begin(
        &self,
        route: &str,
        ingest_metadata: Option<&[u8]>,
    ) -> Result<StreamSession> {
        validate_fixed_route(route, "stream", 3)?;
        let mut e = PayloadEncoder::new();
        e.put_string(route);
        optional_bytes(&mut e, ingest_metadata.filter(|v| !v.is_empty()));
        let response = self
            .connection
            .request(message_type::STREAM_BEGIN, e.finish())
            .await?;
        let decoded = decode_stream_response("BEGIN", &response)?;
        Ok(StreamSession {
            connection: self.connection.clone(),
            generation: self.connection.generation(),
            session_id: decoded
                .session_id
                .ok_or_else(|| FitzError::Protocol("BEGIN response missing session id".into()))?,
            active: true,
        })
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn read_page(
        &self,
        route: &str,
        start_offset: u64,
        limit: u64,
        max_bytes: Option<u64>,
        filter: Option<&StreamFilterSet>,
    ) -> Result<StreamReadPage> {
        validate_selector_route(route, "stream", 3, true)?;
        let mut e = PayloadEncoder::new();
        e.put_string(route).put_u64(start_offset).put_u64(limit);
        optional_u64(&mut e, max_bytes);
        if let Some(filter) = filter.filter(|v| !v.is_empty()) {
            e.put_u8(1).put_bytes(&encode_stream_filter_set(filter)?);
        } else {
            e.put_u8(0);
        }
        let response = self
            .connection
            .request(message_type::STREAM_READ, e.finish())
            .await?;
        parse_stream_read_page(&decode_stream_response("READ", &response)?.data)
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn read(
        &self,
        route: &str,
        start_offset: u64,
        limit: u64,
        max_bytes: Option<u64>,
        filter: Option<&StreamFilterSet>,
    ) -> Result<Vec<StreamRecord>> {
        Ok(flatten_stream_read_items(
            &self
                .read_page(route, start_offset, limit, max_bytes, filter)
                .await?
                .items,
        ))
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn peek(&self, route: &str) -> Result<Option<StreamRecord>> {
        validate_fixed_route(route, "stream", 3)?;
        let mut e = PayloadEncoder::new();
        e.put_string(route);
        let response = self
            .connection
            .request(message_type::STREAM_LAST, e.finish())
            .await?;
        parse_stream_record(&decode_stream_response("LAST", &response)?.data)
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn metadata(&self, route: &str) -> Result<StreamMetadata> {
        validate_fixed_route(route, "stream", 3)?;
        let mut e = PayloadEncoder::new();
        e.put_string(route);
        let response = self
            .connection
            .request(message_type::STREAM_GET_METADATA, e.finish())
            .await?;
        let data = decode_stream_response("METADATA", &response)?.data;
        if data.is_empty() {
            Ok(StreamMetadata {
                first_offset: 0,
                last_offset: 0,
                record_count: 0,
                max_batch_events: 0,
                max_batch_bytes: 0,
                ttl_seconds: None,
                area_watermark: 0,
                realm_watermark: 0,
            })
        } else {
            decode_stream_metadata(&data)
        }
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn subscribe(&self, pattern: &str) -> Result<StreamSubscription> {
        validate_registration_pattern(pattern, "stream", 3)?;
        let receiver = self
            .connection
            .notifications(message_type::STREAM_NOTIFY, 64);
        let mut e = PayloadEncoder::new();
        e.put_string(pattern);
        let payload = e.finish();
        let response = self
            .connection
            .request(message_type::STREAM_SUBSCRIBE, payload.clone())
            .await?;
        let subscription_id = decode_subscription_id(&response)?;
        let registration = self.connection.register_restorable(
            message_type::STREAM_SUBSCRIBE,
            payload,
            subscription_id,
            decode_subscription_id,
        );
        Ok(StreamSubscription {
            connection: self.connection.clone(),
            pattern: pattern.into(),
            registration,
            receiver: BroadcastStream::new(receiver),
            closed: false,
        })
    }
}
pub struct StreamSession {
    connection: AsyncConnection,
    generation: u64,
    session_id: u64,
    active: bool,
}
impl StreamSession {
    fn current(&self) -> Result<()> {
        if self.active && self.connection.generation() == self.generation {
            Ok(())
        } else {
            Err(FitzError::StaleHandle)
        }
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn append(
        &mut self,
        expected_offset: u64,
        body: &[u8],
        metadata: Option<&[u8]>,
        discriminator: Option<&StreamDiscriminator>,
    ) -> Result<Option<u64>> {
        self.current()?;
        let mut e = PayloadEncoder::new();
        e.put_u64(self.session_id)
            .put_u64(expected_offset)
            .put_bytes(body);
        optional_bytes(&mut e, metadata.filter(|v| !v.is_empty()));
        if let Some(value) = discriminator.filter(|v| !v.as_str().is_empty()) {
            e.put_u8(1).put_string(value.as_str());
        } else {
            e.put_u8(0);
        }
        let response = self
            .connection
            .request(message_type::STREAM_APPEND, e.finish())
            .await?;
        let data = decode_stream_response("APPEND", &response)?.data;
        if data.len() < 8 {
            Ok(None)
        } else {
            Ok(Some(PayloadDecoder::new(&data).get_u64()?))
        }
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn commit(mut self, mode: StreamCommitMode) -> Result<()> {
        self.current()?;
        let mut e = PayloadEncoder::new();
        e.put_u64(self.session_id).put_u8(mode as u8);
        decode_stream_response(
            "COMMIT",
            &self
                .connection
                .request(message_type::STREAM_COMMIT, e.finish())
                .await?,
        )?;
        self.active = false;
        Ok(())
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn rollback(mut self) -> Result<()> {
        self.current()?;
        let mut e = PayloadEncoder::new();
        e.put_u64(self.session_id);
        decode_stream_response(
            "ROLLBACK",
            &self
                .connection
                .request(message_type::STREAM_ROLLBACK, e.finish())
                .await?,
        )?;
        self.active = false;
        Ok(())
    }
}
pub struct StreamSubscription {
    connection: AsyncConnection,
    pattern: String,
    registration: RestorableRegistration,
    receiver: BroadcastStream<Vec<u8>>,
    closed: bool,
}
impl StreamSubscription {
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn unsubscribe(mut self) -> Result<()> {
        self.closed = true;
        self.registration.deactivate();
        let mut e = PayloadEncoder::new();
        e.put_string(&self.pattern);
        decode_stream_response(
            "UNSUBSCRIBE",
            &self
                .connection
                .request(message_type::STREAM_UNSUBSCRIBE, e.finish())
                .await?,
        )?;
        Ok(())
    }
}
impl Stream for StreamSubscription {
    type Item = Result<StreamCommitNotification>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.closed {
            return Poll::Ready(None);
        }
        loop {
            match Pin::new(&mut self.receiver).poll_next(cx) {
                Poll::Ready(Some(Ok(payload))) => {
                    let mut d = PayloadDecoder::new(&payload);
                    let id = match d.get_u64() {
                        Ok(v) => v,
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    };
                    if id != self.registration.wire_id() {
                        continue;
                    }
                    return Poll::Ready(Some(decode_stream_notify(&payload)));
                }
                Poll::Ready(Some(Err(_))) => {
                    return Poll::Ready(Some(Err(FitzError::Backpressure(
                        "Stream subscription buffer is full".into(),
                    ))));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
fn decode_subscription_id(response: &[u8]) -> Result<u64> {
    decode_stream_response("SUBSCRIBE", response)?
        .session_id
        .ok_or_else(|| FitzError::Protocol("SUBSCRIBE response missing subscription id".into()))
}
fn optional_bytes(e: &mut PayloadEncoder, value: Option<&[u8]>) {
    if let Some(v) = value {
        e.put_u8(1).put_bytes(v);
    } else {
        e.put_u8(0);
    }
}
fn optional_u64(e: &mut PayloadEncoder, value: Option<u64>) {
    if let Some(v) = value {
        e.put_u8(1).put_u64(v);
    } else {
        e.put_u8(0);
    }
}
