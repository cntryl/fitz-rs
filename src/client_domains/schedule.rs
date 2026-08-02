use crate::async_connection::{AsyncConnection, RestorableRegistration};
use crate::codec::{PayloadDecoder, PayloadEncoder};
use crate::domains::routes::{
    route_matches_pattern, validate_fixed_route, validate_registration_pattern,
};
use crate::protocol::message_type;
use crate::{FitzError, Result};
use futures_core::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_stream::wrappers::BroadcastStream;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleDeliveryMode {
    Broadcast = 0,
    Single = 1,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleEntry {
    pub route: String,
    pub cron: String,
    pub delivery_mode: ScheduleDeliveryMode,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleListResult {
    pub entries: Vec<ScheduleEntry>,
    pub total_count: u64,
}

#[derive(Clone)]
pub struct ScheduleClient {
    connection: AsyncConnection,
}
impl ScheduleClient {
    pub(crate) fn new(connection: AsyncConnection) -> Self {
        Self { connection }
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn create(
        &self,
        route: &str,
        cron: &str,
        mode: ScheduleDeliveryMode,
        payload: &[u8],
    ) -> Result<String> {
        validate_fixed_route(route, "schedule", 4)?;
        let mut e = PayloadEncoder::new();
        e.put_string(route)
            .put_string(cron)
            .put_u8(mode as u8)
            .put_bytes(payload);
        let response = self
            .connection
            .request(message_type::SCHEDULE_CREATE, e.finish())
            .await?;
        let d = success(&response, "CREATE")?;
        if !d.is_empty() {
            return Err(FitzError::Protocol(
                "schedule CREATE response has trailing bytes".into(),
            ));
        }
        Ok(route.into())
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn cancel(&self, route: &str) -> Result<()> {
        validate_fixed_route(route, "schedule", 4)?;
        let mut e = PayloadEncoder::new();
        e.put_string(route);
        success(
            &self
                .connection
                .request(message_type::SCHEDULE_CANCEL, e.finish())
                .await?,
            "CANCEL",
        )?;
        Ok(())
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn list(
        &self,
        offset: Option<u64>,
        limit: Option<u64>,
    ) -> Result<ScheduleListResult> {
        let mut e = PayloadEncoder::new();
        optional_u64(&mut e, offset);
        optional_u64(&mut e, limit);
        let response = self
            .connection
            .request(message_type::SCHEDULE_LIST, e.finish())
            .await?;
        let mut d = success(&response, "LIST")?;
        if d.is_empty() {
            return Ok(ScheduleListResult {
                entries: Vec::new(),
                total_count: 0,
            });
        }
        let total_count = d.get_u64()?;
        let mut entries = Vec::new();
        while !d.is_empty() {
            if d.get_u8()? == 0 {
                break;
            }
            let route = d.get_string()?;
            let cron = d.get_string()?;
            let delivery_mode = match d.get_u8()? {
                0 => ScheduleDeliveryMode::Broadcast,
                1 => ScheduleDeliveryMode::Single,
                v => {
                    return Err(FitzError::Protocol(format!(
                        "invalid schedule delivery mode {v}"
                    )));
                }
            };
            entries.push(ScheduleEntry {
                route,
                cron,
                delivery_mode,
                payload: d.get_bytes()?,
            });
        }
        Ok(ScheduleListResult {
            entries,
            total_count,
        })
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn list_by_selector(
        &self,
        selector: &str,
        offset: u64,
        limit: u64,
    ) -> Result<ScheduleListResult> {
        validate_registration_pattern(selector, "schedule", 4)?;
        let mut source = 0;
        let mut matches = Vec::new();
        let mut total = 0;
        loop {
            let page = self.list(Some(source), Some(100)).await?;
            if page.entries.is_empty() {
                break;
            }
            for entry in &page.entries {
                if route_matches_pattern(&entry.route, selector) {
                    let below_limit =
                        usize::try_from(limit).map_or(true, |limit| matches.len() < limit);
                    if total >= offset && (limit == 0 || below_limit) {
                        matches.push(entry.clone());
                    }
                    total += 1;
                }
            }
            source += page.entries.len() as u64;
            if source >= page.total_count {
                break;
            }
        }
        Ok(ScheduleListResult {
            entries: matches,
            total_count: total,
        })
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn subscribe(&self, pattern: &str) -> Result<ScheduleSubscription> {
        validate_registration_pattern(pattern, "schedule", 4)?;
        let receiver = self
            .connection
            .notifications(message_type::SCHEDULE_NOTIFY, 64);
        let mut e = PayloadEncoder::new();
        e.put_string(pattern);
        let payload = e.finish();
        let response = self
            .connection
            .request(message_type::SCHEDULE_SUBSCRIBE, payload.clone())
            .await?;
        let subscription_id = decode_subscription_id(&response)?;
        let registration = self.connection.register_restorable(
            message_type::SCHEDULE_SUBSCRIBE,
            payload,
            subscription_id,
            decode_subscription_id,
        );
        Ok(ScheduleSubscription {
            connection: self.connection.clone(),
            pattern: pattern.into(),
            registration,
            receiver: BroadcastStream::new(receiver),
            closed: false,
        })
    }
}
fn optional_u64(e: &mut PayloadEncoder, value: Option<u64>) {
    if let Some(v) = value {
        e.put_u8(1).put_u64(v);
    } else {
        e.put_u8(0);
    }
}
fn success<'a>(response: &'a [u8], operation: &str) -> Result<PayloadDecoder<'a>> {
    let mut d = PayloadDecoder::new(response);
    match d.get_u8()? {
        0 => Ok(d),
        1 => Err(FitzError::Domain {
            code: d.get_u32()?,
            message: d.get_string()?,
        }),
        v => Err(FitzError::Protocol(format!(
            "Schedule {operation} returned status {v}"
        ))),
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleNotification {
    pub route: String,
    pub payload: Vec<u8>,
}
pub struct ScheduleSubscription {
    connection: AsyncConnection,
    pattern: String,
    registration: RestorableRegistration,
    receiver: BroadcastStream<Vec<u8>>,
    closed: bool,
}
impl ScheduleSubscription {
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn unsubscribe(mut self) -> Result<()> {
        self.closed = true;
        self.registration.deactivate();
        let mut e = PayloadEncoder::new();
        e.put_string(&self.pattern);
        success(
            &self
                .connection
                .request(message_type::SCHEDULE_UNSUBSCRIBE, e.finish())
                .await?,
            "UNSUBSCRIBE",
        )?;
        Ok(())
    }
}
impl Stream for ScheduleSubscription {
    type Item = Result<ScheduleNotification>;
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
                    return Poll::Ready(Some((|| {
                        Ok(ScheduleNotification {
                            route: d.get_string()?,
                            payload: d.get_bytes()?,
                        })
                    })()));
                }
                Poll::Ready(Some(Err(_))) => {
                    return Poll::Ready(Some(Err(FitzError::Backpressure(
                        "Schedule subscription buffer is full".into(),
                    ))));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
fn decode_subscription_id(response: &[u8]) -> Result<u64> {
    let mut decoder = success(response, "SUBSCRIBE")?;
    if decoder.remaining() == 9 {
        let _ = decoder.get_u8()?;
    }
    decoder.get_u64()
}
