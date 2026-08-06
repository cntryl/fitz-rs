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
pub struct ScheduleListPage {
    pub entries: Vec<ScheduleEntry>,
    pub has_more: bool,
    pub continuation: Option<String>,
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
    pub async fn list_page(
        &self,
        cursor: Option<&str>,
        limit: Option<u64>,
    ) -> Result<ScheduleListPage> {
        if limit.is_some_and(|value| !(1..=1000).contains(&value)) {
            return Err(FitzError::Protocol(
                "schedule LIST_PAGE limit must be between 1 and 1000".into(),
            ));
        }
        let mut e = PayloadEncoder::new();
        match cursor {
            Some(value) => e.put_u8(1).put_string(value),
            None => e.put_u8(0),
        };
        match limit {
            Some(value) => e.put_u8(1).put_u64(value),
            None => e.put_u8(0),
        };
        let response = self
            .connection
            .request(message_type::SCHEDULE_LIST_PAGE, e.finish())
            .await?;
        let d = success(&response, "LIST_PAGE")?;
        if d.is_empty() {
            return Ok(ScheduleListPage {
                entries: Vec::new(),
                has_more: false,
                continuation: None,
            });
        }
        decode_list_page(d)
    }

    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn list_by_selector(&self, selector: &str) -> Result<Vec<ScheduleEntry>> {
        validate_registration_pattern(selector, "schedule", 4)?;
        let mut matches = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let page = self.list_page(cursor.as_deref(), Some(100)).await?;
            for entry in &page.entries {
                if route_matches_pattern(&entry.route, selector) {
                    matches.push(entry.clone());
                }
            }
            if !page.has_more {
                break;
            }
            cursor = page.continuation;
            if cursor.is_none() {
                return Err(FitzError::Protocol(
                    "schedule LIST_PAGE response missing continuation".into(),
                ));
            }
        }
        Ok(matches)
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

fn decode_list_page(mut d: PayloadDecoder<'_>) -> Result<ScheduleListPage> {
    if d.get_u8()? != 1 {
        return Err(FitzError::Protocol(
            "schedule LIST_PAGE response has unsupported version".into(),
        ));
    }
    let has_more = match d.get_u8()? {
        0 => false,
        1 => true,
        value => {
            return Err(FitzError::Protocol(format!(
                "invalid has_more flag {value}"
            )));
        }
    };
    let continuation = match d.get_u8()? {
        0 => None,
        1 => Some(d.get_string()?),
        value => {
            return Err(FitzError::Protocol(format!(
                "invalid continuation flag {value}"
            )));
        }
    };
    let mut entries = Vec::new();
    loop {
        match d.get_u8()? {
            0 => break,
            1 => {}
            value => {
                return Err(FitzError::Protocol(format!(
                    "invalid entry sentinel {value}"
                )));
            }
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
    if !d.is_empty() {
        return Err(FitzError::Protocol(
            "schedule LIST_PAGE response has trailing bytes".into(),
        ));
    }
    Ok(ScheduleListPage {
        entries,
        has_more,
        continuation,
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_decode_schedule_list_page_given_valid_wire_payload() {
        // Arrange
        let mut encoder = PayloadEncoder::new();
        encoder
            .put_u8(1)
            .put_u8(1)
            .put_u8(1)
            .put_string("cursor-2")
            .put_u8(1)
            .put_string("schedule://realm/area/job")
            .put_string("*/5 * * * *")
            .put_u8(ScheduleDeliveryMode::Single as u8)
            .put_bytes(b"payload")
            .put_u8(0);
        let payload = encoder.finish();

        // Act
        let page = decode_list_page(PayloadDecoder::new(&payload)).unwrap();

        // Assert
        assert!(page.has_more);
        assert_eq!(page.continuation.as_deref(), Some("cursor-2"));
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].route, "schedule://realm/area/job");
        assert_eq!(page.entries[0].delivery_mode, ScheduleDeliveryMode::Single);
        assert_eq!(page.entries[0].payload, b"payload");
    }

    #[test]
    fn should_reject_schedule_list_page_given_trailing_bytes() {
        // Arrange
        let mut encoder = PayloadEncoder::new();
        encoder.put_u8(1).put_u8(0).put_u8(0).put_u8(0).put_u8(9);
        let payload = encoder.finish();

        // Act
        let result = decode_list_page(PayloadDecoder::new(&payload));

        // Assert
        assert!(
            matches!(result, Err(FitzError::Protocol(message)) if message.contains("trailing"))
        );
    }
}
