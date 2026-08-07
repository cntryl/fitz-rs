use crate::async_connection::{AsyncConnection, RestorableRegistration};
use crate::codec::{PayloadDecoder, PayloadEncoder};
use crate::domains::routes::{
    route_matches_pattern, validate_fixed_route, validate_registration_pattern,
};
use crate::protocol::message_type;
use crate::{FitzError, Result};
use futures_core::Stream;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
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
    pub total_count: u64,
}

#[derive(Clone)]
pub struct ScheduleClient {
    connection: AsyncConnection,
    subscriptions: Arc<tokio::sync::Mutex<HashMap<String, SharedScheduleSubscription>>>,
}
struct SharedScheduleSubscription {
    registration: Arc<RestorableRegistration>,
    references: usize,
}
impl ScheduleClient {
    pub(crate) fn new(connection: AsyncConnection) -> Self {
        Self {
            connection,
            subscriptions: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
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
        let d = plain_success(&response, "CREATE")?;
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
        let payload = self
            .connection
            .request(message_type::SCHEDULE_CANCEL, e.finish())
            .await?;
        let response = plain_success(&payload, "CANCEL")?;
        if !response.is_empty() {
            return Err(FitzError::Protocol(
                "schedule CANCEL response has trailing bytes".into(),
            ));
        }
        Ok(())
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn list(&self, offset: Option<u64>, limit: Option<u64>) -> Result<ScheduleListPage> {
        let mut e = PayloadEncoder::new();
        match offset {
            Some(value) => e.put_u8(1).put_u64(value),
            None => e.put_u8(0),
        };
        match limit {
            Some(value) => e.put_u8(1).put_u64(value),
            None => e.put_u8(0),
        };
        let response = self
            .connection
            .request_replayable(message_type::SCHEDULE_LIST_PAGE, e.finish())
            .await?;
        decode_list(success(&response, "LIST")?)
    }

    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn list_by_selector(&self, selector: &str) -> Result<Vec<ScheduleEntry>> {
        validate_registration_pattern(selector, "schedule", 4)?;
        let mut matches = Vec::new();
        let mut offset = 0_u64;
        loop {
            let page = self.list(Some(offset), Some(100)).await?;
            for entry in &page.entries {
                if route_matches_pattern(&entry.route, selector) {
                    matches.push(entry.clone());
                }
            }
            offset += u64::try_from(page.entries.len())
                .map_err(|_| FitzError::FrameTooLarge(page.entries.len()))?;
            if offset >= page.total_count {
                break;
            }
            if page.entries.is_empty() {
                return Err(FitzError::Protocol(
                    "schedule LIST returned an empty page before total_count".into(),
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
        let mut subscriptions = self.subscriptions.lock().await;
        let registration = if let Some(shared) = subscriptions.get_mut(pattern) {
            shared.references += 1;
            Arc::clone(&shared.registration)
        } else {
            let mut e = PayloadEncoder::new();
            e.put_string(pattern);
            let payload = e.finish();
            let response = self
                .connection
                .request(message_type::SCHEDULE_SUBSCRIBE, payload.clone())
                .await?;
            let subscription_id = decode_subscription_id(&response)?;
            let registration = Arc::new(self.connection.register_restorable(
                message_type::SCHEDULE_SUBSCRIBE,
                payload,
                subscription_id,
                decode_subscription_id,
            ));
            subscriptions.insert(
                pattern.into(),
                SharedScheduleSubscription {
                    registration: Arc::clone(&registration),
                    references: 1,
                },
            );
            registration
        };
        drop(subscriptions);
        Ok(ScheduleSubscription {
            connection: self.connection.clone(),
            subscriptions: Arc::clone(&self.subscriptions),
            pattern: pattern.into(),
            registration,
            receiver: BroadcastStream::new(receiver),
            closed: false,
        })
    }
}

fn decode_list(mut d: PayloadDecoder<'_>) -> Result<ScheduleListPage> {
    let total_count = d.get_u64()?;
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
            "schedule LIST response has trailing bytes".into(),
        ));
    }
    Ok(ScheduleListPage {
        entries,
        total_count,
    })
}

fn success<'a>(response: &'a [u8], operation: &str) -> Result<PayloadDecoder<'a>> {
    let mut d = PayloadDecoder::new(response);
    match d.get_u8()? {
        0 => Ok(d),
        1 => {
            let code = d.get_u32()?;
            let message = d.get_string()?;
            if !d.is_empty() {
                return Err(FitzError::Protocol(format!(
                    "Schedule {operation} error response has trailing bytes"
                )));
            }
            Err(FitzError::Domain { code, message })
        }
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
    subscriptions: Arc<tokio::sync::Mutex<HashMap<String, SharedScheduleSubscription>>>,
    pattern: String,
    registration: Arc<RestorableRegistration>,
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
        let mut subscriptions = self.subscriptions.lock().await;
        let Some(shared) = subscriptions.get_mut(&self.pattern) else {
            return Ok(());
        };
        if shared.references > 1 {
            shared.references -= 1;
            return Ok(());
        }
        let mut e = PayloadEncoder::new();
        e.put_string(&self.pattern);
        let payload = self
            .connection
            .request(message_type::SCHEDULE_UNSUBSCRIBE, e.finish())
            .await?;
        let response = plain_success(&payload, "UNSUBSCRIBE")?;
        if !response.is_empty() {
            return Err(FitzError::Protocol(
                "schedule UNSUBSCRIBE response has trailing bytes".into(),
            ));
        }
        self.registration.deactivate();
        subscriptions.remove(&self.pattern);
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
    let mut decoder = plain_success(response, "SUBSCRIBE")?;
    if decoder.get_u8()? != 1 {
        return Err(FitzError::Protocol(
            "schedule SUBSCRIBE response is missing subscription id".into(),
        ));
    }
    let subscription_id = decoder.get_u64()?;
    if !decoder.is_empty() {
        return Err(FitzError::Protocol(
            "schedule SUBSCRIBE response has trailing bytes".into(),
        ));
    }
    Ok(subscription_id)
}

fn plain_success<'a>(response: &'a [u8], operation: &str) -> Result<PayloadDecoder<'a>> {
    let mut d = PayloadDecoder::new(response);
    match d.get_u8()? {
        0 => Ok(d),
        1 => {
            let message = d.get_string()?;
            if !d.is_empty() {
                return Err(FitzError::Protocol(format!(
                    "Schedule {operation} error response has trailing bytes"
                )));
            }
            Err(FitzError::Domain { code: 0, message })
        }
        v => Err(FitzError::Protocol(format!(
            "Schedule {operation} returned status {v}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::async_connection::AsyncConnectionOptions;
    use crate::{
        ConnectionState, FitzObservability, HeartbeatOptions, ReconnectPolicy, RetryPolicy,
    };
    use std::time::Duration;
    use tokio::sync::watch;

    #[test]
    fn should_decode_schedule_list_total_count_given_valid_wire_payload() {
        // Arrange
        let mut encoder = PayloadEncoder::new();
        encoder
            .put_u64(7)
            .put_u8(1)
            .put_string("schedule://realm/area/job")
            .put_string("*/5 * * * *")
            .put_u8(ScheduleDeliveryMode::Single as u8)
            .put_bytes(b"payload")
            .put_u8(0);
        let payload = encoder.finish();

        // Act
        let page = decode_list(PayloadDecoder::new(&payload)).unwrap();

        // Assert
        assert_eq!(page.total_count, 7);
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].route, "schedule://realm/area/job");
        assert_eq!(page.entries[0].delivery_mode, ScheduleDeliveryMode::Single);
        assert_eq!(page.entries[0].payload, b"payload");
    }

    #[test]
    fn should_reject_schedule_list_page_given_trailing_bytes() {
        // Arrange
        let mut encoder = PayloadEncoder::new();
        encoder.put_u64(0).put_u8(0).put_u8(9);
        let payload = encoder.finish();

        // Act
        let result = decode_list(PayloadDecoder::new(&payload));

        // Assert
        assert!(
            matches!(result, Err(FitzError::Protocol(message)) if message.contains("trailing"))
        );
    }

    #[tokio::test]
    async fn should_retain_schedule_wire_subscription_until_last_local_handle() {
        let (state, _) = watch::channel(ConnectionState::Disconnected);
        let connection = AsyncConnection::spawn(AsyncConnectionOptions {
            endpoint: "tcp://127.0.0.1:1".into(),
            token_provider: Arc::new(|| async { Ok(String::new()) }),
            timeout: Duration::from_millis(20),
            max_queued: 4,
            reconnect: ReconnectPolicy {
                enabled: false,
                ..ReconnectPolicy::default()
            },
            retry: RetryPolicy::default(),
            heartbeat: HeartbeatOptions::default(),
            observability: FitzObservability::default(),
            state,
        });
        let pattern = "schedule://realm/area/job/*".to_string();
        let registration = Arc::new(connection.register_restorable(
            message_type::SCHEDULE_SUBSCRIBE,
            vec![],
            42,
            decode_subscription_id,
        ));
        let subscriptions = Arc::new(tokio::sync::Mutex::new(HashMap::from([(
            pattern.clone(),
            SharedScheduleSubscription {
                registration: Arc::clone(&registration),
                references: 2,
            },
        )])));
        let receiver = connection.notifications(message_type::SCHEDULE_NOTIFY, 1);
        let handle = ScheduleSubscription {
            connection,
            subscriptions: Arc::clone(&subscriptions),
            pattern: pattern.clone(),
            registration,
            receiver: BroadcastStream::new(receiver),
            closed: false,
        };
        handle.unsubscribe().await.unwrap();
        assert_eq!(subscriptions.lock().await[&pattern].references, 1);
    }
}
