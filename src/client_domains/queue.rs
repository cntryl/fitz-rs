use crate::async_connection::{AsyncConnection, RestorableRegistration};
use crate::codec::{PayloadDecoder, PayloadEncoder};
use crate::domains::routes::{
    validate_fixed_route, validate_registration_pattern, validate_response_fixed_route,
};
use crate::protocol::message_type;
use crate::{FitzError, Result};
use futures_core::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_stream::wrappers::BroadcastStream;

#[derive(Clone)]
pub struct QueueClient {
    connection: AsyncConnection,
}

impl QueueClient {
    pub(crate) fn new(connection: AsyncConnection) -> Self {
        Self { connection }
    }

    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn enqueue(
        &self,
        route: &str,
        body: &[u8],
        delay_seconds: Option<u64>,
    ) -> Result<u64> {
        validate_fixed_route(route, "queue", 3)?;
        let payload = Self::encode_enqueue(route, body, delay_seconds);
        let response = self
            .connection
            .request_confirmed_negative(
                message_type::QUEUE_ENQUEUE,
                payload,
                is_retryable_enqueue_rejection,
            )
            .await?;
        let mut decoder = success_decoder(&response, "ENQUEUE")?;
        if decoder.is_empty() {
            Ok(0)
        } else {
            decoder.get_u64()
        }
    }

    fn encode_enqueue(route: &str, body: &[u8], delay_seconds: Option<u64>) -> Vec<u8> {
        let mut encoder = PayloadEncoder::new();
        encoder.put_string(route).put_bytes(body);
        if let Some(delay) = delay_seconds.filter(|delay| *delay > 0) {
            encoder.put_u8(1).put_u64(delay);
        } else {
            encoder.put_u8(0);
        }
        encoder.finish()
    }

    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn reserve(
        &self,
        route: &str,
        lease_seconds: u64,
        batch_size: u32,
        wait_seconds: Option<u64>,
    ) -> Result<Vec<QueueItem>> {
        validate_registration_pattern(route, "queue", 3)?;
        let mut encoder = PayloadEncoder::new();
        encoder
            .put_string(route)
            .put_u64(lease_seconds)
            .put_u8(1)
            .put_u32(batch_size.max(1));
        if let Some(wait) = wait_seconds.filter(|wait| *wait > 0) {
            encoder.put_u8(1).put_u64(wait);
        } else {
            encoder.put_u8(0);
        }
        let response = self
            .connection
            .request(message_type::QUEUE_RESERVE, encoder.finish())
            .await?;
        let mut decoder = success_decoder(&response, "RESERVE")?;
        if decoder.is_empty() {
            return Ok(Vec::new());
        }
        let count = decoder.get_u32()?;
        let generation = self.connection.generation();
        let mut items = Vec::with_capacity(count as usize);
        let wildcard = route.split('/').any(|segment| segment.contains('*'));
        for _ in 0..count {
            let concrete_route = if wildcard {
                let concrete = decoder.get_string()?;
                validate_response_fixed_route(&concrete, "queue", 3, "RESERVE")?;
                concrete
            } else {
                route.to_owned()
            };
            items.push(QueueItem {
                connection: self.connection.clone(),
                generation,
                route: concrete_route,
                id: decoder.get_u64()?,
                token: decoder.get_u64()?,
                body: decoder.get_bytes()?,
            });
        }
        Ok(items)
    }

    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn subscribe(&self, pattern: &str) -> Result<QueueSubscription> {
        validate_registration_pattern(pattern, "queue", 3)?;
        let receiver = self
            .connection
            .notifications(message_type::QUEUE_NOTIFY, 64);
        let mut encoder = PayloadEncoder::new();
        encoder.put_string(pattern);
        let payload = encoder.finish();
        let response = self
            .connection
            .request(message_type::QUEUE_SUBSCRIBE, payload.clone())
            .await?;
        let subscription_id = decode_subscription_id(&response)?;
        let registration = self.connection.register_restorable(
            message_type::QUEUE_SUBSCRIBE,
            payload,
            subscription_id,
            decode_subscription_id,
        );
        Ok(QueueSubscription {
            connection: self.connection.clone(),
            pattern: pattern.to_owned(),
            registration,
            receiver: BroadcastStream::new(receiver),
            closed: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_enqueue_delay_in_seconds_without_truncation() {
        // Arrange
        let route = "queue://realm/area/jobs";

        // Act
        let payload = QueueClient::encode_enqueue(route, b"body", Some(2));

        // Assert
        let mut decoder = PayloadDecoder::new(&payload);
        assert_eq!(decoder.get_string().unwrap(), route);
        assert_eq!(decoder.get_bytes().unwrap(), b"body");
        assert_eq!(decoder.get_u8().unwrap(), 1);
        assert_eq!(decoder.get_u64().unwrap(), 2);
        assert!(decoder.is_empty());
    }

    #[test]
    fn should_decode_length_prefixed_queue_notify_payload() {
        // Arrange
        let payload = b"opaque queue state";
        let mut wire = PayloadEncoder::new();
        wire.put_string("queue://realm/area/jobs")
            .put_bytes(payload);
        let bytes = wire.finish();
        let mut decoder = PayloadDecoder::new(&bytes);

        // Act
        let notification = decode_queue_notification(&mut decoder).unwrap();

        // Assert
        assert_eq!(notification.route, "queue://realm/area/jobs");
        assert_eq!(notification.payload, payload);
    }
}

fn is_retryable_enqueue_rejection(response: &[u8]) -> bool {
    let mut decoder = PayloadDecoder::new(response);
    decoder.get_u8().is_ok_and(|status| status == 1)
        && decoder.get_u32().is_ok_and(|code| code == 4005)
}

pub struct QueueItem {
    connection: AsyncConnection,
    generation: u64,
    pub route: String,
    id: u64,
    token: u64,
    pub body: Vec<u8>,
}

impl QueueItem {
    fn ensure_current(&self) -> Result<()> {
        if self.connection.generation() == self.generation {
            Ok(())
        } else {
            Err(FitzError::StaleHandle)
        }
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn extend(&self, lease_seconds: u64) -> Result<()> {
        self.ensure_current()?;
        let mut encoder = PayloadEncoder::new();
        encoder
            .put_string(&self.route)
            .put_u64(self.id)
            .put_u64(self.token)
            .put_u64(lease_seconds);
        decode_queue_plain_ok(
            &self
                .connection
                .request(message_type::QUEUE_EXTEND, encoder.finish())
                .await?,
        )
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn complete(self) -> Result<()> {
        self.ensure_current()?;
        let mut encoder = PayloadEncoder::new();
        encoder
            .put_string(&self.route)
            .put_u64(self.id)
            .put_u64(self.token);
        decode_queue_plain_ok(
            &self
                .connection
                .request(message_type::QUEUE_COMPLETE, encoder.finish())
                .await?,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueNotification {
    pub route: String,
    pub payload: Vec<u8>,
}

pub struct QueueSubscription {
    connection: AsyncConnection,
    pattern: String,
    registration: RestorableRegistration,
    receiver: BroadcastStream<Vec<u8>>,
    closed: bool,
}

impl QueueSubscription {
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn unsubscribe(mut self) -> Result<()> {
        self.closed = true;
        self.registration.deactivate();
        let mut encoder = PayloadEncoder::new();
        encoder.put_string(&self.pattern);
        decode_queue_plain_ok(
            &self
                .connection
                .request(message_type::QUEUE_UNSUBSCRIBE, encoder.finish())
                .await?,
        )
    }
}

impl Stream for QueueSubscription {
    type Item = Result<QueueNotification>;
    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.closed {
            return Poll::Ready(None);
        }
        loop {
            match Pin::new(&mut self.receiver).poll_next(context) {
                Poll::Ready(Some(Ok(payload))) => {
                    let mut decoder = PayloadDecoder::new(&payload);
                    let id = match decoder.get_u64() {
                        Ok(v) => v,
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    };
                    if id != self.registration.wire_id() {
                        continue;
                    }
                    let result = decode_queue_notification(&mut decoder);
                    return Poll::Ready(Some(result));
                }
                Poll::Ready(Some(Err(_))) => {
                    return Poll::Ready(Some(Err(FitzError::Backpressure(
                        "Queue subscription buffer is full".into(),
                    ))));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

fn decode_queue_notification(decoder: &mut PayloadDecoder<'_>) -> Result<QueueNotification> {
    let route = decoder.get_string()?;
    let payload = decoder.get_bytes()?;
    if !decoder.is_empty() {
        return Err(FitzError::Protocol(
            "Queue NOTIFY response has trailing bytes".into(),
        ));
    }
    Ok(QueueNotification { route, payload })
}

fn decode_subscription_id(response: &[u8]) -> Result<u64> {
    let mut decoder = success_decoder(response, "SUBSCRIBE")?;
    if decoder.get_u8()? != 1 {
        return Err(FitzError::Protocol(
            "Queue SUBSCRIBE response missing subscription id".into(),
        ));
    }
    decoder.get_u64()
}

fn success_decoder<'a>(response: &'a [u8], operation: &str) -> Result<PayloadDecoder<'a>> {
    let mut decoder = PayloadDecoder::new(response);
    match decoder.get_u8()? {
        0 => Ok(decoder),
        1 if matches!(operation, "ENQUEUE" | "RESERVE") => Err(FitzError::Domain {
            code: decoder.get_u32()?,
            message: decoder.get_string()?,
        }),
        1 => Err(FitzError::Domain {
            code: 0,
            message: decoder.get_string()?,
        }),
        status => Err(FitzError::Protocol(format!(
            "Queue {operation} returned status {status}"
        ))),
    }
}

fn decode_queue_plain_ok(response: &[u8]) -> Result<()> {
    let mut decoder = PayloadDecoder::new(response);
    match decoder.get_u8()? {
        0 if decoder.is_empty() => Ok(()),
        0 => Err(FitzError::Protocol(
            "Queue response has trailing bytes".into(),
        )),
        1 => Err(FitzError::Domain {
            code: 0,
            message: decoder.get_string()?,
        }),
        status => Err(FitzError::Protocol(format!(
            "Queue operation returned status {status}"
        ))),
    }
}
