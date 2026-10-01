use super::decode_ok;
use crate::async_connection::{AsyncConnection, RestorableRegistration};
use crate::codec::{PayloadDecoder, PayloadEncoder};
use crate::domains::routes::{validate_fixed_route, validate_registration_pattern};
use crate::protocol::{TransactionMode, message_type};
use crate::{FitzError, KvDurability, Result};
use futures_core::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_stream::wrappers::BroadcastStream;

#[derive(Clone)]
pub struct KvClient {
    connection: AsyncConnection,
}

impl KvClient {
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
        mode: TransactionMode,
        durability: KvDurability,
    ) -> Result<KvTransaction> {
        validate_fixed_route(route, "kv", 3)?;
        let mut encoder = PayloadEncoder::new();
        encoder
            .put_string(route)
            .put_u8(mode as u8)
            .put_u8(durability as u8);
        let response = self
            .connection
            .request(message_type::KV_BEGIN, encoder.finish())
            .await?;
        let mut decoder = PayloadDecoder::new(&response);
        match decoder.get_u8()? {
            0 => Ok(KvTransaction {
                connection: self.connection.clone(),
                generation: self.connection.generation(),
                transaction_id: decoder.get_u64()?,
                route: route.to_owned(),
                closed: false,
            }),
            1 => Err(FitzError::Domain {
                code: decoder.get_u32()?,
                message: decoder.get_string()?,
            }),
            status => Err(FitzError::Protocol(format!(
                "KV BEGIN returned status {status}"
            ))),
        }
    }

    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn subscribe(&self, pattern: &str) -> Result<KvSubscription> {
        validate_registration_pattern(pattern, "kv", 3)?;
        let mut encoder = PayloadEncoder::new();
        encoder.put_string(pattern);
        let receiver = self.connection.notifications(message_type::KV_NOTIFY, 64);
        let payload = encoder.finish();
        let response = self
            .connection
            .request(message_type::KV_SUBSCRIBE, payload.clone())
            .await?;
        let subscription_id = decode_subscription_id(&response)?;
        let registration = self.connection.register_restorable(
            message_type::KV_SUBSCRIBE,
            payload,
            subscription_id,
            decode_subscription_id,
        );
        Ok(KvSubscription {
            connection: self.connection.clone(),
            pattern: pattern.to_owned(),
            registration,
            receiver: BroadcastStream::new(receiver),
            closed: false,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvGetResult {
    Found(Vec<u8>),
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvPair {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Default)]
pub struct KvScanOptions {
    /// Inclusive directional bound: lower for forward scans, upper for reverse scans.
    pub start_key: Option<Vec<u8>>,
    /// Exclusive directional bound: upper for forward scans, lower for reverse scans.
    pub end_key: Option<Vec<u8>>,
    /// Maximum items in this page; `None` or `Some(0)` uses the server default/frame budget.
    pub limit: Option<u32>,
    /// Selects descending key order and reverse directional bounds.
    pub reverse: bool,
    /// Resume strictly after `start_key` in the selected direction.
    /// Requires the broker to advertise `CAP_KV_SCAN_EXCLUSIVE`.
    pub start_exclusive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvScanPage {
    pub pairs: Vec<KvPair>,
    pub has_more: bool,
}

pub struct KvTransaction {
    connection: AsyncConnection,
    generation: u64,
    transaction_id: u64,
    route: String,
    closed: bool,
}

impl KvTransaction {
    fn ensure_open(&self) -> Result<()> {
        if self.closed {
            return Err(FitzError::StaleHandle);
        }
        if self.connection.generation() != self.generation {
            return Err(FitzError::StaleHandle);
        }
        Ok(())
    }

    fn key_payload(&self, key: &[u8]) -> Vec<u8> {
        let mut encoder = PayloadEncoder::new();
        encoder
            .put_u64(self.transaction_id)
            .put_string(&self.route)
            .put_bytes(key);
        encoder.finish()
    }

    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn get(&self, key: &[u8]) -> Result<KvGetResult> {
        self.ensure_open()?;
        let response = self
            .connection
            .request_replayable_in_generation(
                message_type::KV_GET,
                self.key_payload(key),
                Some(self.generation),
            )
            .await?;
        let mut decoder = PayloadDecoder::new(&response);
        match decoder.get_u8()? {
            0 => {
                let found = decoder.get_u8()? == 1;
                if found {
                    Ok(KvGetResult::Found(decoder.get_bytes()?))
                } else {
                    Ok(KvGetResult::NotFound)
                }
            }
            1 => Err(FitzError::Domain {
                code: decoder.get_u32()?,
                message: decoder.get_string()?,
            }),
            status => Err(FitzError::Protocol(format!(
                "KV GET returned status {status}"
            ))),
        }
    }

    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write(message_type::KV_PUT, key, value).await
    }

    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write(message_type::KV_INSERT, key, value).await
    }

    async fn write(&self, message_type: u16, key: &[u8], value: &[u8]) -> Result<()> {
        self.ensure_open()?;
        let mut encoder = PayloadEncoder::new();
        encoder
            .put_u64(self.transaction_id)
            .put_string(&self.route)
            .put_bytes(key)
            .put_bytes(value);
        decode_ok(
            &self
                .connection
                .request(message_type, encoder.finish())
                .await?,
        )
    }

    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        self.ensure_open()?;
        decode_ok(
            &self
                .connection
                .request(message_type::KV_DELETE, self.key_payload(key))
                .await?,
        )
    }

    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn delete_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.ensure_open()?;
        let mut encoder = PayloadEncoder::new();
        encoder
            .put_u64(self.transaction_id)
            .put_string(&self.route)
            .put_bytes(start_key)
            .put_bytes(end_key);
        decode_ok(
            &self
                .connection
                .request(message_type::KV_DELETE_RANGE, encoder.finish())
                .await?,
        )
    }

    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn scan(&self, options: &KvScanOptions) -> Result<KvScanPage> {
        self.ensure_open()?;
        if options.start_exclusive
            && self.connection.capability_bits()
                & crate::protocol::message_type::CAP_KV_SCAN_EXCLUSIVE
                == 0
        {
            return Err(FitzError::DomainError(
                "broker did not advertise exclusive KV SCAN resume support".into(),
            ));
        }
        if scan_range_is_empty(
            options.start_key.as_deref(),
            options.end_key.as_deref(),
            options.reverse,
        ) {
            return Ok(KvScanPage {
                pairs: Vec::new(),
                has_more: false,
            });
        }
        let mut encoder = PayloadEncoder::new();
        encoder.put_u64(self.transaction_id).put_string(&self.route);
        encode_optional_bytes(&mut encoder, options.start_key.as_deref());
        encode_optional_bytes(&mut encoder, options.end_key.as_deref());
        if let Some(limit) = options.limit {
            encoder.put_u8(1).put_u32(limit);
        } else {
            encoder.put_u8(0);
        }
        encoder.put_u8(u8::from(options.reverse));
        if options.start_exclusive {
            encoder.put_u8(1);
        }
        let response = self
            .connection
            .request_replayable_in_generation(
                message_type::KV_SCAN,
                encoder.finish(),
                Some(self.generation),
            )
            .await?;
        decode_scan_response(&response)
    }

    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn commit(mut self) -> Result<()> {
        self.ensure_open()?;
        let result = decode_ok(
            &self
                .connection
                .request(message_type::KV_COMMIT, self.finish_payload())
                .await?,
        );
        self.closed = true;
        result
    }

    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn rollback(mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.ensure_open()?;
        let result = decode_ok(
            &self
                .connection
                .request(message_type::KV_ROLLBACK, self.finish_payload())
                .await?,
        );
        self.closed = true;
        result
    }

    fn finish_payload(&self) -> Vec<u8> {
        let mut encoder = PayloadEncoder::new();
        encoder.put_u64(self.transaction_id).put_string(&self.route);
        encoder.finish()
    }
}

fn scan_range_is_empty(start: Option<&[u8]>, end: Option<&[u8]>, reverse: bool) -> bool {
    let (Some(start), Some(end)) = (start, end) else {
        return false;
    };
    match start.cmp(end) {
        std::cmp::Ordering::Equal => true,
        std::cmp::Ordering::Greater => !reverse,
        std::cmp::Ordering::Less => reverse,
    }
}

fn decode_scan_response(payload: &[u8]) -> Result<KvScanPage> {
    let mut decoder = PayloadDecoder::new(payload);
    if decoder.get_u8()? != 0 {
        return Err(FitzError::DomainError("KV SCAN failed".into()));
    }
    let count = decoder.get_u32()?;
    let available = decoder.remaining();
    let maximum_count = available.saturating_sub(1) / 8;
    let capacity = usize::try_from(count).map_err(|_| {
        FitzError::Protocol(format!(
            "KV SCAN item count {count} does not fit this target"
        ))
    })?;
    if capacity > maximum_count {
        return Err(FitzError::Protocol(format!(
            "KV SCAN item count {count} exceeds the remaining response payload"
        )));
    }
    let mut pairs = Vec::with_capacity(capacity);
    for _ in 0..count {
        pairs.push(KvPair {
            key: decoder.get_bytes()?,
            value: decoder.get_bytes()?,
        });
    }
    let has_more = match decoder.get_u8()? {
        0 => false,
        1 => true,
        flag => {
            return Err(FitzError::Protocol(format!(
                "KV SCAN returned invalid has_more flag {flag}"
            )));
        }
    };
    if !decoder.is_empty() {
        return Err(FitzError::Protocol(
            "KV SCAN response contains trailing bytes".into(),
        ));
    }
    Ok(KvScanPage { pairs, has_more })
}

#[cfg(test)]
mod scan_response_tests {
    use super::{decode_scan_response, scan_range_is_empty};
    use crate::FitzError;
    use crate::codec::PayloadEncoder;

    #[test]
    fn should_reject_scan_response_without_has_more() {
        // Arrange
        let mut encoder = PayloadEncoder::new();
        encoder.put_u8(0).put_u32(0);

        // Act
        let result = decode_scan_response(&encoder.finish());

        // Assert
        assert!(matches!(result, Err(FitzError::Codec(_))));
    }

    #[test]
    fn should_reject_scan_response_with_invalid_has_more() {
        // Arrange
        let mut encoder = PayloadEncoder::new();
        encoder.put_u8(0).put_u32(0).put_u8(2);

        // Act
        let result = decode_scan_response(&encoder.finish());

        // Assert
        assert!(matches!(result, Err(FitzError::Protocol(_))));
    }

    #[test]
    fn should_reject_scan_response_with_trailing_bytes() {
        // Arrange
        let mut encoder = PayloadEncoder::new();
        encoder.put_u8(0).put_u32(0).put_u8(0).put_u8(0xff);

        // Act
        let result = decode_scan_response(&encoder.finish());

        // Assert
        assert!(matches!(result, Err(FitzError::Protocol(_))));
    }

    #[test]
    fn should_reject_impossible_scan_count_before_allocating() {
        // Arrange
        let mut encoder = PayloadEncoder::new();
        encoder.put_u8(0).put_u32(u32::MAX);

        // Act
        let result = decode_scan_response(&encoder.finish());

        // Assert
        assert!(matches!(result, Err(FitzError::Protocol(_))));
    }

    #[test]
    fn should_treat_inverted_scan_bounds_as_empty_for_direction() {
        // Arrange
        let lower = b"a";
        let upper = b"z";

        // Act
        let forward_is_empty = scan_range_is_empty(Some(upper), Some(lower), false);
        let reverse_is_empty = scan_range_is_empty(Some(lower), Some(upper), true);
        let valid_reverse_is_empty = scan_range_is_empty(Some(upper), Some(lower), true);

        // Assert
        assert!(forward_is_empty);
        assert!(reverse_is_empty);
        assert!(!valid_reverse_is_empty);
    }
}

fn encode_optional_bytes(encoder: &mut PayloadEncoder, value: Option<&[u8]>) {
    if let Some(value) = value {
        encoder.put_u8(1).put_bytes(value);
    } else {
        encoder.put_u8(0);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvNotification {
    pub route: String,
    pub mutation_count: u64,
}

pub struct KvSubscription {
    connection: AsyncConnection,
    pattern: String,
    registration: RestorableRegistration,
    receiver: BroadcastStream<Vec<u8>>,
    closed: bool,
}

impl KvSubscription {
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn unsubscribe(mut self) -> Result<()> {
        self.closed = true;
        self.registration.deactivate();
        let mut encoder = PayloadEncoder::new();
        encoder.put_string(&self.pattern);
        decode_ok(
            &self
                .connection
                .request(message_type::KV_UNSUBSCRIBE, encoder.finish())
                .await?,
        )
    }
}

impl Stream for KvSubscription {
    type Item = Result<KvNotification>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.closed {
            return Poll::Ready(None);
        }
        loop {
            match Pin::new(&mut self.receiver).poll_next(context) {
                Poll::Ready(Some(Ok(payload))) => {
                    let mut decoder = PayloadDecoder::new(&payload);
                    let subscription_id = match decoder.get_u64() {
                        Ok(value) => value,
                        Err(error) => return Poll::Ready(Some(Err(error))),
                    };
                    if subscription_id != self.registration.wire_id() {
                        continue;
                    }
                    return Poll::Ready(Some(Ok(KvNotification {
                        route: match decoder.get_string() {
                            Ok(value) => value,
                            Err(error) => return Poll::Ready(Some(Err(error))),
                        },
                        mutation_count: match decoder.get_u64() {
                            Ok(value) => value,
                            Err(error) => return Poll::Ready(Some(Err(error))),
                        },
                    })));
                }
                Poll::Ready(Some(Err(_))) => {
                    return Poll::Ready(Some(Err(FitzError::Backpressure(
                        "KV subscription buffer is full".into(),
                    ))));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

fn decode_subscription_id(response: &[u8]) -> Result<u64> {
    let mut decoder = PayloadDecoder::new(response);
    match decoder.get_u8()? {
        0 => decoder.get_u64(),
        1 => Err(FitzError::Domain {
            code: decoder.get_u32()?,
            message: decoder.get_string()?,
        }),
        status => Err(FitzError::Protocol(format!(
            "KV SUBSCRIBE returned status {status}"
        ))),
    }
}
