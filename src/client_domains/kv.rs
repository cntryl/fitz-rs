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
        let mut receiver = self.connection.notifications(message_type::KV_NOTIFY, 64);
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
        receiver = receiver.resubscribe();
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
    pub start_key: Option<Vec<u8>>,
    pub end_key: Option<Vec<u8>>,
    pub limit: Option<u32>,
    pub reverse: bool,
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
        if start_key >= end_key {
            return Err(FitzError::Protocol(
                "range start must be less than range end".into(),
            ));
        }
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
        if let (Some(start), Some(end)) = (&options.start_key, &options.end_key)
            && start >= end
        {
            return Err(FitzError::Protocol(
                "range start must be less than range end".into(),
            ));
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
        let response = self
            .connection
            .request_replayable_in_generation(
                message_type::KV_SCAN,
                encoder.finish(),
                Some(self.generation),
            )
            .await?;
        let mut decoder = PayloadDecoder::new(&response);
        if decoder.get_u8()? != 0 {
            return Err(FitzError::DomainError("KV SCAN failed".into()));
        }
        let count = decoder.get_u32()?;
        let mut pairs = Vec::with_capacity(count as usize);
        for _ in 0..count {
            pairs.push(KvPair {
                key: decoder.get_bytes()?,
                value: decoder.get_bytes()?,
            });
        }
        let has_more = !decoder.is_empty() && decoder.get_u8()? == 1;
        Ok(KvScanPage { pairs, has_more })
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
