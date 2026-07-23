//! Lease (distributed lock) domain client
//!
//! Provides ergonomic access to Fitz lease operations. Routes are opaque
//! strings — the client never parses or constructs them.
//!
//! ## Wire format (matches server's `lease_codec.rs`)
//!
//! ### Requests
//!
//! ACQUIRE (400): `[string route][string owner_id][u64 ttl_secs]`
//! RENEW   (401): `[string route][string owner_id][u64 fencing_token][u64 ttl_secs]`
//! RELEASE (402): `[string route][string owner_id][u64 fencing_token]`
//! QUERY   (403): `[string route]`
//!
//! ### Responses (all operations)
//!
//! Success: `[u8 0][u8 has_token][u64 token if has_token=1]`
//! Error:   `[u8 1][u32 error_len][error_msg UTF-8]`
//!
//! The `[u8 has_token][optional u64]` encoding matches
//! `PayloadEncoder::put_optional_u64` on the server.

use crate::codec::{PayloadDecoder, PayloadEncoder};
use crate::connection::SharedConnection;
use crate::domains::routes::validate_fixed_route;
use crate::error::{FitzError, Result};
use crate::protocol::message_type;
use futures_util::FutureExt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Result of a lease acquire or renew operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseGrant {
    /// Server-generated opaque fencing token. Must be passed to renew/release.
    pub fencing_token: u64,
}

/// Contract-correct result of a lease query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseInfo {
    pub held: bool,
    pub owner_id: Option<String>,
    pub ttl_remaining_secs: Option<u64>,
    pub pending_waiters: u32,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LeaseExecutionOptions {
    pub wait_for_availability: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum LeaseExecutionError<E> {
    #[error("lease acquisition failed: {0}")]
    Acquisition(FitzError),
    #[error("lease callback failed")]
    Callback(E),
    #[error("lease ownership lost: {0}")]
    OwnershipLost(FitzError),
    #[error("lease release failed: {0}")]
    Release(FitzError),
    #[error("lease lifecycle and callback both failed")]
    Combined { lifecycle: FitzError, callback: E },
}

/// Lease domain client.
///
/// Stateless handle — all route context is passed per-call so a single
/// `LeaseClient` can operate across any realm/area/resource.
#[derive(Clone)]
pub struct LeaseClient {
    conn: SharedConnection,
}

impl LeaseClient {
    #[must_use]
    pub fn new(conn: SharedConnection) -> Self {
        Self { conn }
    }

    /// Acquire a lease.
    ///
    /// `route` is a fully-qualified opaque route string, e.g.
    /// `"lease://prod/locks/leader-election"`.
    ///
    /// Returns the fencing token on success.
    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn acquire(&self, route: &str, owner_id: &str, ttl_secs: u64) -> Result<LeaseGrant> {
        validate_fixed_route(route, "lease", 3)?;

        let mut enc = PayloadEncoder::new();
        enc.put_string(route);
        enc.put_string(owner_id);
        enc.put_u64(ttl_secs);

        let resp = self
            .conn
            .send_request(message_type::LEASE_ACQUIRE, &enc.finish())?;

        let mut dec = decode_lease_success("ACQUIRE", &resp)?;
        let response_type = dec.get_u8()?;
        if response_type >= 2 {
            return Err(FitzError::Domain {
                code: 5001,
                message: if response_type == 2 {
                    "lease acquisition queued"
                } else {
                    "lease acquisition already queued"
                }
                .into(),
            });
        }
        if response_type > 1 {
            return Err(FitzError::Protocol(format!(
                "unknown ACQUIRE response type: {response_type}"
            )));
        }
        let token = dec.get_u64()?;

        Ok(LeaseGrant {
            fencing_token: token,
        })
    }

    /// Extend an existing lease.
    ///
    /// `fencing_token` must match the token returned by a previous acquire/extend.
    /// Returns the (possibly new) fencing token.
    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn extend(
        &self,
        route: &str,
        owner_id: &str,
        fencing_token: u64,
        ttl_secs: u64,
    ) -> Result<LeaseGrant> {
        validate_fixed_route(route, "lease", 3)?;

        let mut enc = PayloadEncoder::new();
        enc.put_string(route);
        enc.put_string(owner_id);
        enc.put_u64(fencing_token);
        enc.put_u64(ttl_secs);

        let resp = self
            .conn
            .send_request(message_type::LEASE_RENEW, &enc.finish())?;

        let mut dec = decode_lease_success("EXTEND", &resp)?;
        let token = dec.get_u64()?;

        Ok(LeaseGrant {
            fencing_token: token,
        })
    }

    /// Release a lease.
    ///
    /// `fencing_token` must match the current token.
    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn release(&self, route: &str, owner_id: &str, fencing_token: u64) -> Result<()> {
        validate_fixed_route(route, "lease", 3)?;

        let mut enc = PayloadEncoder::new();
        enc.put_string(route);
        enc.put_string(owner_id);
        enc.put_u64(fencing_token);

        let resp = self
            .conn
            .send_request(message_type::LEASE_RELEASE, &enc.finish())?;

        let dec = decode_lease_success("RELEASE", &resp)?;
        if !dec.is_empty() {
            return Err(FitzError::Protocol(
                "RELEASE response has trailing bytes".into(),
            ));
        }
        Ok(())
    }

    /// Query lease status.
    ///
    /// Returns whether the lease is held (with fencing token) or free.
    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn query(&self, route: &str) -> Result<LeaseInfo> {
        validate_fixed_route(route, "lease", 3)?;

        let mut enc = PayloadEncoder::new();
        enc.put_string(route);

        let resp = self
            .conn
            .send_request(message_type::LEASE_QUERY, &enc.finish())?;

        let mut dec = decode_lease_success("QUERY", &resp)?;
        let held = dec.get_u8()? == 1;
        if !held {
            return Ok(LeaseInfo {
                held,
                owner_id: None,
                ttl_remaining_secs: None,
                pending_waiters: dec.get_u32()?,
            });
        }
        Ok(LeaseInfo {
            held,
            owner_id: Some(dec.get_string()?),
            ttl_remaining_secs: Some(dec.get_u64()?),
            pending_waiters: dec.get_u32()?,
        })
    }

    /// Runs a callback while this client owns the lease.
    ///
    /// # Errors
    /// Returns the typed acquisition, callback, ownership-loss, or cleanup failure.
    pub async fn with_lease<F, Fut, T, E>(
        &self,
        route: &str,
        owner_id: &str,
        ttl_secs: u64,
        callback: F,
    ) -> std::result::Result<T, LeaseExecutionError<E>>
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.with_lease_with_options(
            route,
            owner_id,
            ttl_secs,
            callback,
            LeaseExecutionOptions::default(),
        )
        .await
    }

    /// Runs a callback while owning the lease with explicit execution options.
    ///
    /// # Errors
    /// Returns the typed acquisition, callback, ownership-loss, or cleanup failure.
    pub async fn with_lease_with_options<F, Fut, T, E>(
        &self,
        route: &str,
        owner_id: &str,
        ttl_secs: u64,
        callback: F,
        options: LeaseExecutionOptions,
    ) -> std::result::Result<T, LeaseExecutionError<E>>
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        validate_fixed_route(route, "lease", 3).map_err(LeaseExecutionError::Acquisition)?;
        if ttl_secs == 0 || ttl_secs > u64::from(u32::MAX) / 1_000 {
            return Err(LeaseExecutionError::Acquisition(FitzError::Protocol(
                "lease TTL must be positive and schedulable".into(),
            )));
        }
        let route = route.to_owned();
        let owner_id = owner_id.to_owned();
        let mut delay = Duration::from_millis(50);
        let mut grant = loop {
            let client = self.clone();
            let acquire_route = route.clone();
            let acquire_owner = owner_id.clone();
            let result = tokio::task::spawn_blocking(move || {
                client.acquire(&acquire_route, &acquire_owner, ttl_secs)
            })
            .await
            .map_err(|error| {
                LeaseExecutionError::Acquisition(FitzError::Connection(error.to_string()))
            })?;
            match result {
                Ok(grant) => break grant,
                Err(error) if options.wait_for_availability && is_contention(&error) => {
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(1));
                }
                Err(error) => return Err(LeaseExecutionError::Acquisition(error)),
            }
        };

        let cancellation = CancellationToken::new();
        let callback_task =
            tokio::spawn(AssertUnwindSafe(callback(cancellation.clone())).catch_unwind());
        tokio::pin!(callback_task);
        let renewal = tokio::time::sleep(Duration::from_secs(ttl_secs) / 3);
        tokio::pin!(renewal);
        loop {
            tokio::select! {
                biased;
                callback_result = &mut callback_task => {
                    let callback_outcome = callback_result.map_err(|error| {
                        LeaseExecutionError::Acquisition(FitzError::Connection(error.to_string()))
                    })?;
                    let client = self.clone();
                    let release_route = route.clone();
                    let release_owner = owner_id.clone();
                    let token = grant.fencing_token;
                    let release_result = tokio::task::spawn_blocking(move || {
                        client.release(&release_route, &release_owner, token)
                    }).await.map_err(|error| FitzError::Connection(error.to_string())).and_then(|result| result);
                    let callback_result = match callback_outcome {
                        Ok(result) => result,
                        Err(panic) => std::panic::resume_unwind(panic),
                    };
                    return match (callback_result, release_result) {
                        (Ok(value), Ok(())) => Ok(value),
                        (Err(error), Ok(())) => Err(LeaseExecutionError::Callback(error)),
                        (Ok(_), Err(error)) => Err(LeaseExecutionError::Release(error)),
                        (Err(callback), Err(lifecycle)) => Err(LeaseExecutionError::Combined { lifecycle, callback }),
                    };
                }
                () = &mut renewal => {
                    let client = self.clone();
                    let renew_route = route.clone();
                    let renew_owner = owner_id.clone();
                    let token = grant.fencing_token;
                    let renewed = tokio::task::spawn_blocking(move || {
                        client.extend(&renew_route, &renew_owner, token, ttl_secs)
                    }).await.map_err(|error| FitzError::Connection(error.to_string())).and_then(|result| result);
                    match renewed {
                        Ok(next) => {
                            grant = next;
                            renewal.as_mut().reset(tokio::time::Instant::now() + Duration::from_secs(ttl_secs) / 3);
                        }
                        Err(error) => {
                            cancellation.cancel();
                            let callback_outcome = callback_task.await.map_err(|join| {
                                LeaseExecutionError::OwnershipLost(FitzError::Connection(join.to_string()))
                            })?;
                            let callback_result = match callback_outcome {
                                Ok(result) => result,
                                Err(panic) => std::panic::resume_unwind(panic),
                            };
                            return match callback_result {
                                Ok(_) => Err(LeaseExecutionError::OwnershipLost(error)),
                                Err(callback) => Err(LeaseExecutionError::Combined { lifecycle: error, callback }),
                            };
                        }
                    }
                }
            }
        }
    }
}

/// Decode the standard lease response format.
///
/// Server wire format (from `lease_codec::encode_response` +
/// `PayloadEncoder::put_optional_u64`):
///
///   Success: `[u8 0][u8 has_token][u64 token if has_token=1]`
///   Error:   `[u8 1][u32 error_len][error_msg UTF-8]`
///
/// Returns `Ok(Some(token))` for success with token, `Ok(None)` for
/// success without token (e.g. RELEASE), or `Err` for server errors.
fn decode_lease_success<'a>(operation: &str, buf: &'a [u8]) -> Result<PayloadDecoder<'a>> {
    let mut dec = PayloadDecoder::new(buf);
    let status = dec.get_u8()?;
    match status {
        0 => Ok(dec),
        1 => {
            let code = dec.get_u32()?;
            let msg = dec.get_string()?;
            Err(FitzError::Domain {
                code,
                message: format!("{operation} failed: {msg}"),
            })
        }
        _ => Err(FitzError::Protocol(format!(
            "Unknown response status byte: {status}"
        ))),
    }
}

fn is_contention(error: &FitzError) -> bool {
    matches!(error, FitzError::Domain { code: 5001, .. })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_decode_success_with_token() {
        // Arrange
        // [status=0][response_type=Acquired][u64 token=42]
        let mut buf = vec![0x00, 0x00];
        buf.extend_from_slice(&42u64.to_be_bytes());
        // Act
        let mut result = decode_lease_success("ACQUIRE", &buf).unwrap();
        // Assert
        assert_eq!(result.get_u8().unwrap(), 0);
        assert_eq!(result.get_u64().unwrap(), 42);
    }

    #[test]
    fn should_decode_success_without_token() {
        // Arrange
        let buf = vec![0x00];
        // Act
        let result = decode_lease_success("RELEASE", &buf).unwrap();
        // Assert
        assert!(result.is_empty());
    }

    #[test]
    fn should_decode_error_response() {
        // Arrange
        // [status=1][u32 code][u32 len=9]["Not found"]
        let msg = b"Not found";
        let mut buf = vec![0x01];
        buf.extend_from_slice(&5004u32.to_be_bytes());
        buf.extend_from_slice(&u32::try_from(msg.len()).unwrap().to_be_bytes());
        buf.extend_from_slice(msg);
        // Act
        let Err(err) = decode_lease_success("QUERY", &buf) else {
            panic!("expected domain error");
        };
        // Assert
        assert!(err.to_string().contains("Not found"));
    }

    #[test]
    fn should_encode_acquire_request() {
        // Arrange
        let mut enc = PayloadEncoder::new();
        enc.put_string("lease://prod/locks/leader");
        enc.put_string("node-1");
        enc.put_u64(30);
        let payload = enc.finish();

        // Verify round-trip
        // Act
        let mut dec = PayloadDecoder::new(&payload);
        // Assert
        assert_eq!(dec.get_string().unwrap(), "lease://prod/locks/leader");
        assert_eq!(dec.get_string().unwrap(), "node-1");
        assert_eq!(dec.get_u64().unwrap(), 30);
        assert!(dec.is_empty());
    }

    #[test]
    fn should_encode_renew_request() {
        // Arrange
        let mut enc = PayloadEncoder::new();
        enc.put_string("lease://prod/locks/leader");
        enc.put_string("node-1");
        enc.put_u64(12345); // fencing token
        enc.put_u64(60); // ttl
        let payload = enc.finish();

        // Act
        let mut dec = PayloadDecoder::new(&payload);
        // Assert
        assert_eq!(dec.get_string().unwrap(), "lease://prod/locks/leader");
        assert_eq!(dec.get_string().unwrap(), "node-1");
        assert_eq!(dec.get_u64().unwrap(), 12345);
        assert_eq!(dec.get_u64().unwrap(), 60);
        assert!(dec.is_empty());
    }

    #[test]
    fn should_encode_release_request() {
        // Arrange
        let mut enc = PayloadEncoder::new();
        enc.put_string("lease://prod/locks/leader");
        enc.put_string("node-1");
        enc.put_u64(12345);
        let payload = enc.finish();

        // Act
        let mut dec = PayloadDecoder::new(&payload);
        // Assert
        assert_eq!(dec.get_string().unwrap(), "lease://prod/locks/leader");
        assert_eq!(dec.get_string().unwrap(), "node-1");
        assert_eq!(dec.get_u64().unwrap(), 12345);
        assert!(dec.is_empty());
    }

    #[test]
    fn should_encode_query_request() {
        // Arrange
        let mut enc = PayloadEncoder::new();
        enc.put_string("lease://prod/locks/leader");
        let payload = enc.finish();

        // Act
        let mut dec = PayloadDecoder::new(&payload);
        // Assert
        assert_eq!(dec.get_string().unwrap(), "lease://prod/locks/leader");
        assert!(dec.is_empty());
    }

    #[test]
    fn should_reject_empty_response() {
        // Arrange
        let response = [];
        // Act
        let Err(err) = decode_lease_success("QUERY", &response) else {
            panic!("expected codec error");
        };
        // Assert
        assert!(matches!(err, FitzError::Codec(_)));
    }

    #[test]
    fn should_reject_unknown_status_byte() {
        // Arrange
        let response = [0x02];
        // Act
        let Err(err) = decode_lease_success("QUERY", &response) else {
            panic!("expected protocol error");
        };
        // Assert
        assert!(err.to_string().contains("Unknown"));
    }
}
