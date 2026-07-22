//! Notice (pub/sub) domain client.

use crate::codec::{PayloadDecoder, PayloadEncoder};
use crate::connection::SharedConnection;
use crate::domains::routes::{validate_fixed_route, validate_selector_route};
use crate::error::{FitzError, Result};
use crate::protocol::message_type;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Notice payload delivered by a subscription.
pub struct NoticeMessage {
    pub route: String,
    pub body: Vec<u8>,
}

/// Notice domain client for publish/subscribe operations.
pub struct NoticeClient {
    conn: SharedConnection,
}

impl NoticeClient {
    #[must_use]
    pub fn new(conn: SharedConnection) -> Self {
        Self { conn }
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn publish(&self, route: &str, body: &[u8]) -> Result<()> {
        validate_fixed_route(route, "notice", 3)?;

        let mut enc = PayloadEncoder::new();
        enc.put_string(route);
        enc.put_bytes(body);

        let resp = self
            .conn
            .send_request(message_type::NOTICE_PUBLISH, &enc.finish())?;

        decode_notice_response("PUBLISH", &resp)
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn subscribe(&self, pattern: &str) -> Result<NoticeSubscription> {
        validate_selector_route(pattern, "notice", 3, true)?;

        let mut enc = PayloadEncoder::new();
        enc.put_string(pattern);

        let resp = self
            .conn
            .send_request(message_type::NOTICE_SUBSCRIBE, &enc.finish())?;

        let subscription_id = decode_subscription_response("SUBSCRIBE", &resp)?;
        Ok(NoticeSubscription {
            conn: self.conn.clone(),
            subscription_id,
        })
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn unsubscribe_all(&self) -> Result<()> {
        let resp = self
            .conn
            .send_request(message_type::NOTICE_UNSUBSCRIBE_ALL, &[])?;

        decode_notice_response("UNSUBSCRIBE_ALL", &resp)
    }
}

/// Active notice subscription handle.
pub struct NoticeSubscription {
    conn: SharedConnection,
    subscription_id: u64,
}

impl NoticeSubscription {
    #[must_use]
    pub fn subscription_id(&self) -> u64 {
        self.subscription_id
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn next(&self) -> Result<NoticeMessage> {
        let (_, payload) = self.conn.recv_message_matching(|msg_type, payload| {
            msg_type == message_type::NOTICE_NOTIFY
                && decode_notify_subscription_id(payload)
                    .is_ok_and(|sub_id| sub_id == self.subscription_id)
        })?;

        decode_notice_notify(&payload)
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn unsubscribe(&self) -> Result<()> {
        let mut enc = PayloadEncoder::new();
        enc.put_u64(self.subscription_id);

        let resp = self
            .conn
            .send_request(message_type::NOTICE_UNSUBSCRIBE, &enc.finish())?;

        decode_notice_response("UNSUBSCRIBE", &resp)
    }
}

fn decode_notice_response(operation: &str, buf: &[u8]) -> Result<()> {
    let mut dec = PayloadDecoder::new(buf);
    let status = dec.get_u8()?;
    match status {
        0 => Ok(()),
        1 => {
            let message = dec.get_string()?;
            Err(FitzError::DomainError(format!(
                "{operation} failed: {message}"
            )))
        }
        other => Err(FitzError::Protocol(format!(
            "{operation} failed with unknown status byte: {other}"
        ))),
    }
}

fn decode_subscription_response(operation: &str, buf: &[u8]) -> Result<u64> {
    let mut dec = PayloadDecoder::new(buf);
    let status = dec.get_u8()?;
    match status {
        0 => {
            let has_subscription_id = if dec.is_empty() { 0 } else { dec.get_u8()? };
            if has_subscription_id != 1 {
                return Err(FitzError::Protocol(format!(
                    "{operation} response missing subscription id"
                )));
            }
            dec.get_u64()
        }
        1 => {
            let message = dec.get_string()?;
            Err(FitzError::DomainError(format!(
                "{operation} failed: {message}"
            )))
        }
        other => Err(FitzError::Protocol(format!(
            "{operation} failed with unknown status byte: {other}"
        ))),
    }
}

fn decode_notify_subscription_id(payload: &[u8]) -> Result<u64> {
    let mut dec = PayloadDecoder::new(payload);
    dec.get_u64()
}

fn decode_notice_notify(payload: &[u8]) -> Result<NoticeMessage> {
    let mut dec = PayloadDecoder::new(payload);
    let _subscription_id = dec.get_u64()?;
    let route = dec.get_string()?;
    let body = dec.get_bytes()?;
    Ok(NoticeMessage { route, body })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_decode_notice_ok_response() {
        decode_notice_response("PUBLISH", &[0]).unwrap();
    }

    #[test]
    fn should_decode_notice_error_response() {
        let mut buf = vec![1];
        buf.extend_from_slice(&(4u32).to_be_bytes());
        buf.extend_from_slice(b"nope");
        let err = decode_notice_response("PUBLISH", &buf).unwrap_err();
        assert!(err.to_string().contains("nope"));
    }

    #[test]
    fn should_decode_notice_subscription_response() {
        let mut buf = vec![0, 1];
        buf.extend_from_slice(&11u64.to_be_bytes());
        let sub_id = decode_subscription_response("SUBSCRIBE", &buf).unwrap();
        assert_eq!(sub_id, 11);
    }

    #[test]
    fn should_decode_notice_notify_payload() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&11u64.to_be_bytes());
        buf.extend_from_slice(&(21u32).to_be_bytes());
        buf.extend_from_slice(b"notice://realm/area/x");
        buf.extend_from_slice(&(4u32).to_be_bytes());
        buf.extend_from_slice(b"ping");

        let message = decode_notice_notify(&buf).unwrap();
        assert_eq!(message.route, "notice://realm/area/x");
        assert_eq!(message.body, b"ping");
    }
}
