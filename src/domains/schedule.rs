//! Schedule domain client.

use crate::codec::{PayloadDecoder, PayloadEncoder};
use crate::connection::SharedConnection;
use crate::domains::routes::validate_fixed_route;
use crate::error::{FitzError, Result};
use crate::protocol::message_type;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Schedule entry returned by list operations.
pub struct ScheduleEntry {
    pub id: String,
    pub route: String,
    pub cron: String,
    pub delivery_mode: ScheduleDeliveryMode,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ScheduleDeliveryMode {
    Broadcast = 0,
    Single = 1,
}

impl TryFrom<u8> for ScheduleDeliveryMode {
    type Error = FitzError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Broadcast),
            1 => Ok(Self::Single),
            _ => Err(FitzError::Protocol(format!(
                "invalid schedule delivery mode: {value}"
            ))),
        }
    }
}

/// Schedule domain client for create/cancel/list and subscriptions.
pub struct ScheduleClient {
    conn: SharedConnection,
}

impl ScheduleClient {
    #[must_use]
    pub fn new(conn: SharedConnection) -> Self {
        Self { conn }
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn create(
        &self,
        route: &str,
        cron: &str,
        delivery_mode: ScheduleDeliveryMode,
        payload: &[u8],
    ) -> Result<String> {
        validate_schedule_route(route)?;

        let mut enc = PayloadEncoder::new();
        enc.put_string(route);
        enc.put_string(cron);
        enc.put_u8(delivery_mode as u8);
        enc.put_bytes(payload);

        let resp = self
            .conn
            .send_request(message_type::SCHEDULE_CREATE, &enc.finish())?;

        let mut dec = decode_schedule_success("CREATE", &resp)?;
        if dec.is_empty() {
            return Ok(route.to_string());
        }

        let has_schedule_id = dec.get_u8()?;
        if has_schedule_id == 1 {
            dec.get_string()
        } else {
            Ok(route.to_string())
        }
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn cancel(&self, route: &str) -> Result<()> {
        validate_schedule_route(route)?;

        let mut enc = PayloadEncoder::new();
        enc.put_string(route);

        let resp = self
            .conn
            .send_request(message_type::SCHEDULE_CANCEL, &enc.finish())?;

        decode_schedule_success("CANCEL", &resp)?;
        Ok(())
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn list(
        &self,
        offset: Option<u64>,
        limit: Option<u64>,
    ) -> Result<(Vec<ScheduleEntry>, u64)> {
        let mut enc = PayloadEncoder::new();
        match offset {
            Some(value) => {
                enc.put_u8(1);
                enc.put_u64(value);
            }
            None => {
                enc.put_u8(0);
            }
        }
        match limit {
            Some(value) => {
                enc.put_u8(1);
                enc.put_u64(value);
            }
            None => {
                enc.put_u8(0);
            }
        }

        let resp = self
            .conn
            .send_request(message_type::SCHEDULE_LIST, &enc.finish())?;

        let mut dec = decode_schedule_success("LIST", &resp)?;
        if dec.is_empty() {
            return Ok((Vec::new(), 0));
        }

        let total_count = dec.get_u64()?;
        let mut entries = Vec::new();
        while !dec.is_empty() {
            let has_entry = dec.get_u8()?;
            if has_entry == 0 {
                break;
            }

            let route = dec.get_string()?;
            let cron = dec.get_string()?;
            let delivery_mode = ScheduleDeliveryMode::try_from(dec.get_u8()?)?;
            let payload = dec.get_bytes()?;
            entries.push(ScheduleEntry {
                id: route.clone(),
                route,
                cron,
                delivery_mode,
                payload,
            });
        }

        Ok((entries, total_count))
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn subscribe(&self, pattern: &str) -> Result<ScheduleSubscription> {
        validate_schedule_route(pattern)?;

        let mut enc = PayloadEncoder::new();
        enc.put_string(pattern);

        let resp = self
            .conn
            .send_request(message_type::SCHEDULE_SUBSCRIBE, &enc.finish())?;

        let subscription_id = decode_schedule_subscription("SUBSCRIBE", &resp)?;
        Ok(ScheduleSubscription {
            conn: self.conn.clone(),
            pattern: pattern.to_string(),
            subscription_id,
        })
    }
}

/// Active schedule subscription handle.
pub struct ScheduleSubscription {
    conn: SharedConnection,
    pattern: String,
    subscription_id: u64,
}

impl ScheduleSubscription {
    #[must_use]
    pub fn subscription_id(&self) -> u64 {
        self.subscription_id
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn next(&self) -> Result<ScheduleNotification> {
        let (_, payload) = self.conn.recv_message_matching(|msg_type, payload| {
            msg_type == message_type::SCHEDULE_NOTIFY
                && decode_schedule_notify_subscription_id(payload)
                    .is_ok_and(|sub_id| sub_id == self.subscription_id)
        })?;

        decode_schedule_notify(&payload)
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn unsubscribe(&self) -> Result<()> {
        let mut enc = PayloadEncoder::new();
        enc.put_string(&self.pattern);

        let resp = self
            .conn
            .send_request(message_type::SCHEDULE_UNSUBSCRIBE, &enc.finish())?;

        decode_schedule_success("UNSUBSCRIBE", &resp)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Notification payload emitted when a schedule fires.
pub struct ScheduleNotification {
    pub payload: Vec<u8>,
}

fn decode_schedule_success<'a>(operation: &str, buf: &'a [u8]) -> Result<PayloadDecoder<'a>> {
    let mut dec = PayloadDecoder::new(buf);
    let status = dec.get_u8()?;
    match status {
        0 => Ok(dec),
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

fn decode_schedule_subscription(operation: &str, buf: &[u8]) -> Result<u64> {
    let mut dec = decode_schedule_success(operation, buf)?;
    let has_subscription_id = if dec.is_empty() { 0 } else { dec.get_u8()? };
    if has_subscription_id != 1 {
        return Err(FitzError::Protocol(format!(
            "{operation} response missing subscription id"
        )));
    }
    dec.get_u64()
}

fn decode_schedule_notify_subscription_id(payload: &[u8]) -> Result<u64> {
    let mut dec = PayloadDecoder::new(payload);
    dec.get_u64()
}

fn decode_schedule_notify(payload: &[u8]) -> Result<ScheduleNotification> {
    let mut dec = PayloadDecoder::new(payload);
    let _subscription_id = dec.get_u64()?;
    let payload = dec.get_bytes()?;
    Ok(ScheduleNotification { payload })
}

fn validate_schedule_route(route: &str) -> Result<()> {
    validate_fixed_route(route, "schedule", 4)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_decode_schedule_list_response() {
        // Arrange
        let mut buf = vec![0];
        buf.extend_from_slice(&2u64.to_be_bytes());
        buf.push(1);
        buf.extend_from_slice(&(18u32).to_be_bytes());
        buf.extend_from_slice(b"schedule://a/b/c/d");
        buf.extend_from_slice(&(5u32).to_be_bytes());
        buf.extend_from_slice(b"* * *");
        buf.push(ScheduleDeliveryMode::Single as u8);
        buf.extend_from_slice(&(4u32).to_be_bytes());
        buf.extend_from_slice(b"fire");
        buf.push(0);

        // Act
        let mut dec = decode_schedule_success("LIST", &buf).unwrap();
        // Assert
        assert_eq!(dec.get_u64().unwrap(), 2);
        assert_eq!(dec.get_u8().unwrap(), 1);
        assert_eq!(dec.get_string().unwrap(), "schedule://a/b/c/d");
        assert_eq!(dec.get_string().unwrap(), "* * *");
        assert_eq!(dec.get_u8().unwrap(), 1);
        assert_eq!(dec.get_bytes().unwrap(), b"fire");
        assert_eq!(dec.get_u8().unwrap(), 0);
    }

    #[test]
    fn should_accept_concrete_schedule_route_given_four_segments_when_validation_runs() {
        // Arrange
        let route = "schedule://realm/area/resource/op";

        // Act
        let result = validate_schedule_route(route);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_reject_schedule_selector_given_wildcard_when_schedule_validation_runs() {
        // Arrange
        let routes = [
            "schedule://realm/area/resource/*",
            "schedule://realm/area/*/op",
        ];

        // Act
        let results = routes.map(validate_schedule_route);

        // Assert
        assert!(results.iter().all(Result::is_err));
    }

    #[test]
    fn should_decode_schedule_subscription_response() {
        // Arrange
        let mut buf = vec![0, 1];
        buf.extend_from_slice(&42u64.to_be_bytes());
        // Act
        let sub_id = decode_schedule_subscription("SUBSCRIBE", &buf).unwrap();
        // Assert
        assert_eq!(sub_id, 42);
    }

    #[test]
    fn should_decode_schedule_notify_payload() {
        // Arrange
        let mut buf = Vec::new();
        buf.extend_from_slice(&42u64.to_be_bytes());
        buf.extend_from_slice(&(4u32).to_be_bytes());
        buf.extend_from_slice(b"fire");

        // Act
        let notification = decode_schedule_notify(&buf).unwrap();
        // Assert
        assert_eq!(notification.payload, b"fire");
    }

    #[test]
    fn should_preserve_typed_domain_code_given_error_response_when_mapping_errors() {
        // Arrange
        let mut buf = vec![1];
        buf.extend_from_slice(&7008u32.to_be_bytes());
        buf.extend_from_slice(&21u32.to_be_bytes());
        buf.extend_from_slice(b"invalid delivery mode");

        // Act
        let Err(error) = decode_schedule_success("CREATE", &buf) else {
            panic!("expected domain error");
        };

        // Assert
        assert!(matches!(error, FitzError::Domain { code: 7008, .. }));
    }

    #[test]
    fn should_classify_unknown_code_given_unregistered_code_when_mapping_errors() {
        // Arrange
        let mut buf = vec![1];
        buf.extend_from_slice(&u32::MAX.to_be_bytes());
        buf.extend_from_slice(&7u32.to_be_bytes());
        buf.extend_from_slice(b"unknown");

        // Act
        let Err(error) = decode_schedule_success("CREATE", &buf) else {
            panic!("expected unknown domain error");
        };

        // Assert
        assert!(matches!(error, FitzError::Domain { code: u32::MAX, .. }));
    }

    #[test]
    fn should_reject_malformed_domain_envelope_given_missing_required_fields_when_response_parsed()
    {
        // Arrange
        let response = [1, 0, 0, 0, 1];

        // Act
        let Err(error) = decode_schedule_success("CREATE", &response) else {
            panic!("expected malformed envelope error");
        };

        // Assert
        assert!(matches!(error, FitzError::Codec(_)));
    }
}
