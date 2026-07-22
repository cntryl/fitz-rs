//! Schedule domain client.

use crate::codec::{PayloadDecoder, PayloadEncoder};
use crate::connection::SharedConnection;
use crate::error::{FitzError, Result};
use crate::protocol::message_type;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Schedule entry returned by list operations.
pub struct ScheduleEntry {
    pub id: String,
    pub route: String,
    pub cron: String,
    pub payload: Vec<u8>,
}

/// Schedule domain client for create/cancel/list and subscriptions.
pub struct ScheduleClient {
    conn: SharedConnection,
}

impl ScheduleClient {
    pub fn new(conn: SharedConnection) -> Self {
        Self { conn }
    }

    pub fn create(&self, route: &str, cron: &str, payload: &[u8]) -> Result<String> {
        validate_schedule_route(route)?;

        let mut enc = PayloadEncoder::new();
        enc.put_string(route);
        enc.put_string(cron);
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
            let payload = dec.get_bytes()?;
            entries.push(ScheduleEntry {
                id: route.clone(),
                route,
                cron,
                payload,
            });
        }

        Ok((entries, total_count))
    }

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
    pub fn subscription_id(&self) -> u64 {
        self.subscription_id
    }

    pub fn next(&self) -> Result<ScheduleNotification> {
        let (_, payload) = self.conn.recv_message_matching(|msg_type, payload| {
            msg_type == message_type::SCHEDULE_NOTIFY
                && decode_schedule_notify_subscription_id(payload)
                    .map(|sub_id| sub_id == self.subscription_id)
                    .unwrap_or(false)
        })?;

        decode_schedule_notify(&payload)
    }

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
    if route.len() > u16::MAX as usize {
        Err(FitzError::Protocol(
            "route exceeds the 65,535-byte TLV value limit".into(),
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_decode_schedule_list_response() {
        let mut buf = vec![0];
        buf.extend_from_slice(&2u64.to_be_bytes());
        buf.push(1);
        buf.extend_from_slice(&(18u32).to_be_bytes());
        buf.extend_from_slice(b"schedule://a/b/c/d");
        buf.extend_from_slice(&(5u32).to_be_bytes());
        buf.extend_from_slice(b"* * *");
        buf.extend_from_slice(&(4u32).to_be_bytes());
        buf.extend_from_slice(b"fire");
        buf.push(0);

        let mut dec = decode_schedule_success("LIST", &buf).unwrap();
        assert_eq!(dec.get_u64().unwrap(), 2);
        assert_eq!(dec.get_u8().unwrap(), 1);
        assert_eq!(dec.get_string().unwrap(), "schedule://a/b/c/d");
        assert_eq!(dec.get_string().unwrap(), "* * *");
        assert_eq!(dec.get_bytes().unwrap(), b"fire");
        assert_eq!(dec.get_u8().unwrap(), 0);
    }

    #[test]
    fn should_treat_schedule_routes_as_opaque() {
        for route in ["schedule://realm/area/resource/op", "queue://x", "*", ""] {
            validate_schedule_route(route).unwrap();
        }
    }

    #[test]
    fn should_decode_schedule_subscription_response() {
        let mut buf = vec![0, 1];
        buf.extend_from_slice(&42u64.to_be_bytes());
        let sub_id = decode_schedule_subscription("SUBSCRIBE", &buf).unwrap();
        assert_eq!(sub_id, 42);
    }

    #[test]
    fn should_decode_schedule_notify_payload() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&42u64.to_be_bytes());
        buf.extend_from_slice(&(4u32).to_be_bytes());
        buf.extend_from_slice(b"fire");

        let notification = decode_schedule_notify(&buf).unwrap();
        assert_eq!(notification.payload, b"fire");
    }
}
