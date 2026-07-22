//! RPC domain client.

use crate::codec::{PayloadDecoder, PayloadEncoder};
use crate::connection::SharedConnection;
use crate::domains::routes::validate_concrete_route;
use crate::error::{FitzError, Result};
use crate::protocol::message_type;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
/// One response frame from an RPC call stream.
pub struct RpcResponseFrame {
    pub body: Vec<u8>,
    pub sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Inbound worker request data for a registered RPC route.
pub struct RpcInboundRequest {
    correlation_id: [u8; 16],
    pub route: String,
    pub reply_route: String,
    pub body: Vec<u8>,
}

/// RPC domain client for issuing calls and registering workers.
pub struct RpcClient {
    conn: SharedConnection,
}

impl RpcClient {
    #[must_use]
    pub fn new(conn: SharedConnection) -> Self {
        Self { conn }
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn call(&self, route: &str, body: &[u8]) -> Result<RpcResponseStream> {
        validate_concrete_route(route, "rpc")?;

        let correlation_id = *Uuid::new_v4().as_bytes();

        let mut enc = PayloadEncoder::new();
        enc.put_raw(&correlation_id);
        enc.put_string(route);
        enc.put_bytes(body);

        self.conn
            .send_only(message_type::RPC_REQUEST, &enc.finish())?;

        Ok(RpcResponseStream {
            conn: self.conn.clone(),
            correlation_id,
            finished: false,
        })
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn call_all(&self, route: &str, body: &[u8]) -> Result<Vec<RpcResponseFrame>> {
        self.call(route, body)?.collect_all()
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn register_worker(&self, pattern: &str) -> Result<RpcWorkerRegistration> {
        validate_concrete_route(pattern, "rpc")?;

        let mut enc = PayloadEncoder::new();
        enc.put_string(pattern);
        enc.put_u32(1);

        let resp = self
            .conn
            .send_request(message_type::RPC_SUBSCRIBE, &enc.finish())?;
        decode_rpc_status_response("REGISTER_WORKER", &resp)?;

        Ok(RpcWorkerRegistration {
            conn: self.conn.clone(),
            pattern: pattern.to_string(),
        })
    }
}

/// Iterator-like stream of RPC response frames for one correlation id.
pub struct RpcResponseStream {
    conn: SharedConnection,
    correlation_id: [u8; 16],
    finished: bool,
}

impl RpcResponseStream {
    // This blocking, fallible stream API intentionally differs from Iterator::next:
    // protocol errors are returned outside the optional end-of-stream value.
    #[allow(clippy::should_implement_trait)]
    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn next(&mut self) -> Result<Option<RpcResponseFrame>> {
        if self.finished {
            return Ok(None);
        }

        let (_, payload) = self.conn.recv_message_matching(|msg_type, payload| {
            msg_type == message_type::RPC_RESPONSE
                && decode_rpc_response_correlation_id(payload)
                    .is_ok_and(|correlation_id| correlation_id == self.correlation_id)
        })?;

        let response = decode_rpc_response(&payload)?;
        if response.correlation_id != self.correlation_id {
            return Err(FitzError::Protocol(
                "received RPC response for unexpected correlation id".to_string(),
            ));
        }

        if response.stream_end {
            self.finished = true;
        }

        if response.stream_end && response.body.is_empty() {
            return Ok(None);
        }

        Ok(Some(RpcResponseFrame {
            body: response.body,
            sequence: response.sequence,
        }))
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn collect_all(mut self) -> Result<Vec<RpcResponseFrame>> {
        let mut frames = Vec::new();
        while let Some(frame) = self.next()? {
            frames.push(frame);
        }
        Ok(frames)
    }
}

/// Handle for a registered RPC worker pattern.
pub struct RpcWorkerRegistration {
    conn: SharedConnection,
    pattern: String,
}

impl RpcWorkerRegistration {
    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn next(&self) -> Result<RpcWorkerRequest> {
        let (_, payload) = self.conn.recv_message_matching(|msg_type, payload| {
            msg_type == message_type::RPC_REQUEST
                && decode_rpc_request_route(payload)
                    .is_ok_and(|route| route_matches_pattern(&route, &self.pattern))
        })?;

        let request = decode_rpc_request(&payload)?;
        Ok(RpcWorkerRequest {
            conn: self.conn.clone(),
            correlation_id: request.correlation_id,
            route: request.route,
            reply_route: request.reply_route,
            body: request.body,
            next_sequence: 0,
            finished: false,
        })
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn unregister(&self) -> Result<()> {
        let mut enc = PayloadEncoder::new();
        enc.put_string(&self.pattern);

        let resp = self
            .conn
            .send_request(message_type::RPC_UNSUBSCRIBE, &enc.finish())?;
        decode_rpc_status_response("UNREGISTER_WORKER", &resp)
    }
}

/// Mutable worker request context used to send streamed responses.
pub struct RpcWorkerRequest {
    conn: SharedConnection,
    correlation_id: [u8; 16],
    pub route: String,
    pub reply_route: String,
    pub body: Vec<u8>,
    next_sequence: u64,
    finished: bool,
}

impl RpcWorkerRequest {
    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn respond(&mut self, body: &[u8], is_end: bool) -> Result<()> {
        if self.finished {
            return Err(FitzError::Protocol(
                "cannot send RPC response after stream has ended".to_string(),
            ));
        }

        let mut enc = PayloadEncoder::new();
        enc.put_raw(&self.correlation_id);
        enc.put_u64(self.next_sequence);
        enc.put_u8(u8::from(is_end));
        enc.put_bytes(body);

        self.conn
            .send_only(message_type::RPC_RESPONSE, &enc.finish())?;

        self.next_sequence += 1;
        if is_end {
            self.finished = true;
        }

        Ok(())
    }

    ///
    /// # Errors
    /// Returns an error when validation, encoding, transport, or broker processing fails.
    pub fn finish(self) -> Result<()> {
        if self.finished {
            return Ok(());
        }

        let mut request = self;
        request.respond(&[], true)
    }
}

struct DecodedRpcResponse {
    correlation_id: [u8; 16],
    sequence: u64,
    body: Vec<u8>,
    stream_end: bool,
}

fn decode_rpc_status_response(operation: &str, buf: &[u8]) -> Result<()> {
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

fn decode_rpc_request_route(payload: &[u8]) -> Result<String> {
    let mut dec = PayloadDecoder::new(payload);
    let _correlation_id = dec.get_fixed::<16>()?;
    dec.get_string()
}

fn decode_rpc_request(payload: &[u8]) -> Result<RpcInboundRequest> {
    let mut dec = PayloadDecoder::new(payload);
    let correlation_id = dec.get_fixed::<16>()?;
    let route = dec.get_string()?;
    let body = dec.get_bytes()?;

    Ok(RpcInboundRequest {
        correlation_id,
        route,
        reply_route: String::new(),
        body,
    })
}

fn decode_rpc_response_correlation_id(payload: &[u8]) -> Result<[u8; 16]> {
    PayloadDecoder::new(payload).get_fixed::<16>()
}

fn decode_rpc_response(payload: &[u8]) -> Result<DecodedRpcResponse> {
    let mut dec = PayloadDecoder::new(payload);
    let correlation_id = dec.get_fixed::<16>()?;
    let sequence = dec.get_u64()?;
    let stream_end = dec.get_u8()? != 0;
    let body = dec.get_bytes()?;

    Ok(DecodedRpcResponse {
        correlation_id,
        sequence,
        body,
        stream_end,
    })
}

fn route_matches_pattern(route: &str, pattern: &str) -> bool {
    let route_segments: Vec<&str> = route.split('/').collect();
    let pattern_segments: Vec<&str> = pattern.split('/').collect();

    let mut route_index = 0;
    let mut pattern_index = 0;

    while pattern_index < pattern_segments.len() && route_index < route_segments.len() {
        let segment = pattern_segments[pattern_index];
        if segment == "**" {
            return true;
        }

        if segment != "*" && segment != route_segments[route_index] {
            return false;
        }

        pattern_index += 1;
        route_index += 1;
    }

    if pattern_index == pattern_segments.len() && route_index == route_segments.len() {
        return true;
    }

    pattern_index + 1 == pattern_segments.len() && pattern_segments[pattern_index] == "**"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_decode_rpc_response_payload() {
        // Arrange
        let correlation_id = [7u8; 16];
        let mut buf = Vec::new();
        buf.extend_from_slice(&correlation_id);
        buf.extend_from_slice(&3u64.to_be_bytes());
        buf.push(1);
        buf.extend_from_slice(&(4u32).to_be_bytes());
        buf.extend_from_slice(b"pong");

        // Act
        let response = decode_rpc_response(&buf).unwrap();
        // Assert
        assert_eq!(response.correlation_id, correlation_id);
        assert_eq!(response.sequence, 3);
        assert_eq!(response.body, b"pong");
        assert!(response.stream_end);
    }

    #[test]
    fn should_decode_rpc_request_payload() {
        // Arrange
        let correlation_id = [9u8; 16];
        let mut buf = Vec::new();
        buf.extend_from_slice(&correlation_id);
        buf.extend_from_slice(&(19u32).to_be_bytes());
        buf.extend_from_slice(b"rpc://realm/area/op");
        buf.extend_from_slice(&(4u32).to_be_bytes());
        buf.extend_from_slice(b"ping");

        // Act
        let request = decode_rpc_request(&buf).unwrap();
        // Assert
        assert_eq!(request.correlation_id, correlation_id);
        assert_eq!(request.route, "rpc://realm/area/op");
        assert_eq!(request.reply_route, "");
        assert_eq!(request.body, b"ping");
    }

    #[test]
    fn should_match_rpc_route_patterns() {
        // Arrange
        // Act
        // Assert
        assert!(route_matches_pattern(
            "rpc://realm/app/echo",
            "rpc://realm/app/echo"
        ));
        assert!(route_matches_pattern(
            "rpc://realm/app/echo",
            "rpc://realm/*/echo"
        ));
        assert!(route_matches_pattern(
            "rpc://realm/app/echo",
            "rpc://realm/**"
        ));
        assert!(!route_matches_pattern(
            "rpc://realm/app/echo",
            "rpc://other/**"
        ));
    }

    #[test]
    fn should_decode_rpc_status_error_response() {
        // Arrange
        let mut buf = vec![1];
        buf.extend_from_slice(&(4u32).to_be_bytes());
        buf.extend_from_slice(b"nope");

        // Act
        let err = decode_rpc_status_response("CALL", &buf).unwrap_err();
        // Assert
        assert!(err.to_string().contains("nope"));
    }
}
