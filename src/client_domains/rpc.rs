use super::decode_ok;
use crate::async_connection::{AsyncConnection, RestorableRegistration};
use crate::codec::{PayloadDecoder, PayloadEncoder};
use crate::domains::routes::{
    route_matches_pattern, validate_concrete_route, validate_registration_pattern,
};
use crate::protocol::message_type;
use crate::{FitzError, Result};
use futures_core::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_stream::wrappers::BroadcastStream;
use uuid::Uuid;
#[derive(Clone)]
pub struct RpcClient {
    connection: AsyncConnection,
}
impl RpcClient {
    pub(crate) fn new(connection: AsyncConnection) -> Self {
        Self { connection }
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn call(&self, route: &str, body: &[u8]) -> Result<RpcResponseStream> {
        validate_concrete_route(route, "rpc")?;
        let correlation_id = *Uuid::new_v4().as_bytes();
        let receiver = self
            .connection
            .notifications(message_type::RPC_RESPONSE, 64);
        let mut e = PayloadEncoder::new();
        e.put_raw(&correlation_id).put_string(route).put_bytes(body);
        self.connection
            .send(message_type::RPC_REQUEST, e.finish())
            .await?;
        Ok(RpcResponseStream {
            correlation_id,
            receiver: BroadcastStream::new(receiver),
            finished: false,
        })
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn register_worker(&self, pattern: &str, max_concurrency: u32) -> Result<RpcWorker> {
        validate_registration_pattern(pattern, "rpc", 0)?;
        if max_concurrency == 0 {
            return Err(FitzError::Protocol(
                "max_concurrency must be positive".into(),
            ));
        }
        let receiver = self.connection.notifications(message_type::RPC_REQUEST, 64);
        let mut e = PayloadEncoder::new();
        e.put_string(pattern).put_u32(max_concurrency);
        let payload = e.finish();
        decode_ok(
            &self
                .connection
                .request(message_type::RPC_SUBSCRIBE, payload.clone())
                .await?,
        )?;
        let registration = self.connection.register_restorable(
            message_type::RPC_SUBSCRIBE,
            payload,
            0,
            decode_worker_registration,
        );
        Ok(RpcWorker {
            connection: self.connection.clone(),
            pattern: pattern.into(),
            receiver: BroadcastStream::new(receiver),
            registration,
            closed: false,
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcResponseFrame {
    pub body: Vec<u8>,
    pub sequence: u64,
}
pub struct RpcResponseStream {
    correlation_id: [u8; 16],
    receiver: BroadcastStream<Vec<u8>>,
    finished: bool,
}
impl Stream for RpcResponseStream {
    type Item = Result<RpcResponseFrame>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        loop {
            match Pin::new(&mut self.receiver).poll_next(cx) {
                Poll::Ready(Some(Ok(payload))) => {
                    let mut d = PayloadDecoder::new(&payload);
                    let id = match d.get_fixed::<16>() {
                        Ok(v) => v,
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    };
                    if id != self.correlation_id {
                        continue;
                    }
                    let (sequence, end, body) = match decode_rpc_response_frame(&mut d) {
                        Ok(v) => v,
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    };
                    if end {
                        self.finished = true;
                    }
                    return Poll::Ready(Some(Ok(RpcResponseFrame { body, sequence })));
                }
                Poll::Ready(Some(Err(_))) => {
                    self.finished = true;
                    return Poll::Ready(Some(Err(FitzError::Backpressure(
                        "RPC response stream is full".into(),
                    ))));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

fn decode_rpc_response_frame(decoder: &mut PayloadDecoder<'_>) -> Result<(u64, bool, Vec<u8>)> {
    let sequence = decoder.get_u64()?;
    let end = decoder.get_u8()? != 0;
    let body = decoder.get_bytes()?;
    if !decoder.is_empty() {
        return Err(FitzError::Protocol(
            "RPC response frame has trailing bytes".into(),
        ));
    }
    Ok((sequence, end, body))
}

pub struct RpcWorker {
    connection: AsyncConnection,
    pattern: String,
    receiver: BroadcastStream<Vec<u8>>,
    registration: RestorableRegistration,
    closed: bool,
}
impl RpcWorker {
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn deregister(mut self) -> Result<()> {
        self.closed = true;
        self.registration.deactivate();
        let mut e = PayloadEncoder::new();
        e.put_string(&self.pattern);
        decode_ok(
            &self
                .connection
                .request(message_type::RPC_UNSUBSCRIBE, e.finish())
                .await?,
        )
    }
}
fn decode_worker_registration(response: &[u8]) -> Result<u64> {
    decode_ok(response)?;
    Ok(0)
}
impl Stream for RpcWorker {
    type Item = Result<RpcRequest>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.closed {
            return Poll::Ready(None);
        }
        loop {
            match Pin::new(&mut self.receiver).poll_next(cx) {
                Poll::Ready(Some(Ok(payload))) => {
                    let mut d = PayloadDecoder::new(&payload);
                    let correlation_id = match d.get_fixed::<16>() {
                        Ok(v) => v,
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    };
                    let route = match d.get_string() {
                        Ok(v) => v,
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    };
                    if !route_matches_pattern(&route, &self.pattern) {
                        continue;
                    }
                    let body = match d.get_bytes() {
                        Ok(v) => v,
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    };
                    return Poll::Ready(Some(Ok(RpcRequest {
                        connection: self.connection.clone(),
                        correlation_id,
                        route,
                        body,
                        next_sequence: 0,
                        finished: false,
                        generation: self.connection.generation(),
                    })));
                }
                Poll::Ready(Some(Err(_))) => {
                    return Poll::Ready(Some(Err(FitzError::Backpressure(
                        "RPC worker stream is full".into(),
                    ))));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
pub struct RpcRequest {
    connection: AsyncConnection,
    correlation_id: [u8; 16],
    pub route: String,
    pub body: Vec<u8>,
    next_sequence: u64,
    finished: bool,
    generation: u64,
}
impl RpcRequest {
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn respond(&mut self, body: &[u8], is_end: bool) -> Result<()> {
        if self.finished || self.connection.generation() != self.generation {
            return Err(FitzError::StaleHandle);
        }
        let mut e = PayloadEncoder::new();
        e.put_raw(&self.correlation_id)
            .put_u64(self.next_sequence)
            .put_u8(u8::from(is_end))
            .put_bytes(body);
        self.connection
            .send(message_type::RPC_RESPONSE, e.finish())
            .await?;
        self.next_sequence += 1;
        self.finished = is_end;
        Ok(())
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn finish(mut self) -> Result<()> {
        if self.finished {
            Ok(())
        } else {
            self.respond(&[], true).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_preserve_empty_terminal_rpc_response_frame() {
        // Arrange
        let mut encoder = PayloadEncoder::new();
        encoder.put_u64(7).put_u8(1).put_bytes(&[]);
        let payload = encoder.finish();
        let mut decoder = PayloadDecoder::new(&payload);

        // Act
        let (sequence, end, body) = decode_rpc_response_frame(&mut decoder).unwrap();

        // Assert
        assert_eq!(sequence, 7);
        assert!(end);
        assert!(body.is_empty());
    }
}
