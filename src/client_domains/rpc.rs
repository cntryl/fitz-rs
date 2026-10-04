use super::decode_ok;
use crate::async_connection::{AsyncConnection, RestorableRegistration};
use crate::codec::{PayloadDecoder, PayloadEncoder};
use crate::domains::routes::{
    route_matches_pattern, validate_concrete_route, validate_registration_pattern,
};
use crate::protocol::message_type;
use crate::{FitzError, Result};
use futures_core::Stream;
use parking_lot::Mutex;
use std::collections::HashMap;
mod worker;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use worker::decode_worker_registration;
pub use worker::{RpcRequest, RpcWorker};
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
        self.call_with_timeout(route, body, None).await
    }

    /// Invokes an RPC route with an optional end-to-end budget.
    ///
    /// The timeout is sent to a supporting broker as a remaining budget. Use
    /// [`RpcResponseStream::cancel`] to request best-effort cancellation.
    ///
    /// # Errors
    /// Returns an error when validation or transport processing fails.
    pub async fn call_with_timeout(
        &self,
        route: &str,
        body: &[u8],
        timeout: Option<Duration>,
    ) -> Result<RpcResponseStream> {
        validate_concrete_route(route, "rpc")?;
        if timeout.is_some_and(|value| value > Duration::from_secs(86_400)) {
            return Err(FitzError::Protocol(
                "RPC timeout must not exceed one day".into(),
            ));
        }
        let deadline = timeout.map(|value| tokio::time::Instant::now() + value);
        let correlation_id = *Uuid::new_v4().as_bytes();
        let receiver = self
            .connection
            .notifications(message_type::RPC_RESPONSE, 64);
        let lifecycle_receiver = self
            .connection
            .notifications(message_type::RPC_LIFECYCLE, 64);
        let supports_cancellation =
            self.connection.capability_bits() & message_type::CAP_RPC_CANCELLATION != 0;
        let mut e = PayloadEncoder::new();
        e.put_raw(&correlation_id).put_string(route).put_bytes(body);
        if supports_cancellation && let Some(deadline) = deadline {
            let budget_ms = deadline
                .saturating_duration_since(tokio::time::Instant::now())
                .as_millis()
                .min(86_400_000) as u32;
            e.put_u8(1).put_u8(1).put_u32(budget_ms);
        }
        let mut generations = self.connection.generation_changes();
        self.connection
            .send(message_type::RPC_REQUEST, e.finish())
            .await?;
        let cancellation_sent = Arc::new(AtomicBool::new(false));
        let cancellation_shutdown = CancellationToken::new();
        if supports_cancellation && let Some(deadline) = deadline {
            let connection = self.connection.clone();
            let sent = Arc::clone(&cancellation_sent);
            let shutdown = cancellation_shutdown.clone();
            tokio::spawn(async move {
                tokio::select! {
                    () = tokio::time::sleep_until(deadline) => {
                        let _ = send_cancellation_once(&connection, correlation_id, &sent, 2).await;
                    }
                    () = shutdown.cancelled() => {}
                }
            });
        }
        let connection_closed = Box::pin(async move {
            let _ = generations.changed().await;
        });
        Ok(RpcResponseStream {
            connection: Some(self.connection.clone()),
            correlation_id,
            receiver: BroadcastStream::new(receiver),
            lifecycle_receiver,
            supports_cancellation,
            cancellation_sent,
            cancellation_outcome: None,
            timeout: deadline.map(|value| Box::pin(tokio::time::sleep_until(value))),
            parent_cancellation: None,
            cancellation_shutdown,
            connection_closed: Some(connection_closed),
            finished: false,
        })
    }

    /// Invokes a downstream RPC using the remaining budget and cancellation of an inbound call.
    ///
    /// The returned stream ends when the inbound request is cancelled, and the client sends a
    /// best-effort cancellation to the downstream broker when that request's cancellation token
    /// is cancelled.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn call_from_request(
        &self,
        request: &RpcRequest,
        route: &str,
        body: &[u8],
    ) -> Result<RpcResponseStream> {
        let mut stream = self
            .call_with_timeout(route, body, request.remaining_time())
            .await?;
        let parent_cancellation = request.cancellation.clone();
        stream.parent_cancellation = Some(Box::pin(parent_cancellation.clone().cancelled_owned()));
        if stream.supports_cancellation {
            let Some(connection) = stream.connection.clone() else {
                return Ok(stream);
            };
            let correlation_id = stream.correlation_id;
            let cancellation_sent = Arc::clone(&stream.cancellation_sent);
            let cancellation_shutdown = stream.cancellation_shutdown.clone();
            tokio::spawn(async move {
                tokio::select! {
                    () = parent_cancellation.cancelled() => {
                        let _ = send_cancellation_once(
                            &connection,
                            correlation_id,
                            &cancellation_sent,
                            1,
                        ).await;
                    }
                    () = cancellation_shutdown.cancelled() => {}
                }
            });
        }
        Ok(stream)
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn register_worker(&self, pattern: &str, max_concurrency: u32) -> Result<RpcWorker> {
        validate_registration_pattern(pattern, "rpc", 0)?;
        validate_worker_concurrency(max_concurrency)?;
        let receiver = self
            .connection
            .timed_notifications(message_type::RPC_REQUEST, 1024);
        let lifecycle_receiver = self
            .connection
            .notifications(message_type::RPC_LIFECYCLE, 1024);
        let mut e = PayloadEncoder::new();
        e.put_string(pattern).put_u32(max_concurrency);
        let supports_cancellation =
            self.connection.capability_bits() & message_type::CAP_RPC_CANCELLATION != 0;
        if supports_cancellation {
            e.put_u8(1).put_u8(1);
        }
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
        Ok(worker::start(
            self.connection.clone(),
            pattern.to_owned(),
            receiver,
            lifecycle_receiver,
            registration,
            supports_cancellation,
            max_concurrency as usize,
        ))
    }
}

fn validate_worker_concurrency(max_concurrency: u32) -> Result<()> {
    if (1..=1024).contains(&max_concurrency) {
        Ok(())
    } else {
        Err(FitzError::Protocol(
            "max_concurrency must be between 1 and 1024".into(),
        ))
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcResponseFrame {
    pub body: Vec<u8>,
    pub sequence: u64,
}
pub struct RpcResponseStream {
    connection: Option<AsyncConnection>,
    correlation_id: [u8; 16],
    receiver: BroadcastStream<Vec<u8>>,
    lifecycle_receiver: broadcast::Receiver<Vec<u8>>,
    supports_cancellation: bool,
    cancellation_sent: Arc<AtomicBool>,
    cancellation_outcome: Option<RpcCancellationOutcome>,
    timeout: Option<Pin<Box<tokio::time::Sleep>>>,
    parent_cancellation: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    cancellation_shutdown: CancellationToken,
    connection_closed: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    finished: bool,
}

/// Result of a best-effort RPC cancellation request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcCancellationOutcome {
    NotRequested,
    RequestNotSent,
    Unsupported,
    QueuedRemoved,
    Forwarded,
    WorkerUnsupported,
    AlreadyTerminal,
    UnknownOrUnauthorized,
    ForwardingFailed,
    Unconfirmed,
    ConnectionClosed,
}

impl RpcResponseStream {
    fn abandon(&mut self, reason: u8) {
        self.finished = true;
        self.timeout = None;
        self.parent_cancellation = None;
        self.cancellation_shutdown.cancel();
        if self.supports_cancellation
            && let Some(connection) = self.connection.clone()
        {
            spawn_cancellation_once(
                connection,
                self.correlation_id,
                &self.cancellation_sent,
                reason,
            );
        } else {
            self.cancellation_outcome = Some(RpcCancellationOutcome::Unsupported);
        }
    }

    /// Requests best-effort cancellation and waits for the broker's lifecycle result.
    ///
    /// A `Forwarded` outcome confirms that the broker routed a signal to the worker. It
    /// does not confirm that worker code or side effects have stopped.
    pub async fn cancel(&mut self) -> RpcCancellationOutcome {
        if let Some(outcome) = self.cancellation_outcome {
            return outcome;
        }
        if self.finished
            && self.parent_cancellation.is_none()
            && !self.cancellation_sent.load(Ordering::Acquire)
        {
            return RpcCancellationOutcome::AlreadyTerminal;
        }
        if !self.supports_cancellation {
            self.finished = true;
            self.timeout = None;
            self.cancellation_shutdown.cancel();
            self.cancellation_outcome = Some(RpcCancellationOutcome::Unsupported);
            return RpcCancellationOutcome::Unsupported;
        }

        let Some(connection) = &self.connection else {
            return RpcCancellationOutcome::RequestNotSent;
        };
        if let Err(error) =
            send_cancellation_once(connection, self.correlation_id, &self.cancellation_sent, 1)
                .await
        {
            let outcome = if matches!(error, FitzError::Closed | FitzError::ConnectionClosed) {
                RpcCancellationOutcome::ConnectionClosed
            } else {
                RpcCancellationOutcome::RequestNotSent
            };
            self.finished = true;
            self.timeout = None;
            self.cancellation_shutdown.cancel();
            self.cancellation_outcome = Some(outcome);
            return outcome;
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            match tokio::time::timeout_at(deadline, self.lifecycle_receiver.recv()).await {
                Ok(Ok(payload)) => {
                    if let Some(outcome) =
                        decode_cancellation_result(&payload, &self.correlation_id)
                    {
                        self.finished = true;
                        self.timeout = None;
                        self.cancellation_shutdown.cancel();
                        self.cancellation_outcome = Some(outcome);
                        return outcome;
                    }
                }
                Ok(Err(broadcast::error::RecvError::Lagged(_))) => {}
                Ok(Err(broadcast::error::RecvError::Closed)) | Err(_) => {
                    self.finished = true;
                    self.timeout = None;
                    self.cancellation_shutdown.cancel();
                    self.cancellation_outcome = Some(RpcCancellationOutcome::Unconfirmed);
                    return RpcCancellationOutcome::Unconfirmed;
                }
            }
        }
    }
}

fn decode_cancellation_result(
    payload: &[u8],
    correlation_id: &[u8; 16],
) -> Option<RpcCancellationOutcome> {
    if payload.len() != 18 || payload[0] != 4 || payload[1..17] != correlation_id[..] {
        return None;
    }
    Some(match payload[17] {
        1 => RpcCancellationOutcome::QueuedRemoved,
        2 => RpcCancellationOutcome::Forwarded,
        3 => RpcCancellationOutcome::WorkerUnsupported,
        4 => RpcCancellationOutcome::AlreadyTerminal,
        5 => RpcCancellationOutcome::UnknownOrUnauthorized,
        6 => RpcCancellationOutcome::ForwardingFailed,
        _ => return None,
    })
}
impl Stream for RpcResponseStream {
    type Item = Result<RpcResponseFrame>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        if self
            .connection_closed
            .as_mut()
            .is_some_and(|closed| closed.as_mut().poll(cx).is_ready())
        {
            self.finished = true;
            self.cancellation_shutdown.cancel();
            self.cancellation_outcome = Some(RpcCancellationOutcome::ConnectionClosed);
            return Poll::Ready(Some(Err(FitzError::ConnectionClosed)));
        }
        if self
            .parent_cancellation
            .as_mut()
            .is_some_and(|parent| parent.as_mut().poll(cx).is_ready())
        {
            self.abandon(1);
            return Poll::Ready(None);
        }
        if self
            .timeout
            .as_mut()
            .is_some_and(|timeout| timeout.as_mut().poll(cx).is_ready())
        {
            self.abandon(2);
            return Poll::Ready(Some(Err(FitzError::Timeout)));
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
                        self.timeout = None;
                        self.parent_cancellation = None;
                        self.cancellation_shutdown.cancel();
                        if let Some(error) = decode_terminal_error(&body) {
                            return Poll::Ready(Some(Err(error)));
                        }
                    }
                    return Poll::Ready(Some(Ok(RpcResponseFrame { body, sequence })));
                }
                Poll::Ready(Some(Err(_))) => {
                    self.abandon(1);
                    return Poll::Ready(Some(Err(FitzError::Backpressure(
                        "RPC response stream is full".into(),
                    ))));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
}

impl Drop for RpcResponseStream {
    fn drop(&mut self) {
        self.cancellation_shutdown.cancel();
        if self.finished || !self.supports_cancellation {
            return;
        }
        let Some(connection) = self.connection.clone() else {
            return;
        };
        spawn_cancellation_once(connection, self.correlation_id, &self.cancellation_sent, 1);
    }
}

fn spawn_cancellation_once(
    connection: AsyncConnection,
    correlation_id: [u8; 16],
    cancellation_sent: &AtomicBool,
    reason: u8,
) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    if cancellation_sent.swap(true, Ordering::AcqRel) {
        return;
    }
    runtime.spawn(async move {
        let _ = send_cancellation_message(&connection, correlation_id, reason).await;
    });
}

async fn send_cancellation_once(
    connection: &AsyncConnection,
    correlation_id: [u8; 16],
    cancellation_sent: &AtomicBool,
    reason: u8,
) -> Result<()> {
    if cancellation_sent.swap(true, Ordering::AcqRel) {
        return Ok(());
    }
    send_cancellation_message(connection, correlation_id, reason).await
}

async fn send_cancellation_message(
    connection: &AsyncConnection,
    correlation_id: [u8; 16],
    reason: u8,
) -> Result<()> {
    let mut encoder = PayloadEncoder::new();
    encoder.put_u8(1).put_raw(&correlation_id).put_u8(reason);
    connection
        .send(message_type::RPC_CANCEL, encoder.finish())
        .await
}

fn decode_terminal_error(body: &[u8]) -> Option<FitzError> {
    let mut decoder = PayloadDecoder::new(body);
    if decoder.get_u8().ok()? != 1 {
        return None;
    }
    let code = decoder.get_u32().ok()?;
    if !(6001..=6013).contains(&code) {
        return None;
    }
    let message = decoder.get_string().ok()?;
    decoder
        .is_empty()
        .then_some(FitzError::Domain { code, message })
}

fn decode_rpc_response_frame(decoder: &mut PayloadDecoder<'_>) -> Result<(u64, bool, Vec<u8>)> {
    let sequence = decoder.get_u64()?;
    let end = match decoder.get_u8()? {
        0 => false,
        1 => true,
        flags => {
            return Err(FitzError::Protocol(format!(
                "RPC response contains unsupported flags {flags}"
            )));
        }
    };
    let body = decoder.get_bytes()?;
    if !decoder.is_empty() {
        return Err(FitzError::Protocol(
            "RPC response frame has trailing bytes".into(),
        ));
    }
    Ok((sequence, end, body))
}

struct ActiveWorkerCall {
    cancellation: CancellationToken,
    cancellation_requested: bool,
}

fn cancel_worker_call(active_calls: &Mutex<HashMap<[u8; 16], ActiveWorkerCall>>, payload: &[u8]) {
    if payload.len() != 18 || payload[0] != 2 || !(1..=4).contains(&payload[17]) {
        return;
    }
    let correlation_id: [u8; 16] = payload[1..17].try_into().expect("fixed length checked");
    let cancellation = {
        let mut calls = active_calls.lock();
        let Some(call) = calls.get_mut(&correlation_id) else {
            return;
        };
        call.cancellation_requested = true;
        call.cancellation.clone()
    };
    cancellation.cancel();
}

fn decode_rpc_request_budget(decoder: &mut PayloadDecoder<'_>) -> Result<Option<Duration>> {
    if decoder.is_empty() {
        return Ok(None);
    }
    let version = decoder.get_u8()?;
    let flags = decoder.get_u8()?;
    let budget_ms = decoder.get_u32()?;
    if !decoder.is_empty() || version != 1 || flags != 1 || budget_ms > 86_400_000 {
        return Err(FitzError::Protocol(
            "invalid RPC request budget extension".into(),
        ));
    }
    Ok(Some(Duration::from_millis(u64::from(budget_ms))))
}

#[cfg(test)]
mod tests;
