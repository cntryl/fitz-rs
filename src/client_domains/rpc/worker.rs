use super::{
    ActiveWorkerCall, Arc, AsyncConnection, CancellationToken, Context, Duration, FitzError,
    HashMap, Mutex, PayloadDecoder, PayloadEncoder, Pin, Poll, RestorableRegistration, Result,
    Stream, broadcast, cancel_worker_call, decode_ok, decode_rpc_request_budget, message_type,
    route_matches_pattern,
};
use crate::notifications::ReceivedNotification;
use futures_util::task::AtomicWaker;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};

struct WorkerInbox {
    requests: Mutex<VecDeque<Result<RpcRequest>>>,
    waker: AtomicWaker,
    closed: AtomicBool,
    capacity: usize,
}

impl WorkerInbox {
    fn push(&self, request: Result<RpcRequest>) -> Option<Result<RpcRequest>> {
        let mut requests = self.requests.lock();
        if self.closed.load(Ordering::Acquire) || requests.len() >= self.capacity {
            return Some(request);
        }
        requests.push_back(request);
        drop(requests);
        self.waker.wake();
        None
    }

    fn remove(&self, correlation_id: &[u8; 16]) -> Option<Result<RpcRequest>> {
        let mut requests = self.requests.lock();
        let index = requests.iter().position(|request| {
            request
                .as_ref()
                .is_ok_and(|request| &request.correlation_id == correlation_id)
        })?;
        requests.remove(index)
    }

    fn clear(&self) {
        let requests = std::mem::take(&mut *self.requests.lock());
        drop(requests);
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.waker.wake();
    }

    fn poll_next(&self, cx: &mut Context<'_>) -> Poll<Option<Result<RpcRequest>>> {
        let mut requests = self.requests.lock();
        if let Some(request) = requests.pop_front() {
            return Poll::Ready(Some(request));
        }
        self.waker.register(cx.waker());
        if self.closed.load(Ordering::Acquire) {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

pub(super) fn start(
    connection: AsyncConnection,
    pattern: String,
    mut requests: broadcast::Receiver<ReceivedNotification>,
    mut lifecycle: broadcast::Receiver<Vec<u8>>,
    registration: RestorableRegistration,
    supports_cancellation: bool,
    capacity: usize,
) -> RpcWorker {
    let active_calls = Arc::new(Mutex::new(HashMap::<[u8; 16], ActiveWorkerCall>::new()));
    let shutdown = CancellationToken::new();
    let inbox = Arc::new(WorkerInbox {
        requests: Mutex::new(VecDeque::new()),
        waker: AtomicWaker::new(),
        closed: AtomicBool::new(false),
        capacity,
    });
    let worker = RpcWorker {
        connection: connection.clone(),
        pattern: pattern.clone(),
        inbox: Arc::clone(&inbox),
        registration,
        active_calls: Arc::clone(&active_calls),
        cancellation_shutdown: shutdown.clone(),
        closed: false,
    };
    let mut generations = connection.generation_changes();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => break,
                changed = generations.changed() => {
                    if changed.is_err() { break; }
                    for call in active_calls.lock().values() { call.cancellation.cancel(); }
                    active_calls.lock().clear();
                    inbox.clear();
                }
                event = requests.recv() => match event {
                    Ok(received) if received.generation == connection.generation() => {
                        let request = decode_request(&connection, &pattern, &active_calls, supports_cancellation, &received);
                        if let Some(request) = request
                            && let Some(Ok(mut rejected)) = inbox.push(request) {
                                let mut body = PayloadEncoder::new();
                                body.put_u8(1).put_u32(6003).put_string("Local RPC worker is overloaded");
                                let _ = rejected.respond(&body.finish(), true).await;
                        }
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        let _ = inbox.push(Err(FitzError::Backpressure("RPC worker stream is full".into())));
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                },
                event = lifecycle.recv() => match event {
                    Ok(payload) => {
                        if payload.len() == 18 && payload[0] == 2 && (1..=4).contains(&payload[17]) {
                            let correlation_id: [u8; 16] = payload[1..17].try_into().expect("validated UUID length");
                            if let Some(request) = inbox.remove(&correlation_id) {
                                drop(request);
                            } else {
                                cancel_worker_call(&active_calls, &payload);
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
        inbox.close();
    });
    worker
}

fn decode_request(
    connection: &AsyncConnection,
    pattern: &str,
    active_calls: &Arc<Mutex<HashMap<[u8; 16], ActiveWorkerCall>>>,
    supports_cancellation: bool,
    received: &ReceivedNotification,
) -> Option<Result<RpcRequest>> {
    let mut decoder = PayloadDecoder::new(&received.payload);
    let result = (|| {
        let correlation_id = decoder.get_fixed::<16>()?;
        let route = decoder.get_string()?;
        if !route_matches_pattern(&route, pattern) {
            return Ok(None);
        }
        let body = decoder.get_bytes()?;
        if !supports_cancellation && !decoder.is_empty() {
            return Err(FitzError::Protocol(
                "unnegotiated RPC budget extension".into(),
            ));
        }
        let budget = decode_rpc_request_budget(&mut decoder)?;
        let cancellation = CancellationToken::new();
        active_calls.lock().insert(
            correlation_id,
            ActiveWorkerCall {
                cancellation: cancellation.clone(),
                cancellation_requested: false,
            },
        );
        let deadline = budget.map(|budget| received.received_at + budget);
        let cleanup = CancellationToken::new();
        if let Some(deadline) = deadline {
            if deadline <= tokio::time::Instant::now() {
                cancellation.cancel();
            } else {
                let token = cancellation.clone();
                let shutdown = cleanup.clone();
                tokio::spawn(async move {
                    tokio::select! {
                        () = tokio::time::sleep_until(deadline) => token.cancel(),
                        () = shutdown.cancelled() => {}
                    }
                });
            }
        }
        Ok(Some(RpcRequest {
            connection: connection.clone(),
            correlation_id,
            route,
            body,
            cancellation,
            deadline,
            cleanup,
            active_calls: Arc::clone(active_calls),
            supports_cancellation,
            next_sequence: 0,
            finished: false,
            cleanup_done: false,
            generation: received.generation,
        }))
    })();
    match result {
        Ok(request) => request.map(Ok),
        Err(error) => Some(Err(error)),
    }
}

pub struct RpcWorker {
    connection: AsyncConnection,
    pattern: String,
    inbox: Arc<WorkerInbox>,
    registration: RestorableRegistration,
    active_calls: Arc<Mutex<HashMap<[u8; 16], ActiveWorkerCall>>>,
    cancellation_shutdown: CancellationToken,
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
impl Drop for RpcWorker {
    fn drop(&mut self) {
        self.cancellation_shutdown.cancel();
        for call in self.active_calls.lock().values() {
            call.cancellation.cancel();
        }
        self.inbox.close();
        self.inbox.clear();
    }
}
pub(super) fn decode_worker_registration(response: &[u8]) -> Result<u64> {
    decode_ok(response)?;
    Ok(0)
}
impl Stream for RpcWorker {
    type Item = Result<RpcRequest>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.closed {
            return Poll::Ready(None);
        }
        self.inbox.poll_next(cx)
    }
}
pub struct RpcRequest {
    connection: AsyncConnection,
    correlation_id: [u8; 16],
    pub route: String,
    pub body: Vec<u8>,
    /// Cancelled when the broker asks this worker to stop the invocation.
    pub cancellation: CancellationToken,
    deadline: Option<tokio::time::Instant>,
    cleanup: CancellationToken,
    active_calls: Arc<Mutex<HashMap<[u8; 16], ActiveWorkerCall>>>,
    supports_cancellation: bool,
    next_sequence: u64,
    finished: bool,
    cleanup_done: bool,
    generation: u64,
}
impl RpcRequest {
    /// Returns the current remaining request budget, if the caller sent one.
    #[must_use]
    pub fn remaining_time(&self) -> Option<Duration> {
        self.deadline
            .map(|deadline| deadline.saturating_duration_since(tokio::time::Instant::now()))
    }

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

    fn finish_cleanup(&mut self) {
        if self.cleanup_done {
            return;
        }
        self.cleanup_done = true;
        self.cleanup.cancel();
        drop(std::mem::take(&mut self.body));
        drop(std::mem::take(&mut self.route));
        self.active_calls.lock().remove(&self.correlation_id);
        if self.supports_cancellation && self.connection.generation() == self.generation {
            let connection = self.connection.clone();
            let correlation_id = self.correlation_id;
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    let mut encoder = PayloadEncoder::new();
                    encoder.put_u8(3).put_raw(&correlation_id);
                    let _ = connection
                        .send(message_type::RPC_CANCEL, encoder.finish())
                        .await;
                });
            }
        }
    }
}

impl Drop for RpcRequest {
    fn drop(&mut self) {
        self.finish_cleanup();
    }
}
