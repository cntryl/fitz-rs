use crate::codec::{decode_message_frame, try_encode_message_frame};
use crate::{
    ConnectionState, FitzAttributes, FitzError, FitzLifecycleEvent, FitzObservability,
    HeartbeatOptions, ReconnectPolicy, Result, RetryPolicy, TokenProvider,
};
use futures_util::{SinkExt, StreamExt};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio_tungstenite::tungstenite::Message;
use tracing::Instrument;

type Response = oneshot::Sender<Result<Vec<u8>>>;
type RegistrationDecoder = Arc<dyn Fn(&[u8]) -> Result<u64> + Send + Sync>;

#[derive(Clone)]
struct Registration {
    message_type: u16,
    payload: Vec<u8>,
    wire_id: Arc<AtomicU64>,
    decoder: RegistrationDecoder,
}

pub(crate) struct RestorableRegistration {
    key: u64,
    wire_id: Arc<AtomicU64>,
    registrations: Arc<parking_lot::Mutex<HashMap<u64, Registration>>>,
    active: bool,
}

impl RestorableRegistration {
    pub(crate) fn wire_id(&self) -> u64 {
        self.wire_id.load(Ordering::Acquire)
    }

    pub(crate) fn deactivate(&mut self) {
        if self.active {
            self.registrations.lock().remove(&self.key);
            self.active = false;
        }
    }
}

impl Drop for RestorableRegistration {
    fn drop(&mut self) {
        self.deactivate();
    }
}

struct PendingRequest {
    id: u64,
    response: Option<Response>,
}

enum Command {
    Connect(oneshot::Sender<Result<()>>),
    Request {
        id: u64,
        message_type: u16,
        payload: Vec<u8>,
        response: Response,
    },
    Send {
        message_type: u16,
        payload: Vec<u8>,
        response: oneshot::Sender<Result<()>>,
    },
    Close(oneshot::Sender<()>),
}

#[derive(Clone)]
pub(crate) struct AsyncConnection {
    commands: mpsc::Sender<Command>,
    cancellations: mpsc::UnboundedSender<u64>,
    next_request_id: Arc<AtomicU64>,
    generation: Arc<AtomicU64>,
    notifications: Arc<parking_lot::Mutex<HashMap<u16, broadcast::Sender<Vec<u8>>>>>,
    registrations: Arc<parking_lot::Mutex<HashMap<u64, Registration>>>,
    next_registration_id: Arc<AtomicU64>,
    timeout: Duration,
    retry: RetryPolicy,
}

pub(crate) struct AsyncConnectionOptions {
    pub endpoint: String,
    pub token_provider: Arc<dyn TokenProvider>,
    pub timeout: Duration,
    pub max_queued: usize,
    pub reconnect: ReconnectPolicy,
    pub retry: RetryPolicy,
    pub heartbeat: HeartbeatOptions,
    pub observability: FitzObservability,
    pub state: watch::Sender<ConnectionState>,
}

impl AsyncConnection {
    pub(crate) fn spawn(options: AsyncConnectionOptions) -> Self {
        let AsyncConnectionOptions {
            endpoint,
            token_provider,
            timeout,
            max_queued,
            reconnect,
            retry,
            heartbeat,
            observability,
            state,
        } = options;
        let (commands, command_rx) = mpsc::channel(max_queued.max(1));
        let (cancellations, cancellation_rx) = mpsc::unbounded_channel();
        let notifications = Arc::new(parking_lot::Mutex::new(HashMap::new()));
        let generation = Arc::new(AtomicU64::new(0));
        let registrations = Arc::new(parking_lot::Mutex::new(HashMap::new()));
        tokio::spawn(supervise(Supervisor {
            endpoint,
            token_provider,
            timeout,
            reconnect,
            heartbeat,
            observability,
            state,
            commands: command_rx,
            cancellations: cancellation_rx,
            notifications: Arc::clone(&notifications),
            generation: Arc::clone(&generation),
            registrations: Arc::clone(&registrations),
        }));
        Self {
            commands,
            cancellations,
            next_request_id: Arc::new(AtomicU64::new(1)),
            generation,
            notifications,
            registrations,
            next_registration_id: Arc::new(AtomicU64::new(1)),
            timeout,
            retry,
        }
    }

    pub(crate) async fn connect(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::Connect(tx))
            .await
            .map_err(|_| FitzError::Closed)?;
        rx.await.map_err(|_| FitzError::Closed)?
    }

    pub(crate) async fn request(&self, message_type: u16, payload: Vec<u8>) -> Result<Vec<u8>> {
        let span =
            tracing::info_span!("fitz.request", message_type, generation = self.generation());
        async {
            let id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
            let (tx, rx) = oneshot::channel();
            self.commands
                .try_send(Command::Request {
                    id,
                    message_type,
                    payload,
                    response: tx,
                })
                .map_err(|error| match error {
                    mpsc::error::TrySendError::Full(_) => {
                        tracing::warn!(message_type, "fitz outbound request backpressure");
                        FitzError::Backpressure("outbound request queue is full".into())
                    }
                    mpsc::error::TrySendError::Closed(_) => FitzError::Closed,
                })?;
            let mut guard = CancellationGuard {
                id,
                cancellations: self.cancellations.clone(),
                armed: true,
            };
            let result = tokio::time::timeout(self.timeout, rx)
                .await
                .map_err(|_| FitzError::Timeout)?
                .map_err(|_| FitzError::ConnectionClosed)?;
            guard.armed = false;
            result
        }
        .instrument(span)
        .await
    }

    pub(crate) async fn request_replayable(
        &self,
        message_type: u16,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>> {
        let attempts = if self.retry.enabled {
            self.retry.maximum_attempts.max(1)
        } else {
            1
        };
        let mut delay = self.retry.base_delay;
        for attempt in 1..=attempts {
            match self.request(message_type, payload.clone()).await {
                Ok(response) => return Ok(response),
                Err(error) if attempt == attempts || !error.is_retryable() => return Err(error),
                Err(_) => tokio::time::sleep(delay).await,
            }
            delay = delay.saturating_mul(2).min(self.retry.maximum_delay);
        }
        Err(FitzError::ConnectionClosed)
    }

    pub(crate) async fn request_confirmed_negative(
        &self,
        message_type: u16,
        payload: Vec<u8>,
        retryable_rejection: impl Fn(&[u8]) -> bool,
    ) -> Result<Vec<u8>> {
        let attempts = if self.retry.enabled {
            self.retry.maximum_attempts.max(1)
        } else {
            1
        };
        let mut delay = self.retry.base_delay;
        for attempt in 1..=attempts {
            let response = self.request(message_type, payload.clone()).await?;
            if attempt == attempts || !retryable_rejection(&response) {
                return Ok(response);
            }
            tokio::time::sleep(delay).await;
            delay = delay.saturating_mul(2).min(self.retry.maximum_delay);
        }
        Err(FitzError::ConnectionClosed)
    }

    pub(crate) async fn send(&self, message_type: u16, payload: Vec<u8>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .try_send(Command::Send {
                message_type,
                payload,
                response: tx,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    FitzError::Backpressure("outbound request queue is full".into())
                }
                mpsc::error::TrySendError::Closed(_) => FitzError::Closed,
            })?;
        tokio::time::timeout(self.timeout, rx)
            .await
            .map_err(|_| FitzError::Timeout)?
            .map_err(|_| FitzError::ConnectionClosed)?
    }

    pub(crate) fn notifications(
        &self,
        message_type: u16,
        capacity: usize,
    ) -> broadcast::Receiver<Vec<u8>> {
        let mut notifications = self.notifications.lock();
        notifications
            .entry(message_type)
            .or_insert_with(|| broadcast::channel(capacity.max(1)).0)
            .subscribe()
    }

    pub(crate) async fn close(&self) {
        let (tx, rx) = oneshot::channel();
        let _ = self.commands.send(Command::Close(tx)).await;
        let _ = rx.await;
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub(crate) fn register_restorable(
        &self,
        message_type: u16,
        payload: Vec<u8>,
        wire_id: u64,
        decoder: impl Fn(&[u8]) -> Result<u64> + Send + Sync + 'static,
    ) -> RestorableRegistration {
        let key = self.next_registration_id.fetch_add(1, Ordering::Relaxed);
        let wire_id = Arc::new(AtomicU64::new(wire_id));
        self.registrations.lock().insert(
            key,
            Registration {
                message_type,
                payload,
                wire_id: Arc::clone(&wire_id),
                decoder: Arc::new(decoder),
            },
        );
        RestorableRegistration {
            key,
            wire_id,
            registrations: Arc::clone(&self.registrations),
            active: true,
        }
    }
}

struct CancellationGuard {
    id: u64,
    cancellations: mpsc::UnboundedSender<u64>,
    armed: bool,
}

impl Drop for CancellationGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.cancellations.send(self.id);
        }
    }
}

struct Supervisor {
    endpoint: String,
    token_provider: Arc<dyn TokenProvider>,
    timeout: Duration,
    reconnect: ReconnectPolicy,
    heartbeat: HeartbeatOptions,
    observability: FitzObservability,
    state: watch::Sender<ConnectionState>,
    commands: mpsc::Receiver<Command>,
    cancellations: mpsc::UnboundedReceiver<u64>,
    notifications: Arc<parking_lot::Mutex<HashMap<u16, broadcast::Sender<Vec<u8>>>>>,
    generation: Arc<AtomicU64>,
    registrations: Arc<parking_lot::Mutex<HashMap<u64, Registration>>>,
}

async fn supervise(mut supervisor: Supervisor) {
    while let Some(command) = supervisor.commands.recv().await {
        match command {
            Command::Connect(response) => {
                let _ = connect_and_run(&mut supervisor, Some(response)).await;
            }
            Command::Close(response) => {
                supervisor.state.send_replace(ConnectionState::Closed);
                emit_lifecycle(&supervisor, "closed");
                let _ = response.send(());
                return;
            }
            Command::Request { response, .. } => {
                let _ = response.send(Err(FitzError::ConnectionClosed));
            }
            Command::Send { response, .. } => {
                let _ = response.send(Err(FitzError::ConnectionClosed));
            }
        }
    }
}

async fn connect_and_run(
    supervisor: &mut Supervisor,
    mut initial_response: Option<oneshot::Sender<Result<()>>>,
) -> Result<()> {
    supervisor.state.send_replace(ConnectionState::Connecting);
    emit_lifecycle(supervisor, "connect_start");
    let mut attempts = 0_usize;
    loop {
        match open_session(supervisor).await {
            Ok(session) => {
                let mut session = session;
                if let Err(error) = restore_registrations(supervisor, &mut session).await {
                    tracing::warn!(%error, "fitz registration restoration failed");
                    supervisor.generation.fetch_add(1, Ordering::AcqRel);
                } else {
                    let reconnected = initial_response.is_none();
                    supervisor
                        .state
                        .send_replace(ConnectionState::Authenticated);
                    emit_lifecycle(
                        supervisor,
                        if reconnected {
                            "reconnect_succeeded"
                        } else {
                            "connect_succeeded"
                        },
                    );
                    if let Some(response) = initial_response.take() {
                        let _ = response.send(Ok(()));
                    }
                    let result = run_session(supervisor, session).await;
                    if matches!(result, SessionResult::Closed) {
                        return Ok(());
                    }
                    supervisor.generation.fetch_add(1, Ordering::AcqRel);
                    emit_lifecycle(supervisor, "connection_lost");
                    tracing::warn!(
                        generation = supervisor.generation.load(Ordering::Acquire),
                        "fitz transport disconnected"
                    );
                }
            }
            Err(error) if error.is_auth_failure() => {
                supervisor.state.send_replace(ConnectionState::Closed);
                emit_lifecycle(supervisor, "auth_rejected");
                if let Some(response) = initial_response.take() {
                    let _ = response.send(Err(FitzError::Authentication {
                        message: error.to_string(),
                    }));
                }
                return Err(error);
            }
            Err(error) if initial_response.is_some() => {
                supervisor.state.send_replace(ConnectionState::Disconnected);
                emit_lifecycle(supervisor, "connect_failed");
                if let Some(response) = initial_response.take() {
                    let _ = response.send(Err(FitzError::Connection(error.to_string())));
                }
                return Err(error);
            }
            Err(error) if !supervisor.reconnect.enabled => {
                supervisor.state.send_replace(ConnectionState::Disconnected);
                if let Some(response) = initial_response.take() {
                    let _ = response.send(Err(FitzError::Connection(error.to_string())));
                }
                return Err(error);
            }
            Err(_) => emit_lifecycle(supervisor, "reconnect_failed"),
        }
        attempts += 1;
        if supervisor.reconnect.maximum_attempts != 0
            && attempts > supervisor.reconnect.maximum_attempts
        {
            supervisor.state.send_replace(ConnectionState::Disconnected);
            if let Some(response) = initial_response.take() {
                let _ = response.send(Err(FitzError::Connection(
                    "reconnect attempts exhausted".into(),
                )));
            }
            return Err(FitzError::Connection("reconnect attempts exhausted".into()));
        }
        supervisor.state.send_replace(ConnectionState::Reconnecting);
        emit_lifecycle(supervisor, "reconnect_start");
        let multiplier = supervisor
            .reconnect
            .multiplier
            .powi(i32::try_from(attempts - 1).unwrap_or(i32::MAX));
        let delay = supervisor
            .reconnect
            .base_delay
            .mul_f64(multiplier)
            .min(supervisor.reconnect.maximum_delay);
        tracing::warn!(attempts, ?delay, "fitz reconnect scheduled");
        tokio::time::sleep(delay).await;
    }
}

enum Session {
    Tcp(TcpStream),
    WebSocket(
        Box<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>>,
    ),
}

async fn open_session(supervisor: &Supervisor) -> Result<Session> {
    let token = supervisor.token_provider.token().await?;
    supervisor.state.send_replace(ConnectionState::Connecting);
    let mut session = if let Some(address) = supervisor.endpoint.strip_prefix("tcp://") {
        let stream = tokio::time::timeout(supervisor.timeout, TcpStream::connect(address))
            .await
            .map_err(|_| FitzError::Timeout)?
            .map_err(FitzError::from)?;
        if supervisor.heartbeat.enabled {
            let socket = socket2::SockRef::from(&stream);
            socket.set_keepalive(true).map_err(FitzError::from)?;
            socket
                .set_tcp_keepalive(
                    &socket2::TcpKeepalive::new().with_time(supervisor.heartbeat.idle_interval),
                )
                .map_err(FitzError::from)?;
        }
        Session::Tcp(stream)
    } else if supervisor.endpoint.starts_with("ws://") || supervisor.endpoint.starts_with("wss://")
    {
        let (socket, _) = tokio::time::timeout(
            supervisor.timeout,
            tokio_tungstenite::connect_async(&supervisor.endpoint),
        )
        .await
        .map_err(|_| FitzError::Timeout)?
        .map_err(|error| FitzError::Transport(error.to_string()))?;
        Session::WebSocket(Box::new(socket))
    } else {
        return Err(FitzError::Connection(
            "endpoint must use tcp://, ws://, or wss://".into(),
        ));
    };
    supervisor
        .state
        .send_replace(ConnectionState::Authenticating);
    emit_lifecycle(supervisor, "auth_start");
    write_session(
        &mut session,
        crate::protocol::message_type::CONNECT,
        token.into_bytes(),
    )
    .await?;
    settle_authentication(&mut session).await?;
    Ok(session)
}

fn emit_lifecycle(supervisor: &Supervisor, name: &'static str) {
    let mut attributes = FitzAttributes::new();
    attributes.insert("endpoint".into(), supervisor.endpoint.clone());
    if let Some(logger) = &supervisor.observability.logger {
        logger.event(name, &attributes);
    }
    if let Some(tracer) = &supervisor.observability.tracer {
        tracer.start(name, &attributes).end();
    }
    if let Some(meter) = &supervisor.observability.meter {
        meter.increment("fitz.lifecycle.events", 1, &attributes);
    }
    if let Some(handler) = &supervisor.observability.lifecycle {
        handler(FitzLifecycleEvent {
            name,
            state: *supervisor.state.borrow(),
            attributes,
        });
    }
}

async fn settle_authentication(session: &mut Session) -> Result<()> {
    const SETTLE_DELAY: Duration = Duration::from_millis(100);
    match session {
        Session::Tcp(stream) => {
            let mut byte = [0_u8; 1];
            match tokio::time::timeout(SETTLE_DELAY, stream.peek(&mut byte)).await {
                Err(_) => Ok(()),
                Ok(Ok(0)) => Err(FitzError::Authentication {
                    message: "broker closed the connection during authentication".into(),
                }),
                Ok(Ok(_)) => Err(FitzError::Protocol(
                    "broker sent an unexpected authentication response".into(),
                )),
                Ok(Err(error)) => Err(FitzError::Authentication {
                    message: error.to_string(),
                }),
            }
        }
        Session::WebSocket(socket) => {
            match tokio::time::timeout(SETTLE_DELAY, socket.next()).await {
                Err(_) => Ok(()),
                Ok(Some(Ok(Message::Ping(payload)))) => {
                    socket
                        .send(Message::Pong(payload))
                        .await
                        .map_err(|error| FitzError::Transport(error.to_string()))?;
                    Ok(())
                }
                Ok(Some(Ok(_)) | None) => Err(FitzError::Authentication {
                    message: "broker closed the connection during authentication".into(),
                }),
                Ok(Some(Err(error))) => Err(FitzError::Authentication {
                    message: error.to_string(),
                }),
            }
        }
    }
}

enum SessionResult {
    Disconnected,
    Closed,
}

async fn run_session(supervisor: &mut Supervisor, session: Session) -> SessionResult {
    match session {
        Session::Tcp(stream) => {
            let (reader, writer) = stream.into_split();
            run_io(supervisor, reader, writer).await
        }
        Session::WebSocket(socket) => run_websocket(supervisor, *socket).await,
    }
}

async fn run_io<R, W>(supervisor: &mut Supervisor, mut reader: R, mut writer: W) -> SessionResult
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut pending: HashMap<u16, VecDeque<PendingRequest>> = HashMap::new();
    let mut canceled = HashSet::new();
    loop {
        tokio::select! {
            command = supervisor.commands.recv() => match command {
                Some(Command::Request { id, message_type, payload, response }) => {
                    let response = (!canceled.remove(&id)).then_some(response);
                    pending.entry(message_type).or_default().push_back(PendingRequest { id, response });
                    if write_tcp(&mut writer, message_type, payload).await.is_err() { fail_pending(&mut pending); return SessionResult::Disconnected; }
                }
                Some(Command::Send { message_type, payload, response }) => { let _ = response.send(write_tcp(&mut writer, message_type, payload).await); }
                Some(Command::Connect(response)) => { let _ = response.send(Ok(())); }
                Some(Command::Close(response)) => { let _ = writer.shutdown().await; let _=response.send(()); fail_pending(&mut pending); supervisor.state.send_replace(ConnectionState::Closed); emit_lifecycle(supervisor, "closed"); return SessionResult::Closed; }
                None => { let _ = writer.shutdown().await; fail_pending(&mut pending); return SessionResult::Closed; }
            },
            Some(id) = supervisor.cancellations.recv() => if !cancel_pending(&mut pending, id) { canceled.insert(id); },
            frame = read_tcp(&mut reader) => if let Ok((message_type, payload)) = frame {
                dispatch(supervisor, &mut pending, message_type, payload);
            } else {
                fail_pending(&mut pending);
                return SessionResult::Disconnected;
            }
        }
    }
}

async fn run_websocket(
    supervisor: &mut Supervisor,
    socket: tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
) -> SessionResult {
    let (mut writer, mut reader) = socket.split();
    let mut pending: HashMap<u16, VecDeque<PendingRequest>> = HashMap::new();
    let mut canceled = HashSet::new();
    let heartbeat_enabled = supervisor.heartbeat.enabled;
    let heartbeat_idle = supervisor.heartbeat.idle_interval;
    let heartbeat_timeout = supervisor.heartbeat.timeout;
    let heartbeat = tokio::time::sleep(heartbeat_idle);
    tokio::pin!(heartbeat);
    let mut awaiting_pong = false;
    loop {
        tokio::select! {
            command = supervisor.commands.recv() => match command {
                Some(Command::Request { id, message_type, payload, response }) => {
                    let response = (!canceled.remove(&id)).then_some(response);
                    pending.entry(message_type).or_default().push_back(PendingRequest { id, response });
                    let frame = try_encode_message_frame(message_type, &payload);
                    if frame.is_err() || writer.send(Message::Binary(frame.unwrap_or_default().into())).await.is_err() { fail_pending(&mut pending); return SessionResult::Disconnected; }
                    awaiting_pong = false;
                    heartbeat.as_mut().reset(tokio::time::Instant::now() + heartbeat_idle);
                }
                Some(Command::Send { message_type, payload, response }) => { let result = async { let frame=try_encode_message_frame(message_type,&payload)?; writer.send(Message::Binary(frame.into())).await.map_err(|e| FitzError::Transport(e.to_string())) }.await; let _=response.send(result); }
                Some(Command::Connect(response)) => { let _ = response.send(Ok(())); }
                Some(Command::Close(response)) => { let _=writer.close().await; let _=response.send(()); fail_pending(&mut pending); supervisor.state.send_replace(ConnectionState::Closed); emit_lifecycle(supervisor, "closed"); return SessionResult::Closed; }
                None => { let _=writer.close().await; fail_pending(&mut pending); return SessionResult::Closed; }
            },
            Some(id) = supervisor.cancellations.recv() => if !cancel_pending(&mut pending, id) { canceled.insert(id); },
            message = reader.next() => match message {
                Some(Ok(Message::Binary(frame))) => if let Ok((message_type, payload_start)) = decode_message_frame(&frame) {
                    awaiting_pong = false;
                    heartbeat.as_mut().reset(tokio::time::Instant::now() + heartbeat_idle);
                    dispatch(supervisor, &mut pending, message_type, frame[payload_start..].to_vec());
                } else {
                    fail_pending(&mut pending);
                    return SessionResult::Disconnected;
                },
                Some(Ok(Message::Ping(payload))) => {
                    if writer.send(Message::Pong(payload)).await.is_err() { fail_pending(&mut pending); return SessionResult::Disconnected; }
                    heartbeat.as_mut().reset(tokio::time::Instant::now() + heartbeat_idle);
                },
                Some(Ok(Message::Pong(_))) => {
                    awaiting_pong = false;
                    heartbeat.as_mut().reset(tokio::time::Instant::now() + heartbeat_idle);
                },
                Some(Ok(_)) => { heartbeat.as_mut().reset(tokio::time::Instant::now() + heartbeat_idle); },
                _ => { fail_pending(&mut pending); return SessionResult::Disconnected; }
            },
            () = &mut heartbeat, if heartbeat_enabled => {
                if awaiting_pong || writer.send(Message::Ping(Vec::new().into())).await.is_err() {
                    fail_pending(&mut pending);
                    return SessionResult::Disconnected;
                }
                awaiting_pong = true;
                heartbeat.as_mut().reset(tokio::time::Instant::now() + heartbeat_timeout);
            },
        }
    }
}

async fn write_session(session: &mut Session, message_type: u16, payload: Vec<u8>) -> Result<()> {
    match session {
        Session::Tcp(stream) => write_tcp(stream, message_type, payload).await,
        Session::WebSocket(socket) => {
            let frame = try_encode_message_frame(message_type, &payload)?;
            socket
                .send(Message::Binary(frame.into()))
                .await
                .map_err(|e| FitzError::Transport(e.to_string()))
        }
    }
}

async fn restore_registrations(supervisor: &Supervisor, session: &mut Session) -> Result<()> {
    let registrations = supervisor
        .registrations
        .lock()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    for registration in registrations {
        let response = round_trip_session(
            supervisor,
            session,
            registration.message_type,
            registration.payload.clone(),
        )
        .await?;
        let wire_id = (registration.decoder)(&response)?;
        registration.wire_id.store(wire_id, Ordering::Release);
        tracing::info!(
            message_type = registration.message_type,
            wire_id,
            "fitz registration restored"
        );
    }
    Ok(())
}

async fn round_trip_session(
    supervisor: &Supervisor,
    session: &mut Session,
    expected_message_type: u16,
    payload: Vec<u8>,
) -> Result<Vec<u8>> {
    write_session(session, expected_message_type, payload).await?;
    loop {
        let (message_type, payload) =
            tokio::time::timeout(supervisor.timeout, read_session(session))
                .await
                .map_err(|_| FitzError::Timeout)??;
        if message_type == expected_message_type {
            return Ok(payload);
        }
        if let Some(sender) = supervisor.notifications.lock().get(&message_type) {
            let _ = sender.send(payload);
        }
    }
}

async fn read_session(session: &mut Session) -> Result<(u16, Vec<u8>)> {
    match session {
        Session::Tcp(stream) => read_tcp(stream).await,
        Session::WebSocket(socket) => loop {
            match socket.next().await {
                Some(Ok(Message::Binary(frame))) => {
                    let (message_type, payload_start) = decode_message_frame(&frame)?;
                    return Ok((message_type, frame[payload_start..].to_vec()));
                }
                Some(Ok(_)) => {}
                Some(Err(error)) => return Err(FitzError::Transport(error.to_string())),
                None => return Err(FitzError::ConnectionClosed),
            }
        },
    }
}

async fn write_tcp<W: AsyncWrite + Unpin>(
    writer: &mut W,
    message_type: u16,
    payload: Vec<u8>,
) -> Result<()> {
    let frame = try_encode_message_frame(message_type, &payload)?;
    let len = u32::try_from(frame.len()).map_err(|_| FitzError::FrameTooLarge(frame.len()))?;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&frame).await?;
    writer.flush().await?;
    Ok(())
}

async fn read_tcp<R: AsyncRead + Unpin>(reader: &mut R) -> Result<(u16, Vec<u8>)> {
    let len = reader.read_u32().await? as usize;
    let mut frame = vec![0_u8; len];
    reader.read_exact(&mut frame).await?;
    let (message_type, payload_start) = decode_message_frame(&frame)?;
    Ok((message_type, frame[payload_start..].to_vec()))
}

fn dispatch(
    supervisor: &Supervisor,
    pending: &mut HashMap<u16, VecDeque<PendingRequest>>,
    message_type: u16,
    payload: Vec<u8>,
) {
    if let Some(slot) = pending.get_mut(&message_type).and_then(VecDeque::pop_front) {
        if let Some(response) = slot.response {
            let _ = response.send(Ok(payload));
        }
        return;
    }
    if let Some(sender) = supervisor.notifications.lock().get(&message_type) {
        let _ = sender.send(payload);
    }
}

fn cancel_pending(pending: &mut HashMap<u16, VecDeque<PendingRequest>>, id: u64) -> bool {
    for queue in pending.values_mut() {
        if let Some(slot) = queue.iter_mut().find(|slot| slot.id == id) {
            slot.response = None;
            return true;
        }
    }
    false
}

fn fail_pending(pending: &mut HashMap<u16, VecDeque<PendingRequest>>) {
    for slot in pending.values_mut().flat_map(|queue| queue.drain(..)) {
        if let Some(response) = slot.response {
            let _ = response.send(Err(FitzError::ConnectionClosed));
        }
    }
    pending.clear();
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn should_discard_canceled_fifo_slot_given_late_response() {
        // Arrange
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (mut reader, mut writer) = stream.into_split();
            let _connect = read_tcp(&mut reader).await.unwrap();
            let _first = read_tcp(&mut reader).await.unwrap();
            let _second = read_tcp(&mut reader).await.unwrap();
            write_tcp(&mut writer, 100, b"late".to_vec()).await.unwrap();
            write_tcp(&mut writer, 100, b"second".to_vec())
                .await
                .unwrap();
        });
        let (state, _) = watch::channel(ConnectionState::Disconnected);
        let connection = AsyncConnection::spawn(AsyncConnectionOptions {
            endpoint: format!("tcp://{address}"),
            token_provider: Arc::new(|| async { Ok(String::new()) }),
            timeout: Duration::from_secs(1),
            max_queued: 8,
            reconnect: ReconnectPolicy {
                enabled: false,
                ..ReconnectPolicy::default()
            },
            retry: RetryPolicy::default(),
            heartbeat: HeartbeatOptions::default(),
            observability: FitzObservability::default(),
            state,
        });
        connection.connect().await.unwrap();

        // Act
        let first = tokio::spawn({
            let connection = connection.clone();
            async move { connection.request(100, b"first".to_vec()).await }
        });
        tokio::task::yield_now().await;
        first.abort();
        let response = connection.request(100, b"second".to_vec()).await.unwrap();

        // Assert
        assert_eq!(response, b"second");
        server.await.unwrap();
    }
}
