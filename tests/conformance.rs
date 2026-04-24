use cntryl::domains::stream::StreamCommitMode;
use cntryl::protocol::TransactionMode;
use cntryl::{FitzClient, FitzError, FitzErrorKind, Result};
use serde::Serialize;
use std::fs;
use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Barrier};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const CLIENT_NAME: &str = "fitz-rs";
const REALM: &str = "test-realm";
const DEFAULT_SECRET: &str = "test-secret-key";
static ROUTE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug)]
enum Transport {
    Tcp,
    WebSocket,
}

impl Transport {
    fn as_str(self) -> &'static str {
        match self {
            Transport::Tcp => "tcp",
            Transport::WebSocket => "ws",
        }
    }

    fn from_env(value: &str) -> Self {
        match value {
            "ws" | "websocket" => Transport::WebSocket,
            _ => Transport::Tcp,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AuthMode {
    Anonymous,
    ValidJwt,
}

impl AuthMode {
    fn as_str(self) -> &'static str {
        match self {
            AuthMode::Anonymous => "anonymous",
            AuthMode::ValidJwt => "valid_jwt",
        }
    }

    fn from_env(value: &str) -> Self {
        match value {
            "valid_jwt" => AuthMode::ValidJwt,
            _ => AuthMode::Anonymous,
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Verdict {
    Pass,
    Partial,
    Fail,
    NotImplemented,
    Unclear,
}

#[derive(Debug)]
struct ScenarioOutcome {
    verdict: Verdict,
    evidence: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct ScenarioResult {
    scenario_id: String,
    title: String,
    priority: String,
    client: String,
    transport: String,
    auth_mode: String,
    verdict: Verdict,
    evidence: Vec<String>,
    latency_ms: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct AggregateResult {
    suite: String,
    version: String,
    generated_at: String,
    client: String,
    transport: String,
    auth_mode: String,
    p0_pass_rate: f64,
    p1_pass_rate: f64,
    overall_status: String,
    scenarios: Vec<ScenarioResult>,
}

struct ResultCollector {
    results: Vec<ScenarioResult>,
}

impl ResultCollector {
    fn new() -> Self {
        Self { results: Vec::new() }
    }

    fn record(&mut self, result: ScenarioResult) {
        self.results.push(result);
    }

    fn aggregate(&self, transport: Transport, auth_mode: AuthMode) -> AggregateResult {
        let p0: Vec<&ScenarioResult> = self.results.iter().filter(|result| result.priority == "P0").collect();
        let p1: Vec<&ScenarioResult> = self.results.iter().filter(|result| result.priority == "P1").collect();

        let rate = |rows: &[&ScenarioResult]| -> f64 {
            if rows.is_empty() {
                return 1.0;
            }

            rows.iter().filter(|result| matches!(result.verdict, Verdict::Pass)).count() as f64
                / rows.len() as f64
        };

        let has_p0_fail = p0.iter().any(|result| matches!(result.verdict, Verdict::Fail));
        let has_any_non_pass = self
            .results
            .iter()
            .any(|result| !matches!(result.verdict, Verdict::Pass));

        let overall_status = if has_p0_fail {
            "fail"
        } else if has_any_non_pass {
            "partial"
        } else {
            "pass"
        };

        AggregateResult {
            suite: "fitz-cross-language-client-conformance".to_string(),
            version: "1.0".to_string(),
            generated_at: format!(
                "{}",
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            ),
            client: CLIENT_NAME.to_string(),
            transport: transport.as_str().to_string(),
            auth_mode: auth_mode.as_str().to_string(),
            p0_pass_rate: rate(&p0),
            p1_pass_rate: rate(&p1),
            overall_status: overall_status.to_string(),
            scenarios: self.results.clone(),
        }
    }
}

#[derive(Debug)]
struct StubServer {
    host: String,
    port: u16,
    join: Option<JoinHandle<()>>,
}

impl StubServer {
    fn tcp_addr(&self) -> (&str, u16) {
        (&self.host, self.port)
    }

    fn ws_url(&self) -> String {
        format!("ws://{}:{}/ws", self.host, self.port)
    }

    fn join(mut self) {
        if let Some(join) = self.join.take() {
            join.join().expect("stub server thread panicked");
        }
    }
}

enum StubBehavior {
    Stall,
    Close,
}

fn main_auth_mode() -> AuthMode {
    AuthMode::from_env(&std::env::var("CONFORMANCE_AUTH_MODE").unwrap_or_else(|_| "anonymous".to_string()))
}

fn main_transport() -> Transport {
    Transport::from_env(&std::env::var("CONFORMANCE_TRANSPORT").unwrap_or_else(|_| "tcp".to_string()))
}

fn main_output_path() -> PathBuf {
    PathBuf::from(
        std::env::var("CONFORMANCE_OUTPUT").unwrap_or_else(|_| "./artifacts/conformance-results.json".to_string()),
    )
}

fn broker_tcp_addr(auth_mode: AuthMode) -> (String, u16) {
    let key = match auth_mode {
        AuthMode::Anonymous => "FITZ_BROKER_ANON_TCP_ADDR",
        AuthMode::ValidJwt => "FITZ_BROKER_AUTH_TCP_ADDR",
    };
    let fallback = match auth_mode {
        AuthMode::Anonymous => "127.0.0.1:4191",
        AuthMode::ValidJwt => "127.0.0.1:4091",
    };
    parse_tcp_addr(&std::env::var(key).unwrap_or_else(|_| fallback.to_string()))
}

fn broker_ws_url(auth_mode: AuthMode) -> String {
    let key = match auth_mode {
        AuthMode::Anonymous => "FITZ_BROKER_ANON_WS_ADDR",
        AuthMode::ValidJwt => "FITZ_BROKER_AUTH_WS_ADDR",
    };
    let fallback = match auth_mode {
        AuthMode::Anonymous => "ws://127.0.0.1:4190/ws",
        AuthMode::ValidJwt => "ws://127.0.0.1:4090/ws",
    };
    std::env::var(key).unwrap_or_else(|_| fallback.to_string())
}

fn broker_secret() -> String {
    std::env::var("FITZ_BROKER_JWT_HMAC_SECRET").unwrap_or_else(|_| DEFAULT_SECRET.to_string())
}

fn parse_tcp_addr(value: &str) -> (String, u16) {
    let (host, port) = value
        .rsplit_once(':')
        .unwrap_or(("127.0.0.1", "0"));
    let port = port.parse().unwrap_or(0);
    (host.to_string(), port)
}

fn unique_route(scheme: &str) -> String {
    let counter = ROUTE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock drift")
        .as_nanos();

    if scheme == "schedule" {
        format!("{scheme}://{REALM}/{nonce}/{counter}/run")
    } else {
        format!("{scheme}://{REALM}/{nonce}/{counter}/res")
    }
}

fn read_length_prefixed_frame(stream: &mut TcpStream) -> Vec<u8> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).expect("failed to read frame length");
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut frame = vec![0u8; len];
    stream.read_exact(&mut frame).expect("failed to read frame body");
    frame
}

fn spawn_stub_server(transport: Transport, behavior: StubBehavior) -> StubServer {
    let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind stub listener");
    let port = listener.local_addr().expect("missing local addr").port();

    let join = match transport {
        Transport::Tcp => thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("failed to accept TCP client");
            let _ = read_length_prefixed_frame(&mut socket);
            let _ = read_length_prefixed_frame(&mut socket);

            match behavior {
                StubBehavior::Stall => thread::sleep(Duration::from_millis(500)),
                StubBehavior::Close => {}
            }
        }),
        Transport::WebSocket => thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().expect("failed to create runtime");
            runtime.block_on(async move {
                use futures_util::StreamExt;

                let listener = tokio::net::TcpListener::from_std(listener)
                    .expect("failed to convert listener");
                let (stream, _) = listener.accept().await.expect("failed to accept websocket client");
                let mut websocket = tokio_tungstenite::accept_async(stream)
                    .await
                    .expect("failed to accept websocket");

                let _ = websocket.next().await.expect("missing connect frame").expect("invalid connect frame");
                let _ = websocket.next().await.expect("missing request frame").expect("invalid request frame");

                match behavior {
                    StubBehavior::Stall => thread::sleep(Duration::from_millis(500)),
                    StubBehavior::Close => {
                        let _ = websocket.close(None).await;
                    }
                }
            });
        }),
    };

    StubServer {
        host: "127.0.0.1".to_string(),
        port,
        join: Some(join),
    }
}

fn connect_broker_client(transport: Transport, auth_mode: AuthMode) -> Result<FitzClient> {
    let secret = broker_secret();

    match (transport, auth_mode) {
        (Transport::Tcp, AuthMode::Anonymous) => {
            let (host, port) = broker_tcp_addr(auth_mode);
            FitzClient::connect_tcp_anonymous(&host, port, REALM)
        }
        (Transport::Tcp, AuthMode::ValidJwt) => {
            let (host, port) = broker_tcp_addr(auth_mode);
            FitzClient::connect_tcp(&host, port, REALM, &secret)
        }
        (Transport::WebSocket, AuthMode::Anonymous) => {
            let url = broker_ws_url(auth_mode);
            FitzClient::connect_ws_anonymous(&url, REALM)
        }
        (Transport::WebSocket, AuthMode::ValidJwt) => {
            let url = broker_ws_url(auth_mode);
            FitzClient::connect_ws(&url, REALM, &secret)
        }
    }
}

fn connect_stub_client(transport: Transport, auth_mode: AuthMode, stub: &StubServer) -> Result<FitzClient> {
    let secret = DEFAULT_SECRET;

    match (transport, auth_mode) {
        (Transport::Tcp, AuthMode::Anonymous) => {
            let (host, port) = stub.tcp_addr();
            FitzClient::connect_tcp_anonymous(host, port, REALM)
        }
        (Transport::Tcp, AuthMode::ValidJwt) => {
            let (host, port) = stub.tcp_addr();
            FitzClient::connect_tcp(host, port, REALM, secret)
        }
        (Transport::WebSocket, AuthMode::Anonymous) => {
            FitzClient::connect_ws_anonymous(&stub.ws_url(), REALM)
        }
        (Transport::WebSocket, AuthMode::ValidJwt) => {
            FitzClient::connect_ws(&stub.ws_url(), REALM, secret)
        }
    }
}

fn connect_stub_client_with_timeout(
    transport: Transport,
    auth_mode: AuthMode,
    stub: &StubServer,
    timeout: Duration,
) -> Result<FitzClient> {
    match (transport, auth_mode) {
        (Transport::Tcp, AuthMode::Anonymous) => {
            let (host, port) = stub.tcp_addr();
            FitzClient::builder_anonymous(REALM)
                .with_timeout(timeout)
                .connect_tcp(host, port)
        }
        (Transport::Tcp, AuthMode::ValidJwt) => {
            let (host, port) = stub.tcp_addr();
            FitzClient::builder(REALM, DEFAULT_SECRET)
                .with_timeout(timeout)
                .connect_tcp(host, port)
        }
        (Transport::WebSocket, AuthMode::Anonymous) => FitzClient::builder_anonymous(REALM)
            .with_timeout(timeout)
            .connect_ws(&stub.ws_url()),
        (Transport::WebSocket, AuthMode::ValidJwt) => FitzClient::builder(REALM, DEFAULT_SECRET)
            .with_timeout(timeout)
            .connect_ws(&stub.ws_url()),
    }
}

fn connect_invalid_auth_client(transport: Transport) -> Result<FitzClient> {
    let secret = "definitely-wrong-secret";

    match transport {
        Transport::Tcp => {
            let (host, port) = broker_tcp_addr(AuthMode::ValidJwt);
            FitzClient::connect_tcp(&host, port, REALM, secret)
        }
        Transport::WebSocket => {
            let url = broker_ws_url(AuthMode::ValidJwt);
            FitzClient::connect_ws(&url, REALM, secret)
        }
    }
}

fn run_scenario<F>(scenario_id: &str, title: &str, priority: &str, transport: Transport, auth_mode: AuthMode, f: F) -> ScenarioResult
where
    F: FnOnce() -> std::result::Result<ScenarioOutcome, String>,
{
    let start = Instant::now();
    let result = catch_unwind(AssertUnwindSafe(f));

    match result {
        Ok(Ok(outcome)) => ScenarioResult {
            scenario_id: scenario_id.to_string(),
            title: title.to_string(),
            priority: priority.to_string(),
            client: CLIENT_NAME.to_string(),
            transport: transport.as_str().to_string(),
            auth_mode: auth_mode.as_str().to_string(),
            verdict: outcome.verdict,
            evidence: outcome.evidence,
            latency_ms: start.elapsed().as_millis(),
            error: None,
        },
        Ok(Err(error)) => ScenarioResult {
            scenario_id: scenario_id.to_string(),
            title: title.to_string(),
            priority: priority.to_string(),
            client: CLIENT_NAME.to_string(),
            transport: transport.as_str().to_string(),
            auth_mode: auth_mode.as_str().to_string(),
            verdict: Verdict::Fail,
            evidence: Vec::new(),
            latency_ms: start.elapsed().as_millis(),
            error: Some(error),
        },
        Err(payload) => ScenarioResult {
            scenario_id: scenario_id.to_string(),
            title: title.to_string(),
            priority: priority.to_string(),
            client: CLIENT_NAME.to_string(),
            transport: transport.as_str().to_string(),
            auth_mode: auth_mode.as_str().to_string(),
            verdict: Verdict::Fail,
            evidence: Vec::new(),
            latency_ms: start.elapsed().as_millis(),
            error: Some(match payload.downcast_ref::<&str>() {
                Some(message) => (*message).to_string(),
                None => match payload.downcast_ref::<String>() {
                    Some(message) => message.clone(),
                    None => "scenario panicked".to_string(),
                },
            }),
        },
    }
}

fn close_client(client: &FitzClient) {
    let _ = client.close();
}

fn audit_error(err: &FitzError) -> String {
    format!("{:?}:{err}", err.kind())
}

fn execute_suite(transport: Transport, auth_mode: AuthMode) -> AggregateResult {
    let mut collector = ResultCollector::new();

    collector.record(run_scenario("CS-001", "connect success", "P0", transport, auth_mode, || {
        let client = connect_broker_client(transport, auth_mode).map_err(|err| format!("connect failed: {err}"))?;
        let mut evidence = vec!["connect returned successfully".to_string()];

        let route = unique_route("kv");
        let tx = client
            .kv()
            .begin(&route, TransactionMode::ReadWrite)
            .map_err(|err| format!("kv begin failed: {err}"))?;
        tx.put(b"cs001-key", b"cs001-value")
            .map_err(|err| format!("kv put failed: {err}"))?;
        let value = tx
            .get(b"cs001-key")
            .map_err(|err| format!("kv get failed: {err}"))?
            .ok_or_else(|| "kv get returned no value".to_string())?;
        if value != b"cs001-value" {
            return Err("kv round trip mismatch".to_string());
        }
        tx.commit().map_err(|err| format!("kv commit failed: {err}"))?;
        evidence.push("first domain request (kv) succeeded".to_string());
        close_client(&client);

        Ok(ScenarioOutcome {
            verdict: Verdict::Pass,
            evidence,
        })
    }));

    collector.record(run_scenario("CS-002", "auth failure", "P0", transport, auth_mode, || {
        let mut evidence = Vec::new();
        match connect_invalid_auth_client(transport) {
            Err(err) => {
                evidence.push(format!("connect failed as expected: {} ({})", err, audit_error(&err)));
                Ok(ScenarioOutcome {
                    verdict: Verdict::Pass,
                    evidence,
                })
            }
            Ok(client) => {
                let route = unique_route("kv");
                let result = client.kv().begin(&route, TransactionMode::ReadWrite);
                close_client(&client);

                match result {
                    Err(err) if err.is_auth_failure() || matches!(err.kind(), FitzErrorKind::ConnectionClosed) => {
                        evidence.push(format!("request failed after auth rejection: {} ({})", err, audit_error(&err)));
                        Ok(ScenarioOutcome { verdict: Verdict::Pass, evidence })
                    }
                    Err(err) => {
                        evidence.push(format!("request failed with a weaker but non-successful error: {} ({})", err, audit_error(&err)));
                        Ok(ScenarioOutcome { verdict: Verdict::Partial, evidence })
                    }
                    Ok(_) => Ok(ScenarioOutcome {
                        verdict: Verdict::Partial,
                        evidence: vec!["invalid credentials did not fail the first request".to_string()],
                    }),
                }
            }
        }
    }));

    collector.record(run_scenario("CS-003", "request success", "P0", transport, auth_mode, || {
        let client = connect_broker_client(transport, auth_mode).map_err(|err| format!("connect failed: {err}"))?;
        let route = unique_route("kv");
        let tx = client
            .kv()
            .begin(&route, TransactionMode::ReadWrite)
            .map_err(|err| format!("kv begin failed: {err}"))?;
        tx.put(b"cs003-key", b"cs003-value")
            .map_err(|err| format!("kv put failed: {err}"))?;
        let value = tx
            .get(b"cs003-key")
            .map_err(|err| format!("kv get failed: {err}"))?
            .ok_or_else(|| "kv get returned no value".to_string())?;
        if value != b"cs003-value" {
            return Err("kv round trip mismatch".to_string());
        }
        tx.commit().map_err(|err| format!("kv commit failed: {err}"))?;
        close_client(&client);

        Ok(ScenarioOutcome {
            verdict: Verdict::Pass,
            evidence: vec!["read-after-write succeeded".to_string()],
        })
    }));

    collector.record(run_scenario("CS-004", "unknown route", "P0", transport, auth_mode, || {
        let client = connect_broker_client(transport, auth_mode).map_err(|err| format!("connect failed: {err}"))?;
        let route = unique_route("rpc");
        let result = client.rpc().call(&route, b"ping");
        close_client(&client);

        match result {
            Err(err) if matches!(err.kind(), FitzErrorKind::Domain | FitzErrorKind::Protocol) => Ok(ScenarioOutcome {
                verdict: Verdict::Pass,
                evidence: vec![format!("unknown route rejected: {} ({})", err, audit_error(&err))],
            }),
            Err(err) => Ok(ScenarioOutcome {
                verdict: Verdict::Partial,
                evidence: vec![format!("unknown route produced a different error: {} ({})", err, audit_error(&err))],
            }),
            Ok(_) => Ok(ScenarioOutcome {
                verdict: Verdict::Partial,
                evidence: vec!["rpc call unexpectedly succeeded for an unbound route".to_string()],
            }),
        }
    }));

    collector.record(run_scenario("CS-005", "invalid payload", "P0", transport, auth_mode, || {
        let client = connect_broker_client(transport, auth_mode).map_err(|err| format!("connect failed: {err}"))?;
        let route = unique_route("schedule");
        let result = client.schedule().create(&route, "not-a-cron", b"payload");
        close_client(&client);

        match result {
            Err(err) if matches!(err.kind(), FitzErrorKind::Domain | FitzErrorKind::Protocol) => Ok(ScenarioOutcome {
                verdict: Verdict::Pass,
                evidence: vec![format!("invalid payload rejected: {} ({})", err, audit_error(&err))],
            }),
            Err(err) => Ok(ScenarioOutcome {
                verdict: Verdict::Partial,
                evidence: vec![format!("invalid payload produced a different error: {} ({})", err, audit_error(&err))],
            }),
            Ok(_) => Ok(ScenarioOutcome {
                verdict: Verdict::Partial,
                evidence: vec!["schedule create unexpectedly accepted an invalid cron".to_string()],
            }),
        }
    }));

    collector.record(run_scenario("CS-006", "server error mapping", "P0", transport, auth_mode, || {
        let client = connect_broker_client(transport, auth_mode).map_err(|err| format!("connect failed: {err}"))?;
        let route = unique_route("lease");
        let result = client.lease().release(&route, "node-1", 42);
        close_client(&client);

        match result {
            Err(err) if matches!(err.kind(), FitzErrorKind::Domain | FitzErrorKind::Protocol) => Ok(ScenarioOutcome {
                verdict: Verdict::Pass,
                evidence: vec![format!("server error mapped through typed classification: {} ({})", err, audit_error(&err))],
            }),
            Err(err) => Ok(ScenarioOutcome {
                verdict: Verdict::Partial,
                evidence: vec![format!("server error mapped but classification was weaker: {} ({})", err, audit_error(&err))],
            }),
            Ok(_) => Ok(ScenarioOutcome {
                verdict: Verdict::Partial,
                evidence: vec!["release unexpectedly succeeded for an unknown lease".to_string()],
            }),
        }
    }));

    collector.record(run_scenario("CS-007", "timeout handling", "P0", transport, auth_mode, || {
        let stub = spawn_stub_server(transport, StubBehavior::Stall);
        let client = match connect_stub_client_with_timeout(transport, auth_mode, &stub, Duration::from_millis(50)) {
            Ok(client) => client,
            Err(err) => {
                stub.join();
                return Err(format!("timeout scenario connect failed: {err}"));
            }
        };

        let route = unique_route("kv");
        let result = client.kv().begin(&route, TransactionMode::ReadWrite);
        close_client(&client);
        stub.join();

        match result {
            Err(err) if matches!(err.kind(), FitzErrorKind::Timeout) => Ok(ScenarioOutcome {
                verdict: Verdict::Pass,
                evidence: vec![format!("request timed out as expected: {} ({})", err, audit_error(&err))],
            }),
            Err(err) => Ok(ScenarioOutcome {
                verdict: Verdict::Partial,
                evidence: vec![format!("request failed with a different error: {} ({})", err, audit_error(&err))],
            }),
            Ok(_) => Ok(ScenarioOutcome {
                verdict: Verdict::Partial,
                evidence: vec!["timeout scenario unexpectedly completed".to_string()],
            }),
        }
    }));

    collector.record(run_scenario("CS-008", "caller cancellation", "P0", transport, auth_mode, || {
        Ok(ScenarioOutcome {
            verdict: Verdict::NotImplemented,
            evidence: vec![
                "the blocking Rust API currently exposes close() and timeouts rather than per-call cancellation tokens".to_string(),
                "caller cancellation is therefore represented as a documented gap instead of a simulated async abort".to_string(),
            ],
        })
    }));

    collector.record(run_scenario("CS-009", "disconnect during request", "P1", transport, auth_mode, || {
        let stub = spawn_stub_server(transport, StubBehavior::Close);
        let client = match connect_stub_client(transport, auth_mode, &stub) {
            Ok(client) => client,
            Err(err) => {
                stub.join();
                return Err(format!("connect to stub failed: {err}"));
            }
        };

        let route = unique_route("kv");
        let result = client.kv().begin(&route, TransactionMode::ReadWrite);
        close_client(&client);
        stub.join();

        match result {
            Err(err) if matches!(err.kind(), FitzErrorKind::ConnectionClosed) => Ok(ScenarioOutcome {
                verdict: Verdict::Pass,
                evidence: vec![format!("request observed connection close: {} ({})", err, audit_error(&err))],
            }),
            Err(err) => Ok(ScenarioOutcome {
                verdict: Verdict::Partial,
                evidence: vec![format!("disconnect surfaced differently: {} ({})", err, audit_error(&err))],
            }),
            Ok(_) => Ok(ScenarioOutcome {
                verdict: Verdict::Partial,
                evidence: vec!["request unexpectedly completed after server disconnect".to_string()],
            }),
        }
    }));

    collector.record(run_scenario("CS-010", "reconnect and retry behavior", "P1", transport, auth_mode, || {
        Ok(ScenarioOutcome {
            verdict: Verdict::NotImplemented,
            evidence: vec![
                "this client is intentionally synchronous and single-connection; reconnect orchestration is not yet part of the Rust surface".to_string(),
                "state restoration for subscriptions/workers is therefore out of scope for the current implementation".to_string(),
            ],
        })
    }));

    collector.record(run_scenario("CS-011", "stream receive sequence", "P1", transport, auth_mode, || {
        let client = connect_broker_client(transport, auth_mode).map_err(|err| format!("connect failed: {err}"))?;
        let route = unique_route("stream");
        let (ready_tx, ready_rx) = mpsc::channel();
        let subscriber_client = client.stream();
        let subscription = subscriber_client
            .subscribe(&route)
            .map_err(|err| format!("stream subscribe failed: {err}"))?;
        let notification_route = route.clone();

        let listener = thread::spawn(move || {
            ready_tx.send(()).expect("failed to signal listener readiness");
            let notification = subscription.next().expect("failed to receive stream notification");
            subscription.unsubscribe().expect("failed to unsubscribe stream subscription");
            notification
        });

        ready_rx.recv().expect("listener did not become ready");
        let mut session = client
            .stream()
            .begin(&route, None)
            .map_err(|err| format!("stream begin failed: {err}"))?;
        session
            .append(0, b"record-1", None)
            .map_err(|err| format!("append 1 failed: {err}"))?;
        session
            .append(1, b"record-2", None)
            .map_err(|err| format!("append 2 failed: {err}"))?;
        session
            .commit(StreamCommitMode::Sync)
            .map_err(|err| format!("stream commit failed: {err}"))?;

        let notification = listener.join().map_err(|_| "listener thread panicked".to_string())?;
        close_client(&client);

        if notification.route != notification_route {
            return Err("stream notification route mismatch".to_string());
        }

        Ok(ScenarioOutcome {
            verdict: Verdict::Pass,
            evidence: vec![format!("received stream notification for {notification_route}")],
        })
    }));

    collector.record(run_scenario("CS-012", "stream completion", "P1", transport, auth_mode, || {
        let client = connect_broker_client(transport, auth_mode).map_err(|err| format!("connect failed: {err}"))?;
        let route = unique_route("stream");
        let mut session = client
            .stream()
            .begin(&route, None)
            .map_err(|err| format!("stream begin failed: {err}"))?;
        let first_offset = session
            .append(0, b"record-1", None)
            .map_err(|err| format!("append 1 failed: {err}"))?
            .ok_or_else(|| "missing first offset".to_string())?;
        let second_offset = session
            .append(first_offset + 1, b"record-2", None)
            .map_err(|err| format!("append 2 failed: {err}"))?
            .ok_or_else(|| "missing second offset".to_string())?;
        if second_offset < first_offset {
            return Err("stream offsets regressed".to_string());
        }

        session
            .commit(StreamCommitMode::Sync)
            .map_err(|err| format!("stream commit failed: {err}"))?;

        let records = client
            .stream()
            .read(&route, 0, 10, None)
            .map_err(|err| format!("stream read failed: {err}"))?;
        let last = client
            .stream()
            .peek(&route)
            .map_err(|err| format!("stream peek failed: {err}"))?
            .ok_or_else(|| "missing last record".to_string())?;
        let metadata = client
            .stream()
            .metadata(&route)
            .map_err(|err| format!("stream metadata failed: {err}"))?;
        close_client(&client);

        if records.len() != 2 {
            return Err("stream read did not return both records".to_string());
        }
        if last.body != b"record-2" {
            return Err("stream last record mismatch".to_string());
        }
        if metadata.record_count < 2 {
            return Err("stream metadata record count too small".to_string());
        }

        Ok(ScenarioOutcome {
            verdict: Verdict::Pass,
            evidence: vec!["stream commit, read, peek, and metadata all succeeded".to_string()],
        })
    }));

    collector.record(run_scenario("CS-013", "stream error mid-flight", "P1", transport, auth_mode, || {
        let client = connect_broker_client(transport, auth_mode).map_err(|err| format!("connect failed: {err}"))?;
        let route = unique_route("stream");
        let mut session = client
            .stream()
            .begin(&route, None)
            .map_err(|err| format!("stream begin failed: {err}"))?;
        session
            .append(0, b"record-1", None)
            .map_err(|err| format!("append 1 failed: {err}"))?;
        let result = session.append(99, b"record-2", None);
        close_client(&client);

        match result {
            Err(err) if matches!(err.kind(), FitzErrorKind::Domain | FitzErrorKind::Protocol) => Ok(ScenarioOutcome {
                verdict: Verdict::Pass,
                evidence: vec![format!("stream mid-flight error mapped correctly: {} ({})", err, audit_error(&err))],
            }),
            Err(err) => Ok(ScenarioOutcome {
                verdict: Verdict::Partial,
                evidence: vec![format!("stream mid-flight error mapped differently: {} ({})", err, audit_error(&err))],
            }),
            Ok(_) => Ok(ScenarioOutcome {
                verdict: Verdict::Partial,
                evidence: vec!["stream append unexpectedly succeeded with a stale offset".to_string()],
            }),
        }
    }));

    collector.record(run_scenario("CS-014", "concurrent requests", "P1", transport, auth_mode, || {
        let client = Arc::new(connect_broker_client(transport, auth_mode).map_err(|err| format!("connect failed: {err}"))?);
        let barrier = Arc::new(Barrier::new(3));

        let left_client = Arc::clone(&client);
        let left_barrier = Arc::clone(&barrier);
        let left = thread::spawn(move || -> std::result::Result<(), String> {
            left_barrier.wait();
            let route = unique_route("kv");
            let tx = left_client
                .kv()
                .begin(&route, TransactionMode::ReadWrite)
                .map_err(|err| format!("left begin failed: {err}"))?;
            tx.put(b"left-key", b"left-value")
                .map_err(|err| format!("left put failed: {err}"))?;
            tx.commit().map_err(|err| format!("left commit failed: {err}"))?;
            Ok(())
        });

        let right_client = Arc::clone(&client);
        let right_barrier = Arc::clone(&barrier);
        let right = thread::spawn(move || -> std::result::Result<(), String> {
            right_barrier.wait();
            let route = unique_route("kv");
            let tx = right_client
                .kv()
                .begin(&route, TransactionMode::ReadWrite)
                .map_err(|err| format!("right begin failed: {err}"))?;
            tx.put(b"right-key", b"right-value")
                .map_err(|err| format!("right put failed: {err}"))?;
            tx.commit().map_err(|err| format!("right commit failed: {err}"))?;
            Ok(())
        });

        barrier.wait();
        left.join().map_err(|_| "left request thread panicked".to_string())??;
        right.join().map_err(|_| "right request thread panicked".to_string())??;
        close_client(&client);

        Ok(ScenarioOutcome {
            verdict: Verdict::Pass,
            evidence: vec!["two concurrent requests completed safely on one client".to_string()],
        })
    }));

    collector.record(run_scenario("CS-015", "shutdown during active work", "P1", transport, auth_mode, || {
        let stub = spawn_stub_server(transport, StubBehavior::Stall);
        let client = match connect_stub_client(transport, auth_mode, &stub) {
            Ok(client) => client,
            Err(err) => {
                stub.join();
                return Err(format!("connect to stub failed: {err}"));
            }
        };

        let request_client = Arc::new(client);
        let request_route = unique_route("kv");
        let worker_client = Arc::clone(&request_client);
        let worker = thread::spawn(move || worker_client.kv().begin(&request_route, TransactionMode::ReadWrite));

        thread::sleep(Duration::from_millis(50));
        close_client(&request_client);
        let result = worker.join().map_err(|_| "request thread panicked".to_string())?;
        stub.join();

        match result {
            Err(err) if matches!(err.kind(), FitzErrorKind::ConnectionClosed) => Ok(ScenarioOutcome {
                verdict: Verdict::Pass,
                evidence: vec![format!("shutdown interrupted the active request: {} ({})", err, audit_error(&err))],
            }),
            Err(err) => Ok(ScenarioOutcome {
                verdict: Verdict::Partial,
                evidence: vec![format!("shutdown produced a different error: {} ({})", err, audit_error(&err))],
            }),
            Ok(_) => Ok(ScenarioOutcome {
                verdict: Verdict::Partial,
                evidence: vec!["active request completed before shutdown could interrupt it".to_string()],
            }),
        }
    }));

    collector.aggregate(transport, auth_mode)
}

fn write_results(result: &AggregateResult) -> PathBuf {
    let path = main_output_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("failed to create conformance output directory");
    }
    fs::write(&path, serde_json::to_vec_pretty(result).expect("failed to serialize conformance result"))
        .expect("failed to write conformance output");
    path
}

#[test]
#[ignore]
fn conformance_suite() {
    let transport = main_transport();
    let auth_mode = main_auth_mode();
    let result = execute_suite(transport, auth_mode);
    let output_path = write_results(&result);

    assert_ne!(result.overall_status, "fail", "conformance recorded a failing P0 scenario; see {}", output_path.display());
}

#[test]
fn conformance_result_schema_is_serializable() {
    let aggregate = AggregateResult {
        suite: "fitz-cross-language-client-conformance".to_string(),
        version: "1.0".to_string(),
        generated_at: "0".to_string(),
        client: CLIENT_NAME.to_string(),
        transport: "tcp".to_string(),
        auth_mode: "anonymous".to_string(),
        p0_pass_rate: 1.0,
        p1_pass_rate: 1.0,
        overall_status: "pass".to_string(),
        scenarios: vec![ScenarioResult {
            scenario_id: "CS-001".to_string(),
            title: "connect success".to_string(),
            priority: "P0".to_string(),
            client: CLIENT_NAME.to_string(),
            transport: "tcp".to_string(),
            auth_mode: "anonymous".to_string(),
            verdict: Verdict::Pass,
            evidence: vec!["example".to_string()],
            latency_ms: 1,
            error: None,
        }],
    };

    let serialized = serde_json::to_string(&aggregate).expect("aggregate should serialize");
    assert!(serialized.contains("fitz-cross-language-client-conformance"));
}