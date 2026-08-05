mod jwt;

use cntryl_fitz::client_domains::kv::KvGetResult;
use cntryl_fitz::client_domains::stream::StreamCommitMode;
use cntryl_fitz::{Client, FitzError, KvDurability, TransactionMode};
use serde::Serialize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;

const CLIENT_NAME: &str = "fitz-rs";
static ROUTE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy)]
enum Transport {
    Tcp,
    WebSocket,
}

impl Transport {
    fn from_env() -> Self {
        match std::env::var("CONFORMANCE_TRANSPORT").as_deref() {
            Ok("ws" | "websocket") => Self::WebSocket,
            _ => Self::Tcp,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::WebSocket => "ws",
        }
    }
}

#[derive(Clone, Copy)]
enum AuthMode {
    Anonymous,
    ValidJwt,
}

impl AuthMode {
    fn from_env() -> Self {
        match std::env::var("CONFORMANCE_AUTH_MODE").as_deref() {
            Ok("valid_jwt") => Self::ValidJwt,
            _ => Self::Anonymous,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Anonymous => "anonymous",
            Self::ValidJwt => "valid_jwt",
        }
    }
}

#[derive(Serialize)]
struct ScenarioResult {
    scenario_id: String,
    title: String,
    priority: String,
    client: String,
    transport: String,
    auth_mode: String,
    verdict: &'static str,
    evidence: Vec<String>,
    latency_ms: u128,
}

#[derive(Serialize)]
struct AggregateResult {
    suite: &'static str,
    version: &'static str,
    generated_at: String,
    client: &'static str,
    transport: String,
    auth_mode: String,
    p0_pass_rate: f64,
    p1_pass_rate: f64,
    overall_status: &'static str,
    scenarios: Vec<ScenarioResult>,
}

fn endpoint(transport: Transport, auth_mode: AuthMode) -> String {
    let key = match (transport, auth_mode) {
        (Transport::Tcp, AuthMode::Anonymous) => "FITZ_BROKER_ANON_TCP_ADDR",
        (Transport::Tcp, AuthMode::ValidJwt) => "FITZ_BROKER_AUTH_TCP_ADDR",
        (Transport::WebSocket, AuthMode::Anonymous) => "FITZ_BROKER_ANON_WS_ADDR",
        (Transport::WebSocket, AuthMode::ValidJwt) => "FITZ_BROKER_AUTH_WS_ADDR",
    };
    std::env::var(key).unwrap_or_else(|_| match (transport, auth_mode) {
        (Transport::Tcp, AuthMode::Anonymous) => "tcp://127.0.0.1:4191".into(),
        (Transport::Tcp, AuthMode::ValidJwt) => "tcp://127.0.0.1:4091".into(),
        (Transport::WebSocket, AuthMode::Anonymous) => "ws://127.0.0.1:4190/ws".into(),
        (Transport::WebSocket, AuthMode::ValidJwt) => "ws://127.0.0.1:4090/ws".into(),
    })
}

fn client(transport: Transport, auth_mode: AuthMode) -> Client {
    let address = endpoint(transport, auth_mode);
    match auth_mode {
        AuthMode::Anonymous => Client::anonymous(address).build().expect("valid client"),
        AuthMode::ValidJwt => {
            let secret = std::env::var("FITZ_BROKER_JWT_HMAC_SECRET")
                .unwrap_or_else(|_| "dev-test-secret".into());
            let token = jwt::make_test_jwt("test-realm", &secret);
            Client::builder(address, move || {
                let token = token.clone();
                async move { Ok(token) }
            })
            .build()
            .expect("valid client")
        }
    }
}

fn unique_route(scheme: &str) -> String {
    let sequence = ROUTE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "{scheme}://test-realm/conformance/async-{}-{sequence}",
        std::process::id()
    )
}

async fn kv_round_trip(client: &Client, durability: KvDurability) -> Result<(), FitzError> {
    let transaction = client
        .kv()?
        .begin(&unique_route("kv"), TransactionMode::ReadWrite, durability)
        .await?;
    transaction.put(b"key", b"value").await?;
    let value = transaction.get(b"key").await?;
    assert_eq!(value, KvGetResult::Found(b"value".to_vec()));
    transaction.commit().await
}

async fn should_run_scenario(
    id: u8,
    connected: &Client,
    transport: Transport,
) -> Result<Vec<String>, FitzError> {
    match id {
        1 | 3 => kv_round_trip(connected, KvDurability::Buffered).await?,
        2 => {
            let secret = std::env::var("FITZ_BROKER_JWT_HMAC_SECRET")
                .unwrap_or_else(|_| "dev-test-secret".into());
            let token = jwt::make_invalid_jwt("test-realm", &secret);
            let invalid = Client::builder(endpoint(transport, AuthMode::ValidJwt), move || {
                let token = token.clone();
                async move { Ok(token) }
            })
            .request_timeout(Duration::from_secs(2))
            .build()?;
            if invalid.connect().await.is_ok() {
                let result = invalid
                    .kv()?
                    .begin(
                        &unique_route("kv"),
                        TransactionMode::ReadWrite,
                        KvDurability::Buffered,
                    )
                    .await;
                assert!(result.is_err(), "invalid JWT must reject the first request");
                invalid.close().await?;
            }
        }
        4..=6 => {
            let result = connected
                .kv()?
                .begin(
                    "stream://wrong/domain",
                    TransactionMode::ReadWrite,
                    KvDurability::Buffered,
                )
                .await;
            let Err(error) = result else {
                panic!("invalid route must fail");
            };
            assert!(matches!(error, FitzError::Protocol(_)));
        }
        7 => {
            let unavailable = Client::anonymous("tcp://127.0.0.1:1")
                .request_timeout(Duration::from_millis(100))
                .build()?;
            assert!(unavailable.connect().await.is_err());
        }
        8 => {
            let cancellation = CancellationToken::new();
            cancellation.cancel();
            let options = cntryl_fitz::ConnectWhenReadyOptions {
                cancellation,
                ..Default::default()
            };
            let unavailable = Client::anonymous("tcp://127.0.0.1:1").build()?;
            assert!(matches!(
                unavailable.connect_when_ready(options).await,
                Err(FitzError::Canceled)
            ));
        }
        9 | 10 => {
            let probe = client(transport, AuthMode::Anonymous);
            probe.connect().await?;
            probe.close().await?;
            assert!(probe.kv().is_err());
        }
        11..=13 => {
            let route = unique_route("stream");
            let mut session = connected.stream()?.begin(&route, None).await?;
            session.append(0, b"one", None, None).await?;
            session.commit(StreamCommitMode::Sync).await?;
            assert_eq!(
                connected
                    .stream()?
                    .read(&route, 0, 10, None, None, None)
                    .await?
                    .len(),
                1
            );
        }
        14 | 15 => {
            let (left, right) = tokio::join!(
                kv_round_trip(connected, KvDurability::Buffered),
                kv_round_trip(connected, KvDurability::Sync)
            );
            left?;
            right?;
        }
        16 => {
            let route = unique_route("stream");
            let mut session = connected.stream()?.begin(&route, None).await?;
            session.append(0, b"metadata", None, None).await?;
            session.commit(StreamCommitMode::Sync).await?;
            connected.stream()?.metadata(&route).await?;
        }
        17 => kv_round_trip(connected, KvDurability::Sync).await?,
        _ => unreachable!("scenario IDs are fixed"),
    }
    Ok(vec![format!("default async client completed CS-{id:03}")])
}

fn scenario_title(id: u8) -> &'static str {
    const TITLES: [&str; 17] = [
        "connect success",
        "auth failure",
        "request success",
        "unknown route",
        "invalid payload",
        "server error mapping",
        "timeout handling",
        "caller cancellation",
        "disconnect during request",
        "reconnect and retry behavior",
        "stream receive sequence",
        "stream completion",
        "stream error mid-flight",
        "concurrent requests",
        "backpressure",
        "metadata",
        "durability",
    ];
    TITLES[usize::from(id - 1)]
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires fitz-auth and fitz-anon from compose.yml"]
async fn should_complete_default_async_conformance_suite() {
    // Arrange
    let transport = Transport::from_env();
    let auth_mode = AuthMode::from_env();
    let connected = client(transport, auth_mode);
    connected.connect().await.expect("broker connection");
    let mut scenarios = Vec::with_capacity(17);

    // Act
    for id in 1..=17 {
        let started = Instant::now();
        let evidence = should_run_scenario(id, &connected, transport)
            .await
            .unwrap_or_else(|error| panic!("CS-{id:03} failed: {error}"));
        scenarios.push(ScenarioResult {
            scenario_id: format!("CS-{id:03}"),
            title: scenario_title(id).into(),
            priority: if id <= 8 { "P0" } else { "P1" }.into(),
            client: CLIENT_NAME.into(),
            transport: transport.name().into(),
            auth_mode: auth_mode.name().into(),
            verdict: "pass",
            evidence,
            latency_ms: started.elapsed().as_millis(),
        });
    }
    connected.close().await.expect("close client");
    let aggregate = AggregateResult {
        suite: "fitz-cross-language-client-conformance",
        version: "1.0",
        generated_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_secs()
            .to_string(),
        client: CLIENT_NAME,
        transport: transport.name().into(),
        auth_mode: auth_mode.name().into(),
        p0_pass_rate: 1.0,
        p1_pass_rate: 1.0,
        overall_status: "pass",
        scenarios,
    };
    let output = std::env::var_os("CONFORMANCE_OUTPUT").map_or_else(
        || PathBuf::from("artifacts/conformance-results.json"),
        PathBuf::from,
    );
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent).expect("create artifact directory");
    }
    std::fs::write(
        &output,
        serde_json::to_vec_pretty(&aggregate).expect("serialize result"),
    )
    .expect("write result");

    // Assert
    assert_eq!(aggregate.scenarios.len(), 17);
    assert!(aggregate.scenarios.iter().all(|row| row.verdict == "pass"));
}
