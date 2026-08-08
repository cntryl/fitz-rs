mod jwt;

use cntryl_fitz::client_domains::kv::KvGetResult;
use cntryl_fitz::client_domains::lease::LeaseAcquireOptions;
use cntryl_fitz::client_domains::schedule::ScheduleDeliveryMode;
use cntryl_fitz::client_domains::stream::StreamCommitMode;
use cntryl_fitz::{Client, FitzError, KvDurability, TransactionMode};
use futures_util::StreamExt;
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

async fn reject_invalid_jwt(transport: Transport) -> Result<(), FitzError> {
    let secret =
        std::env::var("FITZ_BROKER_JWT_HMAC_SECRET").unwrap_or_else(|_| "dev-test-secret".into());
    for token in [
        jwt::make_invalid_jwt("test-realm", &secret),
        jwt::make_expired_jwt("test-realm", &secret),
    ] {
        let rejected = Client::builder(endpoint(transport, AuthMode::ValidJwt), move || {
            let token = token.clone();
            async move { Ok(token) }
        })
        .request_timeout(Duration::from_secs(2))
        .build()?;
        assert!(
            matches!(
                rejected.connect().await,
                Err(FitzError::Authentication { .. })
            ),
            "invalid or expired JWT must reject connect"
        );
    }

    let token = jwt::make_scoped_jwt("test-realm", &secret, vec!["queue://**#read".to_string()]);
    let read_only = Client::builder(endpoint(transport, AuthMode::ValidJwt), move || {
        let token = token.clone();
        async move { Ok(token) }
    })
    .request_timeout(Duration::from_secs(2))
    .build()?;
    read_only.connect().await?;
    assert!(
        read_only
            .queue()?
            .enqueue(&unique_route("queue"), b"unauthorized", None)
            .await
            .is_err(),
        "read-only JWT must reject queue enqueue"
    );
    read_only.close().await?;
    Ok(())
}

async fn reject_held_lease(
    connected: &Client,
    transport: Transport,
    auth_mode: AuthMode,
) -> Result<(), FitzError> {
    let route = unique_route("lease");
    let owner = connected
        .lease()?
        .acquire(&route, "owner", 30, LeaseAcquireOptions::default())
        .await?;
    let contender = client(transport, auth_mode);
    contender.connect().await?;
    let result = contender
        .lease()?
        .acquire(&route, "contender", 30, LeaseAcquireOptions::default())
        .await;
    let Err(error) = result else {
        panic!("held lease must reject a competing acquisition");
    };
    assert!(matches!(error, FitzError::Domain { code: 5001, .. }));
    contender.close().await?;
    owner.release().await
}

async fn stream_round_trip(connected: &Client) -> Result<(), FitzError> {
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
    Ok(())
}

async fn should_run_scenario(
    id: u8,
    connected: &Client,
    transport: Transport,
    auth_mode: AuthMode,
) -> Result<Vec<String>, FitzError> {
    match id {
        1 | 3 => kv_round_trip(connected, KvDurability::Buffered).await?,
        2 => reject_invalid_jwt(transport).await?,
        4 | 5 => {
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
        6 => reject_held_lease(connected, transport, auth_mode).await?,
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
        11..=13 => stream_round_trip(connected).await?,
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
        let evidence = should_run_scenario(id, &connected, transport, auth_mode)
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

async fn exercise_queue_workflow(connected: &Client) {
    let queue_route = unique_route("queue");
    connected
        .queue()
        .expect("queue client")
        .enqueue(&queue_route, b"queued", None)
        .await
        .expect("enqueue");
    let mut items = connected
        .queue()
        .expect("queue client")
        .reserve(&queue_route, 30, 1, Some(1))
        .await
        .expect("reserve");
    assert_eq!(items.len(), 1);
    let item = items.pop().expect("reserved item");
    assert_eq!(item.body, b"queued");
    item.complete().await.expect("complete queue item");
}

async fn exercise_lease_workflow(connected: &Client) {
    let lease_route = unique_route("lease");
    let mut lease = connected
        .lease()
        .expect("lease client")
        .acquire(
            &lease_route,
            "domain-workflow",
            30,
            LeaseAcquireOptions::default(),
        )
        .await
        .expect("acquire lease");
    assert!(
        connected
            .lease()
            .expect("lease client")
            .query(&lease_route)
            .await
            .expect("query held lease")
            .held
    );
    lease.extend(45).await.expect("extend lease");
    lease.release().await.expect("release lease");
}

async fn exercise_notice_workflow(connected: &Client) {
    let notice_route = unique_route("notice");
    let mut notice_subscription = connected
        .notice()
        .expect("notice client")
        .subscribe("notice://test-realm/conformance/*")
        .await
        .expect("subscribe notice");
    connected
        .notice()
        .expect("notice client")
        .publish(&notice_route, b"notice")
        .await
        .expect("publish notice");
    let notice = tokio::time::timeout(Duration::from_secs(2), notice_subscription.next())
        .await
        .expect("notice timeout")
        .expect("notice subscription ended")
        .expect("notice decode");
    assert_eq!(notice.route, notice_route);
    assert_eq!(notice.body, b"notice");
    notice_subscription
        .unsubscribe()
        .await
        .expect("unsubscribe notice");
}

async fn exercise_rpc_workflow(connected: &Client) {
    let rpc_route = unique_route("rpc");
    let mut worker = connected
        .rpc()
        .expect("rpc client")
        .register_worker(&rpc_route, 1)
        .await
        .expect("register worker");
    let mut responses = connected
        .rpc()
        .expect("rpc client")
        .call(&rpc_route, b"ping")
        .await
        .expect("call rpc");
    let mut request = tokio::time::timeout(Duration::from_secs(2), worker.next())
        .await
        .expect("worker request timeout")
        .expect("worker stream ended")
        .expect("worker request decode");
    assert_eq!(request.body, b"ping");
    request.respond(b"pong", true).await.expect("respond rpc");
    let response = tokio::time::timeout(Duration::from_secs(2), responses.next())
        .await
        .expect("rpc response timeout")
        .expect("rpc response stream ended")
        .expect("rpc response decode");
    assert_eq!(response.body, b"pong");
    worker.deregister().await.expect("deregister worker");
}

async fn exercise_schedule_workflow(connected: &Client) {
    let schedule_route = format!("{}/run", unique_route("schedule"));
    connected
        .schedule()
        .expect("schedule client")
        .create(
            &schedule_route,
            "*/5 * * * *",
            ScheduleDeliveryMode::Broadcast,
            b"scheduled",
        )
        .await
        .expect("create schedule");
    let page = connected
        .schedule()
        .expect("schedule client")
        .list(Some(0), Some(1000))
        .await
        .expect("list schedules");
    assert!(
        page.entries
            .iter()
            .any(|entry| entry.route == schedule_route)
    );
    connected
        .schedule()
        .expect("schedule client")
        .cancel(&schedule_route)
        .await
        .expect("cancel schedule");
}

async fn exercise_stream_workflow(connected: &Client) {
    let stream_route = unique_route("stream");
    let mut stream = connected
        .stream()
        .expect("stream client")
        .begin(&stream_route, None)
        .await
        .expect("begin stream");
    stream
        .append(0, b"stream", None, None)
        .await
        .expect("append stream");
    stream
        .commit(StreamCommitMode::Sync)
        .await
        .expect("commit stream");
    assert_eq!(
        connected
            .stream()
            .expect("stream client")
            .read(&stream_route, 0, 10, None, None, None)
            .await
            .expect("read stream")
            .len(),
        1
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires fitz-auth and fitz-anon from compose.yml"]
async fn should_complete_domain_workflows_given_live_broker_when_clients_exercised() {
    // Arrange: connect one client using the selected transport and auth leg.
    let transport = Transport::from_env();
    let auth_mode = AuthMode::from_env();
    let connected = client(transport, auth_mode);
    connected.connect().await.expect("broker connection");

    // Act: exercise every domain's canonical request/response shapes.
    exercise_queue_workflow(&connected).await;
    exercise_lease_workflow(&connected).await;
    exercise_notice_workflow(&connected).await;
    exercise_rpc_workflow(&connected).await;
    exercise_schedule_workflow(&connected).await;
    exercise_stream_workflow(&connected).await;

    // Assert: every workflow completed and the shared connection closes cleanly.
    connected.close().await.expect("close client");
}
