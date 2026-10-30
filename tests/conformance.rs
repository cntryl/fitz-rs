#[path = "support/global_replay.rs"]
mod global_replay;
mod jwt;

use cntryl_fitz::client_domains::kv::KvGetResult;
use cntryl_fitz::client_domains::lease::LeaseAcquireOptions;
use cntryl_fitz::client_domains::schedule::{ScheduleDeliveryMode, ScheduleEntry};
use cntryl_fitz::client_domains::stream::{StreamCommitMode, StreamReadItem, StreamReadOptions};
use cntryl_fitz::{Client, FitzError, KvDurability, TransactionMode};
use futures_util::StreamExt;
use serde::Serialize;
use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;

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
    p0_pass: u32,
    p0_total: u32,
    p1_pass: u32,
    p1_total: u32,
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
    let route = unique_route("kv");
    let transaction = client
        .kv()?
        .begin(&route, TransactionMode::ReadWrite)
        .await?;
    transaction.put(b"key", b"value").await?;
    transaction.commit(durability).await?;
    let read = client
        .kv()?
        .begin(&route, TransactionMode::ReadOnly)
        .await?;
    let value = read.get(b"key").await?;
    assert_eq!(value, KvGetResult::Found(b"value".to_vec()));
    read.rollback().await
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
    assert!(matches!(error, FitzError::DomainWire { code: 5050, .. }));
    assert!(
        error.is_retryable(),
        "held lease should be classified as retryable"
    );
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

async fn exercise_timeout_and_reuse(
    connected: &Client,
    transport: Transport,
    auth_mode: AuthMode,
) -> Result<Vec<String>, FitzError> {
    let route = unique_route("rpc");
    let mut worker = connected.rpc()?.register_worker(&route, 1).await?;
    let (accepted_tx, accepted_rx) = oneshot::channel();
    let serving = tokio::spawn(async move {
        let request = worker.next().await.expect("worker stream closed")?;
        accepted_tx
            .send(())
            .expect("caller is waiting for the request");
        request.cancellation.cancelled().await;
        drop(request);
        Ok::<(), FitzError>(())
    });
    let caller = client(transport, auth_mode);
    caller.connect().await?;
    let mut response = caller
        .rpc()?
        .call_with_timeout(&route, b"timeout", Some(Duration::from_millis(75)))
        .await?;
    tokio::time::timeout(Duration::from_secs(2), accepted_rx)
        .await
        .map_err(|_| FitzError::Protocol("CS-007 worker did not receive the stalled RPC".into()))?
        .map_err(|_| FitzError::ConnectionClosed)?;
    assert!(matches!(
        response.next().await,
        Some(Err(FitzError::Timeout))
    ));
    kv_round_trip(&caller, KvDurability::Buffered).await?;
    serving.await.map_err(|_| FitzError::ConnectionClosed)??;
    caller.close().await?;
    Ok(vec![
        "a stalled RPC timed out as Timeout rather than cancellation".into(),
        "a KV transaction succeeded on the same connection after timeout".into(),
    ])
}

async fn exercise_cancellation_and_reuse(
    connected: &Client,
    transport: Transport,
    auth_mode: AuthMode,
) -> Result<Vec<String>, FitzError> {
    let route = unique_route("rpc");
    let mut worker = connected.rpc()?.register_worker(&route, 1).await?;
    let (accepted_tx, accepted_rx) = oneshot::channel();
    let serving = tokio::spawn(async move {
        let canceled = worker.next().await.expect("worker stream closed")?;
        accepted_tx
            .send(())
            .expect("caller is waiting for the request");
        canceled.cancellation.cancelled().await;
        drop(canceled);
        let mut next = worker.next().await.expect("worker stream closed")?;
        next.respond(b"after-cancel", true).await
    });
    let caller = client(transport, auth_mode);
    caller.connect().await?;
    let mut canceled = caller.rpc()?.call(&route, b"cancel").await?;
    tokio::time::timeout(Duration::from_secs(2), accepted_rx)
        .await
        .map_err(|_| FitzError::Protocol("CS-008 worker did not receive the canceled RPC".into()))?
        .map_err(|_| FitzError::ConnectionClosed)?;
    let outcome = canceled.cancel().await;
    assert_eq!(
        outcome,
        cntryl_fitz::client_domains::rpc::RpcCancellationOutcome::Forwarded
    );
    let mut next = caller.rpc()?.call(&route, b"next").await?;
    let result = tokio::time::timeout(Duration::from_secs(2), next.next())
        .await
        .map_err(|_| FitzError::Protocol("CS-008 follow-up RPC did not receive a response".into()))?
        .expect("worker response stream ended without a terminal frame")?;
    assert_eq!(result.body, b"after-cancel");
    serving.await.map_err(|_| FitzError::ConnectionClosed)??;
    caller.close().await?;
    Ok(vec![
        "explicit RPC cancellation was acknowledged as Forwarded, not Timeout".into(),
        "a subsequent request succeeded on the same connection".into(),
    ])
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
        4 => {
            let route = unique_route("rpc");
            let mut response = connected.rpc()?.call(&route, b"unknown route").await?;
            assert!(matches!(
                response.next().await,
                Some(Err(FitzError::DomainWire { code: 6051, .. }))
            ));
        }
        5 => {
            let route = format!("{}/run", unique_route("schedule"));
            let result = connected
                .schedule()?
                .create(&route, "invalid cron", ScheduleDeliveryMode::Single, b"")
                .await;
            assert!(matches!(result, Err(FitzError::DomainWire { .. })));
            kv_round_trip(connected, KvDurability::Buffered).await?;
        }
        6 => reject_held_lease(connected, transport, auth_mode).await?,
        7 => {
            return exercise_timeout_and_reuse(connected, transport, auth_mode).await;
        }
        8 => {
            return exercise_cancellation_and_reuse(connected, transport, auth_mode).await;
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
        17 => return global_replay::exercise(connected, &unique_route("stream")).await,
        19 => {
            exercise_same_type_requests(connected).await;
            return Ok(vec![
                "second reserve resolved first; each caller received its own payload".into(),
            ]);
        }
        20 => {
            let (version, capabilities) = connected.server_capabilities();
            let (max_message_bytes, max_in_flight) = connected.server_limits();
            assert_eq!(version, 2);
            assert_eq!(capabilities, 0);
            assert!(max_message_bytes > 0);
            assert!(max_in_flight > 0);
            return Ok(vec![
                format!("protocol_version={version}"),
                format!("broker_caps={capabilities}"),
                format!("max_message_bytes={max_message_bytes}"),
                format!("max_in_flight={max_in_flight}"),
            ]);
        }
        24 => {
            exercise_stream_boundary(connected).await;
            return Ok(vec![
                "409600 combined bytes accepted; 409601 rejected with BAD_REQUEST/NOT_APPLIED"
                    .into(),
                "three events replayed across multiple pages on a 4096-byte route".into(),
            ]);
        }
        _ => unreachable!("scenario IDs are fixed"),
    }
    Ok(vec![format!("default async client completed CS-{id:03}")])
}

fn scenario_title(id: u8) -> &'static str {
    const TITLES: [&str; 24] = [
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
        "concurrent in-flight requests",
        "shutdown during active work",
        "filtered stream replay",
        "global filtered multi-page replay",
        "bounded concurrency under burst load",
        "concurrent same-message-type requests",
        "handshake negotiation",
        "correlation survives an ingress-synthesized error",
        "notifications are never correlated",
        "one request yields exactly one terminal frame",
        "400 KiB Stream event boundary and replay",
    ];
    TITLES[usize::from(id - 1)]
}

fn scenario_counts(scenarios: &[ScenarioResult], priority: &str) -> (u32, u32) {
    let selected: Vec<_> = scenarios
        .iter()
        .filter(|row| row.priority == priority)
        .collect();
    let passed = selected.iter().filter(|row| row.verdict == "pass").count();
    (
        u32::try_from(passed).expect("scenario count"),
        u32::try_from(selected.len()).expect("scenario count"),
    )
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires fitz-auth and fitz-anon from compose.yml"]
async fn should_complete_default_async_conformance_suite() {
    // Arrange
    let transport = Transport::from_env();
    let auth_mode = AuthMode::from_env();
    let connected = client(transport, auth_mode);
    connected.connect().await.expect("broker connection");
    let mut scenarios = Vec::with_capacity(24);

    // Act
    for id in 1..=24 {
        let started = Instant::now();
        let (verdict, evidence) = if matches!(id, 18 | 21..=23) {
            (
                "not_implemented",
                vec!["scenario is not implemented by this runner".into()],
            )
        } else {
            let mut evidence = should_run_scenario(id, &connected, transport, auth_mode)
                .await
                .unwrap_or_else(|error| panic!("CS-{id:03} failed: {error}"));
            let verdict = if (9..=16).contains(&id) {
                evidence.push(
                    "this smoke check does not establish all shared scenario expectations".into(),
                );
                "partial"
            } else {
                "pass"
            };
            (verdict, evidence)
        };
        scenarios.push(ScenarioResult {
            scenario_id: format!("CS-{id:03}"),
            title: scenario_title(id).into(),
            priority: if id <= 8 || matches!(id, 19 | 20 | 24) {
                "P0"
            } else {
                "P1"
            }
            .into(),
            client: CLIENT_NAME.into(),
            transport: transport.name().into(),
            auth_mode: auth_mode.name().into(),
            verdict,
            evidence,
            latency_ms: started.elapsed().as_millis(),
        });
    }
    connected.close().await.expect("close client");
    let (p0_pass, p0_total) = scenario_counts(&scenarios, "P0");
    let (p1_pass, p1_total) = scenario_counts(&scenarios, "P1");
    let p0_pass_rate = f64::from(p0_pass) / f64::from(p0_total);
    let p1_pass_rate = f64::from(p1_pass) / f64::from(p1_total);
    let overall_status = if p0_pass_rate < 1.0 {
        "fail"
    } else if p1_pass_rate < 1.0 {
        "partial"
    } else {
        "pass"
    };
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
        p0_pass,
        p0_total,
        p1_pass,
        p1_total,
        p0_pass_rate,
        p1_pass_rate,
        overall_status,
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
    assert_eq!(aggregate.scenarios.len(), 24);
    assert_eq!(
        aggregate.p0_pass, aggregate.p0_total,
        "shared P0 conformance gate is incomplete"
    );
    assert_ne!(
        aggregate.overall_status, "fail",
        "shared P0 conformance gate failed"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires fitz-auth and fitz-anon from compose.yml"]
async fn should_correlate_same_type_requests_out_of_order() {
    // Arrange
    let connected = client(Transport::from_env(), AuthMode::from_env());
    connected.connect().await.expect("broker connection");

    // Act
    exercise_same_type_requests(&connected).await;

    // Assert
    connected.close().await.expect("close client");
}

async fn exercise_same_type_requests(connected: &Client) {
    assert!(connected.correlation_enabled());
    assert_eq!(connected.server_capabilities(), (2, 0));
    let parked_route = unique_route("queue");
    let ready_route = unique_route("queue");
    connected
        .queue()
        .expect("queue client")
        .enqueue(&ready_route, b"second", None)
        .await
        .expect("seed ready queue");

    let parked_queue = connected.queue().expect("queue client");
    let parked_request_route = parked_route.clone();
    let parked = tokio::spawn(async move {
        parked_queue
            .reserve(&parked_request_route, 30, 1, Some(5))
            .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!parked.is_finished(), "first reserve must remain parked");
    let mut second = tokio::time::timeout(
        Duration::from_secs(2),
        connected
            .queue()
            .expect("queue client")
            .reserve(&ready_route, 30, 1, Some(0)),
    )
    .await
    .expect("second reserve was serialized")
    .expect("second reserve failed");
    assert_eq!(second.pop().expect("second item").body, b"second");
    assert!(!parked.is_finished(), "second reserve must resolve first");

    connected
        .queue()
        .expect("queue client")
        .enqueue(&parked_route, b"first", None)
        .await
        .expect("seed parked queue");
    let mut first = tokio::time::timeout(Duration::from_secs(5), parked)
        .await
        .expect("parked reserve timed out")
        .expect("parked task panicked")
        .expect("parked reserve failed");
    assert_eq!(first.pop().expect("first item").body, b"first");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires fitz-auth and fitz-anon from compose.yml"]
async fn should_preserve_empty_and_absent_stream_event_metadata() {
    // Arrange
    let connected = client(Transport::from_env(), AuthMode::from_env());
    connected.connect().await.expect("broker connection");
    let route = unique_route("stream");
    let mut session = connected
        .stream()
        .unwrap()
        .begin(&route, None)
        .await
        .unwrap();

    // Act
    session.append(0, b"empty", Some(b""), None).await.unwrap();
    session.append(1, b"absent", None, None).await.unwrap();
    session.commit(StreamCommitMode::Sync).await.unwrap();
    let records = connected
        .stream()
        .unwrap()
        .read(&route, 0, 10, None, None, None)
        .await
        .unwrap();

    // Assert
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].metadata, Some(Vec::new()));
    assert_eq!(records[1].metadata, None);
    connected.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires fitz-auth and fitz-anon from compose.yml"]
async fn should_replay_boundary_sized_events_on_the_longest_route() {
    // Arrange
    let connected = client(Transport::from_env(), AuthMode::from_env());
    connected.connect().await.unwrap();

    // Act
    exercise_stream_boundary(&connected).await;

    // Assert
    connected.close().await.unwrap();
}

async fn exercise_stream_boundary(connected: &Client) {
    let prefix = unique_route("stream");
    let route = format!("{prefix}{}", "r".repeat(4096 - prefix.len()));
    let metadata = [7_u8; 16];
    let body = vec![42_u8; 400 * 1024 - metadata.len()];
    let stream = connected.stream().unwrap();

    for offset in 0..3_u64 {
        let mut session = stream.begin(&route, None).await.unwrap();
        if offset == 0 {
            let oversized = vec![42_u8; body.len() + 1];
            let error = session
                .append(0, &oversized, Some(&metadata), None)
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                FitzError::DomainWire {
                    code: 2008,
                    effect: 0,
                    ..
                }
            ));
        }
        assert_eq!(
            session
                .append(offset, &body, Some(&metadata), None)
                .await
                .unwrap(),
            Some(offset)
        );
        session.commit(StreamCommitMode::Sync).await.unwrap();
    }
    let mut records = Vec::new();
    let mut next = 0;
    let mut pages = 0;
    loop {
        let page = stream
            .read_page(&route, next, 10, StreamReadOptions::default())
            .await
            .unwrap();
        pages += 1;
        records.extend(page.items.into_iter().map(|item| match item {
            StreamReadItem::Event(record) => record,
            _ => panic!("unexpected filtered item"),
        }));
        if !page.cursor.has_more {
            break;
        }
        assert!(page.cursor.last_resource_offset >= next);
        next = page.cursor.last_resource_offset + 1;
    }

    assert!(pages > 1);
    assert_eq!(records.len(), 3);
    for (offset, record) in records.iter().enumerate() {
        assert_eq!(record.route, route);
        assert_eq!(record.offset, u64::try_from(offset).unwrap());
        assert_eq!(record.body, body);
        assert_eq!(record.metadata.as_deref(), Some(metadata.as_slice()));
    }
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
    connected
        .notice()
        .expect("notice client")
        .unsubscribe_all()
        .await
        .expect("unsubscribe all notices");
    assert!(notice_subscription.next().await.is_none());
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
    let batch_route = format!("{}/run", unique_route("schedule"));
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
    connected
        .schedule()
        .expect("schedule client")
        .create_batch(&[ScheduleEntry {
            route: batch_route.clone(),
            cron: "*/5 * * * *".into(),
            delivery_mode: ScheduleDeliveryMode::Single,
            payload: b"batch".to_vec(),
        }])
        .await
        .expect("create schedule batch");
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
    let cursor_page = connected
        .schedule()
        .expect("schedule client")
        .list_v2(None, Some(1000))
        .await
        .expect("list cursor page");
    assert!(
        cursor_page
            .entries
            .iter()
            .any(|entry| entry.route == batch_route)
    );
    connected
        .schedule()
        .expect("schedule client")
        .cancel(&schedule_route)
        .await
        .expect("cancel schedule");
    connected
        .schedule()
        .expect("schedule client")
        .cancel(&batch_route)
        .await
        .expect("cancel batch schedule");
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

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires fitz-auth and fitz-anon from compose.yml"]
async fn should_increase_managed_lease_authority_after_handoff() {
    // Arrange: connect independent owners over the selected transport and auth matrix leg.
    let transport = Transport::from_env();
    let auth_mode = AuthMode::from_env();
    let first = client(transport, auth_mode);
    let successor = client(transport, auth_mode);
    first.connect().await.expect("first broker connection");
    successor
        .connect()
        .await
        .expect("successor broker connection");
    let route = unique_route("lease");

    // Act: release one managed owner before admitting its successor on the same route.
    let first_token = first
        .lease()
        .expect("first lease client")
        .with_lease_authority(&route, "first", 30, |_, authority| async move {
            Ok::<u64, Infallible>(authority.fencing_token)
        })
        .await
        .expect("first managed lease");
    let successor_token = successor
        .lease()
        .expect("successor lease client")
        .with_lease_authority(&route, "successor", 30, |_, authority| async move {
            Ok::<u64, Infallible>(authority.fencing_token)
        })
        .await
        .expect("successor managed lease");

    // Assert: each new ownership receives a strictly greater admission fence.
    assert!(successor_token > first_token);
    first.close().await.expect("close first client");
    successor.close().await.expect("close successor client");
}
