mod jwt;

use cntryl_fitz::FitzClient;
use cntryl_fitz::domains::schedule::ScheduleDeliveryMode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static ROUTE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy)]
enum Transport {
    Tcp,
    WebSocket,
}

fn connect_client(transport: Transport) -> FitzClient {
    let token = jwt::make_test_jwt("test-realm", "dev-test-secret");
    match transport {
        Transport::Tcp => FitzClient::connect_tcp("127.0.0.1", 4091, &token),
        Transport::WebSocket => FitzClient::connect_ws("ws://127.0.0.1:4090/ws", &token),
    }
    .expect("failed to connect to fitz broker")
}

fn unique_route(domain: &str) -> String {
    let counter = ROUTE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock drift")
        .as_nanos();
    let resource = format!("{nonce}-{counter}");

    if domain == "schedule" {
        format!("{domain}://test-realm/integration/{resource}/run")
    } else {
        format!("{domain}://test-realm/integration/{resource}")
    }
}

fn run_queue_lifecycle(transport: Transport) {
    let client = connect_client(transport);
    let route = unique_route("queue");

    let id = client
        .queue()
        .enqueue(&route, b"queue-body", None)
        .expect("failed to enqueue queue item");
    assert_ne!(id, 0);

    let items = client
        .queue()
        .reserve(&route, 30, Some(1), None)
        .expect("failed to reserve queue item");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].body, b"queue-body");
    items[0].complete().expect("failed to complete queue item");

    client.close().expect("failed to close queue client");
}

fn run_lease_lifecycle(transport: Transport) {
    let client = connect_client(transport);
    let route = unique_route("lease");
    let owner = "integration-owner";

    let grant = client
        .lease()
        .acquire(&route, owner, 30)
        .expect("failed to acquire lease");
    let held = client
        .lease()
        .query(&route)
        .expect("failed to query held lease");
    assert!(held.held);
    assert!(
        held.owner_id
            .as_deref()
            .is_some_and(|value| value.ends_with(owner))
    );

    let extended = client
        .lease()
        .extend(&route, owner, grant.fencing_token, 45)
        .expect("failed to extend lease");
    client
        .lease()
        .release(&route, owner, extended.fencing_token)
        .expect("failed to release lease");

    let released = client
        .lease()
        .query(&route)
        .expect("failed to query released lease");
    assert!(!released.held);

    client.close().expect("failed to close lease client");
}

fn run_notice_lifecycle(transport: Transport) {
    let route = unique_route("notice");
    let subscriber_client = connect_client(transport);
    let publisher_client = connect_client(transport);
    let subscription = subscriber_client
        .notice()
        .subscribe(&route)
        .expect("failed to subscribe to notice route");
    let (message_tx, message_rx) = mpsc::channel();

    let listener = thread::spawn(move || {
        let message = subscription
            .next()
            .expect("failed to receive notice message");
        message_tx
            .send(message)
            .expect("failed to forward notice message");
        subscription
            .unsubscribe()
            .expect("failed to unsubscribe from notice route");
        subscriber_client
            .close()
            .expect("failed to close notice subscriber");
    });

    publisher_client
        .notice()
        .publish(&route, b"notice-body")
        .expect("failed to publish notice");
    let message = message_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("notice message was not delivered");
    assert_eq!(message.route, route);
    assert_eq!(message.body, b"notice-body");

    publisher_client
        .close()
        .expect("failed to close notice publisher");
    listener.join().expect("notice listener panicked");
}

fn run_schedule_lifecycle(transport: Transport) {
    let client = connect_client(transport);
    let route = unique_route("schedule");

    let id = client
        .schedule()
        .create(
            &route,
            "*/5 * * * *",
            ScheduleDeliveryMode::Broadcast,
            b"schedule-body",
        )
        .expect("failed to create schedule");
    assert!(!id.is_empty());

    let (entries, _) = client
        .schedule()
        .list(None, None)
        .expect("failed to list schedules");
    assert!(entries.iter().any(|entry| entry.route == route));

    client
        .schedule()
        .cancel(&route)
        .expect("failed to cancel schedule");
    client.close().expect("failed to close schedule client");
}

#[test]
#[ignore = "requires fitz-auth from compose.yml on 127.0.0.1:4090/4091"]
fn should_complete_queue_lifecycle_over_tcp() {
    run_queue_lifecycle(Transport::Tcp);
}

#[test]
#[ignore = "requires fitz-auth from compose.yml on 127.0.0.1:4090/4091"]
fn should_complete_queue_lifecycle_over_websocket() {
    run_queue_lifecycle(Transport::WebSocket);
}

#[test]
#[ignore = "requires fitz-auth from compose.yml on 127.0.0.1:4090/4091"]
fn should_complete_lease_lifecycle_over_tcp() {
    run_lease_lifecycle(Transport::Tcp);
}

#[test]
#[ignore = "requires fitz-auth from compose.yml on 127.0.0.1:4090/4091"]
fn should_complete_lease_lifecycle_over_websocket() {
    run_lease_lifecycle(Transport::WebSocket);
}

#[test]
#[ignore = "requires fitz-auth from compose.yml on 127.0.0.1:4090/4091"]
fn should_complete_notice_lifecycle_over_tcp() {
    run_notice_lifecycle(Transport::Tcp);
}

#[test]
#[ignore = "requires fitz-auth from compose.yml on 127.0.0.1:4090/4091"]
fn should_complete_notice_lifecycle_over_websocket() {
    run_notice_lifecycle(Transport::WebSocket);
}

#[test]
#[ignore = "requires fitz-auth from compose.yml on 127.0.0.1:4090/4091"]
fn should_complete_schedule_lifecycle_over_tcp() {
    run_schedule_lifecycle(Transport::Tcp);
}

#[test]
#[ignore = "requires fitz-auth from compose.yml on 127.0.0.1:4090/4091"]
fn should_complete_schedule_lifecycle_over_websocket() {
    run_schedule_lifecycle(Transport::WebSocket);
}
