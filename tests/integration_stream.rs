use cntryl::domains::stream::{
    StreamCommitMode, StreamDiscriminator, StreamFilterClause, StreamFilterSet,
    StreamFilteredReason, StreamReadItem,
};
use cntryl::FitzClient;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

static ROUTE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy)]
enum Transport {
    Tcp,
    WebSocket,
}

fn connect_client(transport: Transport) -> FitzClient {
    match transport {
        Transport::Tcp => {
            FitzClient::connect_tcp("127.0.0.1", 4091, "test-realm", "test-secret-key")
        }
        Transport::WebSocket => {
            FitzClient::connect_ws("ws://127.0.0.1:4090/ws", "test-realm", "test-secret-key")
        }
    }
    .expect("failed to connect to fitz broker")
}

fn unique_stream_route(suffix: &str) -> String {
    let counter = ROUTE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock drift")
        .as_nanos();
    format!("stream://test-realm/{nonce}-{counter}/{suffix}")
}

fn run_stream_commit_and_read(transport: Transport) {
    let client = connect_client(transport);
    let route = unique_stream_route("records");
    let alpha = StreamDiscriminator::from("proj.alpha");
    let beta = StreamDiscriminator::from("audit.beta");

    let mut session = client
        .stream()
        .begin(&route, None)
        .expect("failed to begin stream session");

    let first_offset = session
        .append(0, b"record-1", None, Some(&alpha))
        .expect("failed to append first record")
        .expect("missing first offset");
    let second_offset = session
        .append(first_offset + 1, b"record-2", None, Some(&beta))
        .expect("failed to append second record")
        .expect("missing second offset");

    assert!(second_offset >= first_offset);

    session
        .commit(StreamCommitMode::Sync)
        .expect("failed to commit stream session");

    let records = client
        .stream()
        .read(
            &route,
            0,
            10,
            None,
            Some(&StreamFilterSet {
                clauses: vec![StreamFilterClause::Equals("proj.alpha".to_string())],
            }),
        )
        .expect("failed to read stream records");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].body, b"record-1");
    assert_eq!(records[0].offset, first_offset);

    let page = client
        .stream()
        .read_page(
            &route,
            0,
            10,
            None,
            Some(&StreamFilterSet {
                clauses: vec![StreamFilterClause::Equals("proj.alpha".to_string())],
            }),
        )
        .expect("failed to read stream page");
    assert_eq!(page.cursor.last_resource_offset, second_offset);
    assert!(!page.cursor.has_more);
    assert_eq!(page.items.len(), 2);
    assert!(matches!(page.items[0], StreamReadItem::Event(_)));
    assert_eq!(
        page.items[1],
        StreamReadItem::Filtered {
            offset: second_offset,
            reason: Some(StreamFilteredReason::ServerFilter),
        }
    );

    let last = client
        .stream()
        .peek(&route)
        .expect("failed to read last stream record")
        .expect("expected last stream record");
    assert_eq!(last.body, b"record-2");

    let metadata = client
        .stream()
        .metadata(&route)
        .expect("failed to read stream metadata");
    assert_eq!(metadata.record_count, 2);
    assert!(metadata.last_offset >= metadata.first_offset);

    client.close().expect("failed to close client");
}

fn run_stream_subscription(transport: Transport) {
    let route = unique_stream_route("notify");
    let subscriber_client = connect_client(transport);
    let writer_client = connect_client(transport);

    let subscription = subscriber_client
        .stream()
        .subscribe(&route)
        .expect("failed to subscribe to stream");
    let (notification_tx, notification_rx) = mpsc::channel();

    let listener = thread::spawn(move || {
        let notification = subscription
            .next()
            .expect("failed to receive stream notification");
        notification_tx
            .send(notification)
            .expect("failed to forward stream notification");
        subscription
            .unsubscribe()
            .expect("failed to unsubscribe stream subscription");
        subscriber_client
            .close()
            .expect("failed to close subscriber client");
    });

    let mut session = writer_client
        .stream()
        .begin(&route, None)
        .expect("failed to begin writer session");
    session
        .append(0, b"notify", None, None)
        .expect("failed to append notification record");
    session
        .commit(StreamCommitMode::Sync)
        .expect("failed to commit notification session");

    let notification = notification_rx
        .recv()
        .expect("did not receive stream notification");
    assert_eq!(notification.route, route);
    assert_eq!(notification.event, "committed");
    assert_eq!(notification.batch_size, 1);
    assert!(notification.last_resource_offset >= notification.first_resource_offset);

    writer_client
        .close()
        .expect("failed to close writer client");
    listener.join().expect("listener thread panicked");
}

#[test]
fn should_commit_and_read_stream_over_tcp() {
    run_stream_commit_and_read(Transport::Tcp);
}

#[test]
fn should_commit_and_read_stream_over_websocket() {
    run_stream_commit_and_read(Transport::WebSocket);
}

#[test]
fn should_receive_stream_notifications_over_tcp() {
    run_stream_subscription(Transport::Tcp);
}

#[test]
fn should_receive_stream_notifications_over_websocket() {
    run_stream_subscription(Transport::WebSocket);
}
