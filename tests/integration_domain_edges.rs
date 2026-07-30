mod jwt;

use cntryl_fitz::domains::schedule::ScheduleDeliveryMode;
use cntryl_fitz::{FitzClient, TransactionMode};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

static ROUTE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy)]
enum Transport {
    Tcp,
    WebSocket,
}

const TRANSPORTS: [Transport; 2] = [Transport::Tcp, Transport::WebSocket];

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
    if domain == "schedule" {
        format!("{domain}://test-realm/integration/{nonce}-{counter}/run")
    } else {
        format!("{domain}://test-realm/integration/{nonce}-{counter}")
    }
}

#[test]
#[ignore = "requires fitz-auth from compose.yml on 127.0.0.1:4090/4091"]
fn should_return_not_found_given_missing_key_when_get_called() {
    // Arrange
    for transport in TRANSPORTS {
        let client = connect_client(transport);
        let transaction = client
            .kv()
            .begin(&unique_route("kv"), TransactionMode::ReadOnly)
            .expect("failed to begin read-only transaction");

        // Act
        let result = transaction.get(b"missing").expect("get failed");

        // Assert
        assert!(result.is_none());
        transaction.rollback().expect("rollback failed");
        client.close().expect("close failed");
    }
}

#[test]
#[ignore = "requires fitz-auth from compose.yml on 127.0.0.1:4090/4091"]
fn should_reject_write_given_read_only_transaction_mode_when_put_called() {
    // Arrange
    for transport in TRANSPORTS {
        let client = connect_client(transport);
        let transaction = client
            .kv()
            .begin(&unique_route("kv"), TransactionMode::ReadOnly)
            .expect("failed to begin read-only transaction");

        // Act
        let result = transaction.put(b"key", b"value");

        // Assert
        assert!(result.is_err());
        transaction.rollback().expect("rollback failed");
        client.close().expect("close failed");
    }
}

#[test]
#[ignore = "requires fitz-auth from compose.yml on 127.0.0.1:4090/4091"]
fn should_redeliver_given_expired_reservation_when_reserve_called() {
    // Arrange
    for transport in TRANSPORTS {
        let client = connect_client(transport);
        let route = unique_route("queue");
        client
            .queue()
            .enqueue(&route, b"redeliver", None)
            .expect("enqueue failed");
        let first = client
            .queue()
            .reserve(&route, 1, Some(1), None)
            .expect("first reserve failed");
        assert_eq!(first.len(), 1);
        thread::sleep(Duration::from_millis(1_100));

        // Act
        let deadline = Instant::now() + Duration::from_secs(3);
        let second = loop {
            let items = client
                .queue()
                .reserve(&route, 30, Some(1), None)
                .expect("second reserve failed");
            if !items.is_empty() {
                break items;
            }
            assert!(Instant::now() < deadline, "reservation was not redelivered");
            thread::sleep(Duration::from_millis(100));
        };

        // Assert
        assert_eq!(second[0].body, b"redeliver");
        second[0].complete().expect("completion failed");
        client.close().expect("close failed");
    }
}

#[test]
#[ignore = "requires fitz-auth from compose.yml on 127.0.0.1:4090/4091"]
fn should_delay_visibility_given_nonzero_delay_when_reserve_called() {
    // Arrange
    for transport in TRANSPORTS {
        let client = connect_client(transport);
        let route = unique_route("queue");
        client
            .queue()
            .enqueue(&route, b"delayed", Some(2_000))
            .expect("enqueue failed");

        // Act
        let early = client
            .queue()
            .reserve(&route, 30, Some(1), None)
            .expect("early reserve failed");

        // Assert
        assert!(early.is_empty());
        client.close().expect("close failed");
    }
}

#[test]
#[ignore = "requires fitz-auth from compose.yml on 127.0.0.1:4090/4091"]
fn should_deliver_given_visibility_delay_elapsed_when_reserve_called() {
    // Arrange
    for transport in TRANSPORTS {
        let client = connect_client(transport);
        let route = unique_route("queue");
        client
            .queue()
            .enqueue(&route, b"delayed", Some(2_000))
            .expect("enqueue failed");
        thread::sleep(Duration::from_millis(2_100));

        // Act
        let visible = client
            .queue()
            .reserve(&route, 30, Some(1), None)
            .expect("visible reserve failed");

        // Assert
        assert_eq!(visible[0].body, b"delayed");
        visible[0].complete().expect("completion failed");
        client.close().expect("close failed");
    }
}

#[test]
#[ignore = "requires fitz-auth from compose.yml on 127.0.0.1:4090/4091"]
fn should_reject_acquire_given_held_lease_when_acquire_called() {
    // Arrange
    for transport in TRANSPORTS {
        let owner = connect_client(transport);
        let contender = connect_client(transport);
        let route = unique_route("lease");
        let grant = owner
            .lease()
            .acquire(&route, "owner", 30)
            .expect("owner acquire failed");

        // Act
        let result = contender.lease().acquire(&route, "contender", 30);

        // Assert
        assert!(result.is_err());
        owner
            .lease()
            .release(&route, "owner", grant.fencing_token)
            .expect("release failed");
        owner.close().expect("owner close failed");
        contender.close().expect("contender close failed");
    }
}

#[test]
#[ignore = "requires fitz-auth from compose.yml on 127.0.0.1:4090/4091"]
fn should_reject_stale_token_given_renew_when_lease_handle_used() {
    // Arrange
    for transport in TRANSPORTS {
        let client = connect_client(transport);
        let route = unique_route("lease");
        let grant = client
            .lease()
            .acquire(&route, "owner", 30)
            .expect("acquire failed");

        // Act
        let result = client
            .lease()
            .extend(&route, "owner", grant.fencing_token + 1, 30);

        // Assert
        assert!(result.is_err());
        client
            .lease()
            .release(&route, "owner", grant.fencing_token)
            .expect("release failed");
        client.close().expect("close failed");
    }
}

#[test]
#[ignore = "requires fitz-auth from compose.yml on 127.0.0.1:4090/4091"]
fn should_reject_invalid_cron_given_malformed_syntax_when_create_called() {
    // Arrange
    for transport in TRANSPORTS {
        let client = connect_client(transport);

        // Act
        let result = client.schedule().create(
            &unique_route("schedule"),
            "not a cron",
            ScheduleDeliveryMode::Broadcast,
            &[],
        );

        // Assert
        assert!(result.is_err());
        client.close().expect("close failed");
    }
}

#[test]
#[ignore = "requires fitz-auth from compose.yml on 127.0.0.1:4090/4091"]
fn should_reject_append_given_mismatched_expected_offset_when_append_called() {
    // Arrange
    for transport in TRANSPORTS {
        let client = connect_client(transport);
        let mut session = client
            .stream()
            .begin(&unique_route("stream"), None)
            .expect("begin failed");

        // Act
        let result = session.append(42, b"mismatch", None, None);

        // Assert
        assert!(result.is_err());
        session.rollback().expect("rollback failed");
        client.close().expect("close failed");
    }
}

#[test]
#[ignore = "requires fitz-auth from compose.yml on 127.0.0.1:4090/4091"]
fn should_return_empty_given_offset_beyond_watermark_when_read_called() {
    // Arrange
    for transport in TRANSPORTS {
        let client = connect_client(transport);
        let route = unique_route("stream");

        // Act
        let records = client
            .stream()
            .read(&route, 999_999, 10, None, None)
            .expect("read failed");

        // Assert
        assert!(records.is_empty());
        client.close().expect("close failed");
    }
}

#[test]
#[ignore = "requires fitz-auth from compose.yml on 127.0.0.1:4090/4091"]
fn should_discard_writes_given_open_session_when_rollback_called() {
    // Arrange
    for transport in TRANSPORTS {
        let client = connect_client(transport);
        let route = unique_route("stream");
        let mut session = client.stream().begin(&route, None).expect("begin failed");
        session
            .append(0, b"discarded", None, None)
            .expect("append failed");

        // Act
        session.rollback().expect("rollback failed");

        // Assert
        let records = client
            .stream()
            .read(&route, 0, 10, None, None)
            .expect("read failed");
        assert!(records.is_empty());
        client.close().expect("close failed");
    }
}
