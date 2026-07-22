//! Integration test: KV domain over WebSocket transport
//! This test connects to a real Fitz server running on 127.0.0.1:4090 (WebSocket port)
//! and executes a complete KV transaction sequence

mod jwt;

use cntryl_fitz::FitzClient;
use cntryl_fitz::TransactionMode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static ROUTE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_kv_route(suffix: &str) -> String {
    let counter = ROUTE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock drift")
        .as_nanos();
    format!("kv://test-realm/{nonce}-{counter}/{suffix}")
}

/// Note: This test requires Fitz server with WebSocket support running on 127.0.0.1:4090
/// Start with: docker compose up (or cargo run -F boot)
#[test]
fn should_execute_kv_transaction_over_websocket() {
    // Arrange
    let token = jwt::make_test_jwt("test-realm", "dev-test-secret");
    let client = FitzClient::connect_ws("ws://127.0.0.1:4090/ws", &token)
        .expect("Failed to connect to Fitz WebSocket");

    let kv = client.kv();
    let route = unique_kv_route("users");

    // Act - Begin transaction
    let tx = kv
        .begin(&route, TransactionMode::ReadWrite)
        .expect("Failed to begin transaction");

    // Act - Put a value
    let key = b"username:bob";
    let value = b"Bob Smith";
    tx.put(key, value).expect("Failed to put value");

    // Act - Get the value back
    let retrieved = tx
        .get(key)
        .expect("Failed to get value")
        .expect("Value not found after put");

    // Assert
    assert_eq!(retrieved, value, "Retrieved value should match put value");

    // Act - Delete the key
    tx.delete(key).expect("Failed to delete key");

    // Assert - Key should not exist after delete
    let after_delete = tx.get(key).expect("Failed to get after delete");

    assert!(
        after_delete.is_none(),
        "Value should not exist after delete"
    );

    // Act - Commit transaction
    tx.commit().expect("Failed to commit transaction");

    // Act - Verify in new transaction that changes persisted
    let verify_tx = kv
        .begin(&route, TransactionMode::ReadOnly)
        .expect("Failed to begin verify transaction");

    let verify = verify_tx.get(key).expect("Failed to verify");

    assert!(verify.is_none(), "Value should not exist after commit");

    verify_tx
        .commit()
        .expect("Failed to commit verify transaction");

    // Cleanup
    client.close().expect("Failed to close connection");
}

/// Test rollback over WebSocket
#[test]
fn should_rollback_kv_transaction_over_websocket() {
    // Arrange
    let token = jwt::make_test_jwt("test-realm", "dev-test-secret");
    let client =
        FitzClient::connect_ws("ws://127.0.0.1:4090/ws", &token).expect("Failed to connect");

    let kv = client.kv();
    let route = unique_kv_route("rollback");

    // Put initial value
    let setup_tx = kv
        .begin(&route, TransactionMode::ReadWrite)
        .expect("Failed to begin setup");

    let key = b"rollback_test_ws";
    let original_value = b"original_ws";
    setup_tx
        .put(key, original_value)
        .expect("Failed to put setup value");
    setup_tx.commit().expect("Failed to commit setup");

    // Act - Begin transaction, modify value, then rollback
    let tx = kv
        .begin(&route, TransactionMode::ReadWrite)
        .expect("Failed to begin");

    let new_value = b"modified_ws";
    tx.put(key, new_value).expect("Failed to put");

    // Verify we see the new value in the transaction
    let in_tx = tx
        .get(key)
        .expect("Failed to get in tx")
        .expect("Value not found in tx");
    assert_eq!(
        in_tx, new_value,
        "Should see updated value in same transaction"
    );

    // Act - Rollback instead of commit
    tx.rollback().expect("Failed to rollback");

    // Assert - New transaction should see original value
    let verify_tx = kv
        .begin(&route, TransactionMode::ReadOnly)
        .expect("Failed to begin verify");

    let after_rollback = verify_tx
        .get(key)
        .expect("Failed to get after rollback")
        .expect("Original value should still exist");

    assert_eq!(
        after_rollback, original_value,
        "Rollback should restore original value"
    );

    verify_tx.commit().expect("Failed to commit verify");

    client.close().expect("Failed to close");
}
