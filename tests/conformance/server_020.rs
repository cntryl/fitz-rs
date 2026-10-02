use super::{AuthMode, Transport, client, unique_route};
use cntryl_fitz::client_domains::kv::KvScanOptions;
use cntryl_fitz::{KvDurability, TransactionMode};

#[tokio::test]
#[ignore = "requires fitz-auth and fitz-anon from compose.yml"]
async fn should_advertise_server_020_capabilities() {
    // Arrange
    let connected = client(Transport::from_env(), AuthMode::from_env());

    // Act
    connected.connect().await.expect("broker connection");
    let (version, capabilities) = connected.server_capabilities();
    connected.close().await.expect("close client");

    // Assert
    assert_eq!(version, 1);
    assert_eq!(
        capabilities & 7,
        7,
        "server 0.2.0 requires capability bits 0, 1 and 2"
    );
}

#[tokio::test]
#[ignore = "requires fitz-auth and fitz-anon from compose.yml"]
async fn should_exclude_resume_key_in_forward_scan() {
    // Arrange
    let reverse = false;
    // Act
    let keys = exclusive_resume(reverse).await;
    // Assert
    assert_eq!(keys, vec![vec![0x10, 0], vec![0x20]]);
}

#[tokio::test]
#[ignore = "requires fitz-auth and fitz-anon from compose.yml"]
async fn should_exclude_resume_key_in_reverse_scan() {
    // Arrange
    let reverse = true;
    // Act
    let keys = exclusive_resume(reverse).await;
    // Assert
    assert_eq!(keys, vec![vec![0x10, 0], vec![0x10]]);
}

async fn exclusive_resume(reverse: bool) -> Vec<Vec<u8>> {
    let connected = client(Transport::from_env(), AuthMode::from_env());
    connected.connect().await.expect("broker connection");
    let route = unique_route("kv");
    let seed = connected
        .kv()
        .expect("kv")
        .begin(&route, TransactionMode::ReadWrite, KvDurability::Sync)
        .await
        .expect("seed");
    for key in [vec![0x10], vec![0x10, 0], vec![0x20]] {
        seed.put(&key, &key).await.expect("put");
    }
    seed.commit().await.expect("commit");
    let tx = connected
        .kv()
        .expect("kv")
        .begin(&route, TransactionMode::ReadOnly, KvDurability::Sync)
        .await
        .expect("read transaction");
    let first = tx
        .scan(&KvScanOptions {
            limit: Some(1),
            reverse,
            ..Default::default()
        })
        .await
        .expect("first page");
    assert_eq!(first.pairs.len(), 1);
    assert!(first.has_more);
    let resumed = tx
        .scan(&KvScanOptions {
            start_key: Some(first.pairs[0].key.clone()),
            start_exclusive: true,
            limit: Some(2),
            reverse,
            ..Default::default()
        })
        .await
        .expect("server 0.2.0 exclusive resume");
    assert!(!resumed.has_more);
    tx.rollback().await.expect("rollback");
    connected.close().await.expect("close client");
    resumed.pairs.into_iter().map(|pair| pair.key).collect()
}
