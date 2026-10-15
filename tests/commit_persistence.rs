use cntryl_fitz::client_domains::stream::StreamCommitMode;
use cntryl_fitz::{Client, KvDurability, TransactionMode};

#[tokio::test]
#[ignore = "requires fitz:develop anonymous broker at TCP 4191 and WebSocket 4190"]
async fn should_commit_with_explicit_persistence_against_develop()
-> Result<(), Box<dyn std::error::Error>> {
    // Arrange
    for (transport, endpoint) in ["tcp://127.0.0.1:4191", "ws://127.0.0.1:4190/ws"]
        .into_iter()
        .enumerate()
    {
        let client = Client::anonymous(endpoint).build()?;
        client.connect().await?;
        for (index, mode) in [KvDurability::Buffered, KvDurability::Sync]
            .into_iter()
            .enumerate()
        {
            let suffix = format!("{}-{transport}-{index}", std::process::id());
            let kv = client.kv()?;
            let route = format!("kv://test-realm/commit/{suffix}");
            // Act
            let transaction = kv.begin(&route, TransactionMode::ReadWrite).await?;
            transaction.put(b"key", b"value").await?;
            transaction.commit(mode).await?;
            // Assert
            let read = kv.begin(&route, TransactionMode::ReadOnly).await?;
            assert_eq!(
                read.get(b"key").await?,
                cntryl_fitz::client_domains::kv::KvGetResult::Found(b"value".to_vec())
            );
            read.rollback().await?;
            let route = format!("stream://test-realm/commit/{suffix}");
            let mut session = client.stream()?.begin(&route, None).await?;
            session.append(0, b"record", None, None).await?;
            session
                .commit(if index == 0 {
                    StreamCommitMode::Buffered
                } else {
                    StreamCommitMode::Sync
                })
                .await?;
        }
        client.close().await?;
    }
    Ok(())
}
