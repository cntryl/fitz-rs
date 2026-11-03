use super::{AuthMode, Transport, client, unique_route};
use cntryl_fitz::client_domains::queue::QueueEnqueueOptions;
use std::num::NonZeroU32;

#[tokio::test]
#[ignore = "requires Wire 3 fitz-auth and fitz-anon from compose.yml"]
async fn should_relinquish_ttl_message_immediately_through_nack() {
    // Arrange
    let connected = client(Transport::from_env(), AuthMode::from_env());
    connected.connect().await.expect("broker connection");
    let queue = connected.queue().expect("queue client");
    let route = unique_route("queue");
    queue
        .enqueue_with_options(
            &route,
            b"wire3",
            QueueEnqueueOptions {
                ttl_hours: NonZeroU32::new(1),
                ..QueueEnqueueOptions::default()
            },
        )
        .await
        .expect("enqueue with TTL");
    let item = queue
        .reserve(&route, 30, 1, None)
        .await
        .expect("reserve")
        .remove(0);
    // Act
    item.nack(0).await.expect("immediate NACK");
    let items = queue
        .reserve(&route, 30, 1, None)
        .await
        .expect("reserve again");
    // Assert
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].body, b"wire3");
    items[0].complete().await.expect("complete");
    connected.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires Wire 3 fitz-auth and fitz-anon from compose.yml"]
async fn should_defer_visibility_after_delayed_nack() {
    // Arrange
    let connected = client(Transport::from_env(), AuthMode::from_env());
    connected.connect().await.expect("broker connection");
    let queue = connected.queue().expect("queue client");
    let route = unique_route("queue");
    queue
        .enqueue(&route, b"delay", None)
        .await
        .expect("enqueue");
    let item = queue
        .reserve(&route, 30, 1, None)
        .await
        .expect("reserve")
        .remove(0);
    // Act
    item.nack(60).await.expect("delayed NACK");
    let items = queue
        .reserve(&route, 30, 1, None)
        .await
        .expect("reserve before visibility");
    // Assert
    assert!(items.is_empty());
    connected.close().await.expect("close");
}
