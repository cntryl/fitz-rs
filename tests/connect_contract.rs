use cntryl_fitz::{Client, ConnectWhenReadyOptions, FitzError, Result};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[test]
fn should_validate_service_name_using_utf8_byte_limit() {
    // Arrange / Act / Assert
    let valid = Client::builder("tcp://127.0.0.1:1", || async { Ok(String::new()) })
        .service_name("é".repeat(64))
        .build();
    assert!(valid.is_ok());

    let oversized = Client::builder("tcp://127.0.0.1:1", || async { Ok(String::new()) })
        .service_name("é".repeat(65))
        .build();
    assert!(oversized.is_err());
}

#[tokio::test]
async fn should_omit_service_name_given_legacy_broker() -> Result<()> {
    // Arrange
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut length = [0_u8; 4];
        stream.read_exact(&mut length).await.unwrap();
        let mut connect = vec![0; u32::from_be_bytes(length) as usize];
        stream.read_exact(&mut connect).await.unwrap();
        tokio::time::timeout(Duration::from_millis(200), stream.read_exact(&mut length))
            .await
            .is_err()
    });
    let client = Client::builder(format!("tcp://{address}"), || async { Ok(String::new()) })
        .service_name("orders-worker")
        .build()?;

    // Act
    client.connect().await?;

    // Assert
    assert!(server.await.expect("server task panicked"));
    client.close().await?;
    Ok(())
}

#[tokio::test]
async fn should_return_after_one_attempt_given_unavailable_broker_when_connect_called() -> Result<()>
{
    // Arrange
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    drop(listener);
    let client = Client::anonymous(format!("tcp://{address}")).build()?;

    // Act
    let result = tokio::time::timeout(Duration::from_millis(500), client.connect()).await;

    // Assert
    assert!(
        result.is_ok(),
        "connect retried instead of returning one attempt"
    );
    assert!(result.expect("timeout checked").is_err());
    Ok(())
}

#[tokio::test]
async fn should_connect_given_delayed_broker_when_connect_when_ready_called() -> Result<()> {
    // Arrange
    let reservation = TcpListener::bind("127.0.0.1:0").await?;
    let address = reservation.local_addr()?;
    drop(reservation);
    let server = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let listener = TcpListener::bind(address).await.unwrap();
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut length = [0_u8; 4];
        stream.read_exact(&mut length).await.unwrap();
        let mut frame = vec![0; u32::from_be_bytes(length) as usize];
        stream.read_exact(&mut frame).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
    });
    let client = Client::anonymous(format!("tcp://{address}")).build()?;

    // Act
    client
        .connect_when_ready(ConnectWhenReadyOptions {
            timeout: Duration::from_secs(1),
            initial_delay: Duration::from_millis(10),
            maximum_delay: Duration::from_millis(20),
            ..ConnectWhenReadyOptions::default()
        })
        .await?;

    // Assert
    assert_eq!(client.state(), cntryl_fitz::ConnectionState::Authenticated);
    client.close().await?;
    server.await.expect("server task panicked");
    Ok(())
}

#[tokio::test]
async fn should_send_service_name_given_late_metadata_capability() -> Result<()> {
    // Arrange
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut length = [0_u8; 4];
        stream.read_exact(&mut length).await.unwrap();
        let mut connect = vec![0; u32::from_be_bytes(length) as usize];
        stream.read_exact(&mut connect).await.unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        // A delayed SERVER_HELLO (type 4) advertises CAP_SESSION_METADATA (bit 1).
        stream
            .write_all(&[0, 0, 0, 9, 4, 0, 6, 0, 1, 0, 0, 0, 2])
            .await
            .unwrap();
        stream.read_exact(&mut length).await.unwrap();
        let mut metadata = vec![0; u32::from_be_bytes(length) as usize];
        stream.read_exact(&mut metadata).await.unwrap();
        metadata
    });
    let client = Client::builder(format!("tcp://{address}"), || async { Ok(String::new()) })
        .service_name("orders-worker")
        .build()?;

    // Act
    client.connect().await?;

    // Assert
    let metadata = tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .expect("broker should receive metadata")
        .expect("server task panicked");
    assert_eq!(&metadata[..3], &[5, 0, 17]);
    assert_eq!(
        &metadata[3..],
        &[
            0, 0, 0, 13, b'o', b'r', b'd', b'e', b'r', b's', b'-', b'w', b'o', b'r', b'k', b'e',
            b'r'
        ]
    );
    assert_eq!(client.server_capabilities(), (1, 1 << 1));
    client.close().await?;
    Ok(())
}

#[tokio::test]
async fn should_stop_given_cancellation_when_connect_when_ready_called() -> Result<()> {
    // Arrange
    let cancellation = tokio_util::sync::CancellationToken::new();
    cancellation.cancel();
    let client = Client::anonymous("tcp://127.0.0.1:1").build()?;

    // Act
    let error = client
        .connect_when_ready(ConnectWhenReadyOptions {
            cancellation,
            ..ConnectWhenReadyOptions::default()
        })
        .await
        .expect_err("canceled readiness should fail");

    // Assert
    assert!(matches!(error, FitzError::Canceled));
    Ok(())
}
