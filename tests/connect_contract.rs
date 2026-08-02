use cntryl_fitz::{Client, ConnectWhenReadyOptions, FitzError, Result};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

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
