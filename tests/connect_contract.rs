use cntryl_fitz::{Client, ConnectWhenReadyOptions, FitzError, Result};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[test]
fn should_accept_service_name_at_utf8_byte_limit() {
    // Arrange
    let name = "é".repeat(64);

    // Act
    let result = Client::builder("tcp://127.0.0.1:1", || async { Ok(String::new()) })
        .service_name(name)
        .build();

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_trim_service_name_before_utf8_byte_limit_validation() {
    // Arrange
    let name = format!(" {} ", "é".repeat(64));

    // Act
    let result = Client::builder("tcp://127.0.0.1:1", || async { Ok(String::new()) })
        .service_name(name)
        .build();

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_reject_service_name_exceeding_utf8_byte_limit() {
    // Arrange
    let name = "é".repeat(65);

    // Act
    let result = Client::builder("tcp://127.0.0.1:1", || async { Ok(String::new()) })
        .service_name(name)
        .build();

    // Assert
    assert!(result.is_err());
}

#[tokio::test]
async fn should_reject_legacy_broker_hello() -> Result<()> {
    // Arrange
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut length = [0_u8; 4];
        stream.read_exact(&mut length).await.unwrap();
        let mut connect = vec![0; u32::from_be_bytes(length) as usize];
        stream.read_exact(&mut connect).await.unwrap();
        stream
            .write_all(&[0, 0, 0, 9, 4, 0, 6, 0, 1, 0, 0, 0, 0])
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_millis(200), stream.read_exact(&mut length))
            .await
            .is_err()
    });
    let client = Client::builder(format!("tcp://{address}"), || async { Ok(String::new()) })
        .service_name("orders-worker")
        .build()?;

    // Act
    let connected = client.connect().await;

    // Assert
    assert!(connected.is_err());
    let _ = server.await.expect("server task panicked");
    client.close().await?;
    Ok(())
}

#[tokio::test]
async fn should_surface_version_mismatch_without_retrying_connect_when_ready() -> Result<()> {
    // Arrange
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut length = [0_u8; 4];
        stream.read_exact(&mut length).await.unwrap();
        let mut connect = vec![0; u32::from_be_bytes(length) as usize];
        stream.read_exact(&mut connect).await.unwrap();
        let message = b"no common wire version";
        let mut payload = 50_u16.to_be_bytes().to_vec();
        payload.extend_from_slice(&u32::try_from(message.len()).unwrap().to_be_bytes());
        payload.extend_from_slice(message);
        let mut frame = vec![0, 3, 0];
        frame.extend_from_slice(&0_u64.to_be_bytes());
        frame.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
        frame.extend_from_slice(&payload);
        stream
            .write_all(&u32::try_from(frame.len()).unwrap().to_be_bytes())
            .await
            .unwrap();
        stream.write_all(&frame).await.unwrap();
        tokio::time::timeout(Duration::from_millis(250), listener.accept())
            .await
            .is_err()
    });
    let client = Client::anonymous(format!("tcp://{address}")).build()?;

    // Act
    let result = client
        .connect_when_ready(ConnectWhenReadyOptions {
            timeout: Duration::from_secs(1),
            initial_delay: Duration::from_millis(10),
            maximum_delay: Duration::from_millis(10),
            ..ConnectWhenReadyOptions::default()
        })
        .await;

    // Assert
    let error = result.expect_err("version mismatch must fail");
    assert_eq!(error.kind(), cntryl_fitz::FitzErrorKind::Protocol);
    assert!(error.to_string().contains("50"));
    assert!(error.to_string().contains("no common wire version"));
    assert!(
        matches!(error, FitzError::VersionMismatch { code: 50, ref message } if message == "no common wire version")
    );
    assert!(
        server.await.expect("server task panicked"),
        "version mismatch retried"
    );
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
        stream.write_all(&hello_frame()).await.unwrap();
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
async fn should_send_service_name_in_connect_before_delayed_hello() -> Result<()> {
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
        stream.write_all(&hello_frame()).await.unwrap();
        connect
    });
    let service_name = "é".repeat(64);
    let client = Client::builder(format!("tcp://{address}"), || async { Ok(String::new()) })
        .service_name(format!(" {service_name} "))
        .build()?;

    // Act
    client.connect().await?;

    // Assert
    let connect = server.await.expect("server task panicked");
    let payload = &connect[15..];
    assert_eq!(&payload[..4], &[0, 2, 0, 2]);
    assert_eq!(&payload[8..12], &[0, 0, 0, 0]);
    assert_eq!(&payload[12..16], &128_u32.to_be_bytes());
    assert_eq!(&payload[16..144], service_name.as_bytes());
    assert_eq!(client.server_capabilities(), (2, 0));
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

fn hello_frame() -> Vec<u8> {
    let mut payload = 2_u16.to_be_bytes().to_vec();
    payload.extend_from_slice(&0_u32.to_be_bytes());
    payload.extend_from_slice(&1_048_576_u32.to_be_bytes());
    payload.extend_from_slice(&64_u32.to_be_bytes());
    payload.extend_from_slice(&1_u64.to_be_bytes());
    let mut frame = vec![0, 2, 0];
    frame.extend_from_slice(&0_u64.to_be_bytes());
    frame.extend_from_slice(&22_u32.to_be_bytes());
    frame.extend_from_slice(&payload);
    let mut transport = u32::try_from(frame.len()).unwrap().to_be_bytes().to_vec();
    transport.extend_from_slice(&frame);
    transport
}
