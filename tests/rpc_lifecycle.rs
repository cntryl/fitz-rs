use cntryl_fitz::{Client, Result};
use futures_util::StreamExt;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

#[tokio::test]
async fn should_count_unpolled_worker_buffer_time_against_inbound_budget() -> Result<()> {
    // Arrange
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (delivered, received) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        handshake_worker(&mut stream).await;
        write_request(&mut stream, 30).await;
        delivered.send(()).unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
    });
    let client = Client::anonymous(format!("tcp://{address}")).build()?;
    client.connect().await?;
    let mut worker = client
        .rpc()?
        .register_worker("rpc://prod/app/work", 1)
        .await?;

    // Act
    received.await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let request = worker.next().await.unwrap()?;

    // Assert
    assert_eq!(request.remaining_time(), Some(Duration::ZERO));
    assert!(request.cancellation.is_cancelled());
    drop(request);
    client.close().await?;
    server.abort();
    Ok(())
}

#[tokio::test]
async fn should_expire_worker_token_locally_without_broker_signal() -> Result<()> {
    // Arrange
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        handshake_worker(&mut stream).await;
        write_request(&mut stream, 30).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    });
    let client = Client::anonymous(format!("tcp://{address}")).build()?;
    client.connect().await?;
    let mut worker = client
        .rpc()?
        .register_worker("rpc://prod/app/work", 1)
        .await?;
    let request = worker.next().await.unwrap()?;

    // Act
    let result =
        tokio::time::timeout(Duration::from_millis(300), request.cancellation.cancelled()).await;

    // Assert
    assert!(result.is_ok());
    drop(request);
    client.close().await?;
    server.abort();
    Ok(())
}

#[tokio::test]
async fn should_acknowledge_cleanup_only_when_request_is_released_after_terminal_response()
-> Result<()> {
    // Arrange
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (terminal, terminal_received) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        handshake_worker(&mut stream).await;
        write_request(&mut stream, 3000).await;
        assert_eq!(read_frame(&mut stream).await.0, 303);
        terminal.send(()).unwrap();
        tokio::time::timeout(Duration::from_millis(100), read_frame(&mut stream)).await
    });
    let client = Client::anonymous(format!("tcp://{address}")).build()?;
    client.connect().await?;
    let mut worker = client
        .rpc()?
        .register_worker("rpc://prod/app/work", 1)
        .await?;
    let mut request = worker.next().await.unwrap()?;

    // Act
    request.respond(&[], true).await?;
    terminal_received.await.unwrap();
    let early_ack = server.await.unwrap();

    // Assert
    assert!(
        early_ack.is_err(),
        "cleanup ACK arrived while invocation context was still held"
    );
    drop(request);
    client.close().await?;
    Ok(())
}

#[tokio::test]
async fn should_acknowledge_negotiated_worker_cleanup_when_request_is_released() -> Result<()> {
    // Arrange
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        handshake_worker(&mut stream).await;
        write_request(&mut stream, 3000).await;
        read_frame(&mut stream).await
    });
    let client = Client::anonymous(format!("tcp://{address}")).build()?;
    client.connect().await?;
    let mut worker = client
        .rpc()?
        .register_worker("rpc://prod/app/work", 1)
        .await?;
    let request = worker.next().await.unwrap()?;

    // Act
    drop(request);
    let (kind, payload) = tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();

    // Assert
    assert_eq!(kind, 304);
    assert_eq!(payload, [vec![3], vec![0; 16]].concat());
    client.close().await?;
    Ok(())
}

async fn handshake_worker(stream: &mut TcpStream) {
    assert_eq!(read_frame(stream).await.0, 1);
    stream
        .write_all(&encode_message_frame(4, &[0, 1, 0, 0, 0, 8]))
        .await
        .unwrap();
    assert_eq!(read_frame(stream).await.0, 300);
    stream
        .write_all(&encode_message_frame(300, &[0, 0, 0, 0, 0]))
        .await
        .unwrap();
}

#[tokio::test]
async fn should_preserve_legacy_caller_framing_given_local_timeout() -> Result<()> {
    // Arrange
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        assert_eq!(read_frame(&mut stream).await.0, 1);
        stream
            .write_all(&encode_message_frame(4, &[0, 1, 0, 0, 0, 0]))
            .await
            .unwrap();
        let request = read_frame(&mut stream).await;
        let extra = tokio::time::timeout(Duration::from_millis(100), read_frame(&mut stream)).await;
        (request, extra.is_err())
    });
    let client = Client::anonymous(format!("tcp://{address}")).build()?;
    client.connect().await?;
    let mut call = client
        .rpc()?
        .call_with_timeout(
            "rpc://prod/app/work",
            b"work",
            Some(Duration::from_millis(20)),
        )
        .await?;

    // Act
    let result = call.next().await;
    let outcome = call.cancel().await;
    let ((kind, payload), omitted_cancellation) = server.await.unwrap();

    // Assert
    assert!(matches!(result, Some(Err(cntryl_fitz::FitzError::Timeout))));
    assert_eq!(
        outcome,
        cntryl_fitz::client_domains::rpc::RpcCancellationOutcome::Unsupported
    );
    assert_eq!(kind, 302);
    assert_eq!(payload.len(), 16 + 4 + "rpc://prod/app/work".len() + 4 + 4);
    assert!(omitted_cancellation);
    client.close().await?;
    Ok(())
}

async fn write_request(stream: &mut TcpStream, budget_ms: u32) {
    let route = b"rpc://prod/app/work";
    let mut payload = vec![0; 16];
    payload.extend_from_slice(&u32::try_from(route.len()).unwrap().to_be_bytes());
    payload.extend_from_slice(route);
    payload.extend_from_slice(&[0, 0, 0, 0, 1, 1]);
    payload.extend_from_slice(&budget_ms.to_be_bytes());
    stream
        .write_all(&encode_message_frame(302, &payload))
        .await
        .unwrap();
}

async fn read_frame(stream: &mut TcpStream) -> (u16, Vec<u8>) {
    let mut length = [0; 4];
    stream.read_exact(&mut length).await.unwrap();
    let mut body = vec![0; u32::from_be_bytes(length) as usize];
    stream.read_exact(&mut body).await.unwrap();
    let (kind, header) = if body[0] == 255 {
        (u16::from_be_bytes([body[1], body[2]]), 5)
    } else {
        (u16::from(body[0]), 3)
    };
    (kind, body[header..].to_vec())
}

fn encode_message_frame(kind: u16, payload: &[u8]) -> Vec<u8> {
    let mut body = if kind < 255 {
        vec![u8::try_from(kind).unwrap()]
    } else {
        let mut header = vec![255];
        header.extend_from_slice(&kind.to_be_bytes());
        header
    };
    body.extend_from_slice(&u16::try_from(payload.len()).unwrap().to_be_bytes());
    body.extend_from_slice(payload);
    let mut frame = u32::try_from(body.len()).unwrap().to_be_bytes().to_vec();
    frame.extend_from_slice(&body);
    frame
}
