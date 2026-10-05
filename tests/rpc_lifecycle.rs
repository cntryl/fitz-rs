use cntryl_fitz::{Client, Result};
use futures_util::{SinkExt, StreamExt};
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
        let mut cancellation = vec![2];
        cancellation.extend_from_slice(&[0; 16]);
        cancellation.push(1);
        stream
            .write_all(&encode_message_frame(305, &cancellation))
            .await
            .unwrap();
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
    request.cancellation.cancelled().await;
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

#[tokio::test]
async fn should_report_connection_closed_while_waiting_for_cancellation_confirmation() -> Result<()>
{
    // Arrange
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        assert_eq!(read_frame(&mut stream).await.0, 1);
        stream
            .write_all(&encode_message_frame(4, &[0, 1, 0, 0, 0, 8]))
            .await
            .unwrap();
        assert_eq!(read_frame(&mut stream).await.0, 302);
        assert_eq!(read_frame(&mut stream).await.0, 304);
    });
    let client = Client::anonymous(format!("tcp://{address}")).build()?;
    client.connect().await?;
    let mut call = client.rpc()?.call("rpc://prod/app/work", &[]).await?;

    // Act
    let outcome = tokio::time::timeout(Duration::from_secs(1), call.cancel()).await;

    // Assert
    assert_eq!(
        outcome.unwrap(),
        cntryl_fitz::client_domains::rpc::RpcCancellationOutcome::ConnectionClosed
    );
    server.await.unwrap();
    client.close().await?;
    Ok(())
}

#[tokio::test]
async fn should_wait_for_delayed_server_hello_before_negotiating_worker_cancellation() -> Result<()>
{
    // Arrange
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        assert_eq!(read_frame(&mut stream).await.0, 1);
        tokio::time::sleep(Duration::from_millis(200)).await;
        stream
            .write_all(&encode_message_frame(4, &[0, 1, 0, 0, 0, 8]))
            .await
            .unwrap();
        let registration = read_frame(&mut stream).await;
        stream
            .write_all(&encode_message_frame(300, &[0, 0, 0, 0, 0]))
            .await
            .unwrap();
        registration
    });
    let client = Client::anonymous(format!("tcp://{address}")).build()?;

    // Act
    client.connect().await?;
    let worker = client
        .rpc()?
        .register_worker("rpc://prod/app/work", 1)
        .await?;
    let (kind, registration) = server.await.unwrap();

    // Assert
    assert_eq!(kind, 300);
    assert!(
        registration.ends_with(&[1, 1]),
        "worker registration omitted negotiated cancellation"
    );
    drop(worker);
    client.close().await?;
    Ok(())
}

#[tokio::test]
async fn should_wait_for_delayed_websocket_hello_before_negotiating_worker_cancellation()
-> Result<()> {
    // Arrange
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        assert_eq!(socket.next().await.unwrap().unwrap().into_data()[0], 1);
        tokio::time::sleep(Duration::from_millis(200)).await;
        socket
            .send(tokio_tungstenite::tungstenite::Message::Binary(
                encode_message_frame(4, &[0, 1, 0, 0, 0, 8])[4..]
                    .to_vec()
                    .into(),
            ))
            .await
            .unwrap();
        let registration = socket.next().await.unwrap().unwrap().into_data();
        socket
            .send(tokio_tungstenite::tungstenite::Message::Binary(
                encode_message_frame(300, &[0, 0, 0, 0, 0])[4..]
                    .to_vec()
                    .into(),
            ))
            .await
            .unwrap();
        registration
    });
    let client = Client::anonymous(format!("ws://{address}")).build()?;

    // Act
    client.connect().await?;
    let worker = client
        .rpc()?
        .register_worker("rpc://prod/app/work", 1)
        .await?;
    let registration = server.await.unwrap();

    // Assert
    assert_eq!(&registration[..3], &[255, 1, 44]);
    assert!(registration.ends_with(&[1, 1]));
    drop(worker);
    client.close().await?;
    Ok(())
}

#[tokio::test]
async fn should_bound_connection_wait_given_missing_server_hello() -> Result<()> {
    // Arrange
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        assert_eq!(read_frame(&mut stream).await.0, 1);
        tokio::time::sleep(Duration::from_secs(1)).await;
    });
    let client = Client::anonymous(format!("tcp://{address}"))
        .request_timeout(Duration::from_millis(50))
        .build()?;

    // Act
    let result = tokio::time::timeout(Duration::from_millis(300), client.connect()).await;

    // Assert
    assert!(result.unwrap().is_err());
    client.close().await?;
    server.abort();
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
async fn should_cleanup_cancelled_buffered_invocation_without_waiting_for_worker_poll() -> Result<()>
{
    // Arrange
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        handshake_worker(&mut stream).await;
        write_request(&mut stream, 3000).await;
        let mut cancellation = vec![2];
        cancellation.extend_from_slice(&[0; 16]);
        cancellation.push(1);
        stream
            .write_all(&encode_message_frame(305, &cancellation))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_millis(300), read_frame(&mut stream)).await
    });
    let client = Client::anonymous(format!("tcp://{address}")).build()?;
    client.connect().await?;
    let mut worker = client
        .rpc()?
        .register_worker("rpc://prod/app/work", 1)
        .await?;

    // Act
    let acknowledgement = server.await.unwrap();
    let delivered = tokio::time::timeout(Duration::from_millis(50), worker.next()).await;

    // Assert
    let (kind, payload) =
        acknowledgement.expect("buffered invocation cleanup was not acknowledged");
    assert_eq!(kind, 304);
    assert_eq!(payload, [vec![3], vec![0; 16]].concat());
    assert!(!matches!(delivered, Ok(Some(Ok(_)))));
    client.close().await?;
    Ok(())
}

#[tokio::test]
async fn should_cleanup_buffered_invocation_while_preserving_unrelated_active_request() -> Result<()>
{
    // Arrange
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (held, active) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        handshake_worker(&mut stream).await;
        write_request(&mut stream, 3000).await;
        active.await.unwrap();
        write_request_with_id(&mut stream, 3000, [1; 16]).await;
        let mut cancellation = vec![2];
        cancellation.extend_from_slice(&[1; 16]);
        cancellation.push(1);
        stream
            .write_all(&encode_message_frame(305, &cancellation))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_millis(300), read_frame(&mut stream)).await
    });
    let client = Client::anonymous(format!("tcp://{address}")).build()?;
    client.connect().await?;
    let mut worker = client
        .rpc()?
        .register_worker("rpc://prod/app/work", 1)
        .await?;
    let unrelated = worker.next().await.unwrap()?;

    // Act
    held.send(()).unwrap();
    let (kind, payload) = server.await.unwrap().unwrap();
    let canceled_unrelated = unrelated.cancellation.is_cancelled();
    let delivered = tokio::time::timeout(Duration::from_millis(50), worker.next()).await;

    // Assert
    assert_eq!(kind, 304);
    assert_eq!(payload, [vec![3], vec![1; 16]].concat());
    assert!(!canceled_unrelated);
    assert!(!matches!(delivered, Ok(Some(Ok(_)))));
    drop(unrelated);
    client.close().await?;
    Ok(())
}

#[tokio::test]
async fn should_reject_downstream_work_when_parent_is_already_cancelled() -> Result<()> {
    // Arrange
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        handshake_worker(&mut stream).await;
        write_request(&mut stream, 3000).await;
        tokio::time::timeout(Duration::from_millis(100), read_frame(&mut stream)).await
    });
    let client = Client::anonymous(format!("tcp://{address}")).build()?;
    client.connect().await?;
    let rpc = client.rpc()?;
    let mut worker = rpc.register_worker("rpc://prod/app/work", 1).await?;
    let request = worker.next().await.unwrap()?;
    request.cancellation.cancel();

    // Act
    let call = rpc
        .call_from_request(&request, "rpc://prod/app/child", &[])
        .await;
    let frame = server.await.unwrap();

    // Assert
    assert!(matches!(call, Err(cntryl_fitz::FitzError::Canceled)));
    assert!(frame.is_err());
    drop(request);
    client.close().await?;
    Ok(())
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
    write_request_with_id(stream, budget_ms, [0; 16]).await;
}

async fn write_request_with_id(stream: &mut TcpStream, budget_ms: u32, correlation_id: [u8; 16]) {
    let route = b"rpc://prod/app/work";
    let mut payload = correlation_id.to_vec();
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
