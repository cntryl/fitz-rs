use cntryl_fitz::{Client, ConnectionState, ReconnectPolicy, Result};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::Message;

const CONNECT: u16 = 1;
const NOTICE_SUBSCRIBE: u16 = 501;
const NOTICE_NOTIFY: u16 = 504;

#[tokio::test]
async fn should_restore_notice_subscription_given_real_transport_loss() -> Result<()> {
    // Arrange
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (drop_first_tx, drop_first_rx) = oneshot::channel();
    let server = tokio::spawn(run_reconnect_server(listener, drop_first_rx));
    let token_calls = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&token_calls);
    let client = Client::builder(format!("tcp://{address}"), move || {
        let calls = Arc::clone(&calls);
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok("opaque-token".to_owned())
        }
    })
    .request_timeout(Duration::from_secs(2))
    .reconnect_policy(ReconnectPolicy {
        base_delay: Duration::from_millis(10),
        maximum_delay: Duration::from_millis(20),
        maximum_attempts: 10,
        ..ReconnectPolicy::default()
    })
    .build()?;

    client.connect().await?;
    let mut subscription = client.notice()?.subscribe("notice://realm/app/*").await?;

    // Act
    let _ = drop_first_tx.send(());
    wait_for_reauthenticated(&client).await;
    let notification = tokio::time::timeout(Duration::from_secs(2), subscription.next())
        .await
        .expect("restored subscription notification timed out")
        .expect("restored subscription ended")?;

    // Assert
    assert_eq!(notification.route, "notice://realm/app/event");
    assert_eq!(notification.body, b"restored");
    assert!(token_calls.load(Ordering::SeqCst) >= 2);
    client.close().await?;
    server.await.expect("server task panicked")?;
    Ok(())
}

#[tokio::test]
async fn should_restore_notice_subscription_given_real_websocket_loss() -> Result<()> {
    // Arrange
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (drop_first_tx, drop_first_rx) = oneshot::channel();
    let server = tokio::spawn(run_websocket_reconnect_server(listener, drop_first_rx));
    let token_calls = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&token_calls);
    let client = Client::builder(format!("ws://{address}"), move || {
        let calls = Arc::clone(&calls);
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok("opaque-token".to_owned())
        }
    })
    .request_timeout(Duration::from_secs(2))
    .reconnect_policy(ReconnectPolicy {
        base_delay: Duration::from_millis(10),
        maximum_delay: Duration::from_millis(20),
        maximum_attempts: 10,
        ..ReconnectPolicy::default()
    })
    .build()?;

    client.connect().await?;
    let mut subscription = client.notice()?.subscribe("notice://realm/app/*").await?;

    // Act
    let _ = drop_first_tx.send(());
    let notification = tokio::time::timeout(Duration::from_secs(2), subscription.next())
        .await
        .expect("restored WebSocket subscription notification timed out")
        .expect("restored WebSocket subscription ended")?;

    // Assert
    assert_eq!(notification.route, "notice://realm/app/event");
    assert_eq!(notification.body, b"restored");
    assert!(token_calls.load(Ordering::SeqCst) >= 2);
    client.close().await?;
    server.await.expect("server task panicked")?;
    Ok(())
}

async fn run_reconnect_server(
    listener: TcpListener,
    drop_first_rx: oneshot::Receiver<()>,
) -> std::io::Result<()> {
    let (mut first, _) = listener.accept().await?;
    assert_eq!(read_frame(&mut first).await?.0, CONNECT);
    assert_eq!(read_frame(&mut first).await?.0, NOTICE_SUBSCRIBE);
    write_frame(&mut first, NOTICE_SUBSCRIBE, &subscribe_response(11)).await?;
    let _ = drop_first_rx.await;
    first.shutdown().await?;

    let (mut second, _) = listener.accept().await?;
    assert_eq!(read_frame(&mut second).await?.0, CONNECT);
    assert_eq!(read_frame(&mut second).await?.0, NOTICE_SUBSCRIBE);
    write_frame(&mut second, NOTICE_SUBSCRIBE, &subscribe_response(29)).await?;
    let mut notification = Vec::new();
    notification.extend_from_slice(&29_u64.to_be_bytes());
    put_bytes(&mut notification, b"notice://realm/app/event");
    put_bytes(&mut notification, b"restored");
    write_frame(&mut second, NOTICE_NOTIFY, &notification).await?;
    let _ = read_frame(&mut second).await;
    Ok(())
}

async fn run_websocket_reconnect_server(
    listener: TcpListener,
    drop_first_rx: oneshot::Receiver<()>,
) -> std::io::Result<()> {
    let (first_stream, _) = listener.accept().await?;
    let mut first = tokio_tungstenite::accept_async(first_stream)
        .await
        .map_err(std::io::Error::other)?;
    assert_eq!(read_websocket_frame(&mut first).await?.0, CONNECT);
    assert_eq!(read_websocket_frame(&mut first).await?.0, NOTICE_SUBSCRIBE);
    write_websocket_frame(&mut first, NOTICE_SUBSCRIBE, &subscribe_response(11)).await?;
    let _ = drop_first_rx.await;
    drop(first);

    let (second_stream, _) = listener.accept().await?;
    let mut second = tokio_tungstenite::accept_async(second_stream)
        .await
        .map_err(std::io::Error::other)?;
    assert_eq!(read_websocket_frame(&mut second).await?.0, CONNECT);
    assert_eq!(read_websocket_frame(&mut second).await?.0, NOTICE_SUBSCRIBE);
    write_websocket_frame(&mut second, NOTICE_SUBSCRIBE, &subscribe_response(29)).await?;
    let mut notification = Vec::new();
    notification.extend_from_slice(&29_u64.to_be_bytes());
    put_bytes(&mut notification, b"notice://realm/app/event");
    put_bytes(&mut notification, b"restored");
    write_websocket_frame(&mut second, NOTICE_NOTIFY, &notification).await?;
    let _ = second.next().await;
    Ok(())
}

async fn wait_for_reauthenticated(client: &Client) {
    let mut states = client.subscribe_state();
    loop {
        if client.state() == ConnectionState::Authenticated {
            tokio::task::yield_now().await;
            if client.state() == ConnectionState::Authenticated {
                return;
            }
        }
        states.changed().await.expect("client state channel closed");
    }
}

fn subscribe_response(id: u64) -> Vec<u8> {
    let mut payload = vec![0, 1];
    payload.extend_from_slice(&id.to_be_bytes());
    payload
}

fn put_bytes(target: &mut Vec<u8>, value: &[u8]) {
    target.extend_from_slice(
        &u32::try_from(value.len())
            .expect("test payload length")
            .to_be_bytes(),
    );
    target.extend_from_slice(value);
}

async fn read_frame(stream: &mut TcpStream) -> std::io::Result<(u16, Vec<u8>)> {
    let length = stream.read_u32().await? as usize;
    let mut frame = vec![0; length];
    stream.read_exact(&mut frame).await?;
    let (message_type, header_length) = if frame[0] == 0xff {
        (u16::from_be_bytes([frame[1], frame[2]]), 3)
    } else {
        (u16::from(frame[0]), 1)
    };
    Ok((message_type, frame[header_length + 2..].to_vec()))
}

async fn write_frame(
    stream: &mut TcpStream,
    message_type: u16,
    payload: &[u8],
) -> std::io::Result<()> {
    let mut frame = vec![0xff];
    frame.extend_from_slice(&message_type.to_be_bytes());
    frame.extend_from_slice(
        &u16::try_from(payload.len())
            .expect("test payload length")
            .to_be_bytes(),
    );
    frame.extend_from_slice(payload);
    stream
        .write_all(
            &u32::try_from(frame.len())
                .expect("test frame length")
                .to_be_bytes(),
        )
        .await?;
    stream.write_all(&frame).await?;
    stream.flush().await
}

async fn read_websocket_frame(
    socket: &mut tokio_tungstenite::WebSocketStream<TcpStream>,
) -> std::io::Result<(u16, Vec<u8>)> {
    loop {
        match socket.next().await {
            Some(Ok(Message::Binary(frame))) => return Ok(decode_frame(&frame)),
            Some(Ok(_)) => {}
            Some(Err(error)) => return Err(std::io::Error::other(error)),
            None => return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof)),
        }
    }
}

async fn write_websocket_frame(
    socket: &mut tokio_tungstenite::WebSocketStream<TcpStream>,
    message_type: u16,
    payload: &[u8],
) -> std::io::Result<()> {
    socket
        .send(Message::Binary(encode_frame(message_type, payload).into()))
        .await
        .map_err(std::io::Error::other)
}

fn encode_frame(message_type: u16, payload: &[u8]) -> Vec<u8> {
    let mut frame = vec![0xff];
    frame.extend_from_slice(&message_type.to_be_bytes());
    frame.extend_from_slice(
        &u16::try_from(payload.len())
            .expect("test payload length")
            .to_be_bytes(),
    );
    frame.extend_from_slice(payload);
    frame
}

fn decode_frame(frame: &[u8]) -> (u16, Vec<u8>) {
    let (message_type, header_length) = if frame[0] == 0xff {
        (u16::from_be_bytes([frame[1], frame[2]]), 3)
    } else {
        (u16::from(frame[0]), 1)
    };
    (message_type, frame[header_length + 2..].to_vec())
}
