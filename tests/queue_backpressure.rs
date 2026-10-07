use cntryl_fitz::{Client, FitzError, Result};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
async fn should_retain_reservation_given_capacity_rejection_when_completing() -> Result<()> {
    // Arrange
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let client = Client::builder(format!("tcp://{}", listener.local_addr()?), || async {
        Ok("token".into())
    })
    .request_timeout(Duration::from_secs(2))
    .build()?;
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        assert_eq!(read_frame(&mut socket).await?.0, 1);
        write_frame(&mut socket, 4, &[0, 1, 0, 0, 0, 0]).await?;
        assert_eq!(read_frame(&mut socket).await?.0, 202);
        let mut reservation = vec![0];
        reservation.extend_from_slice(&1_u32.to_be_bytes());
        reservation.extend_from_slice(&7_u64.to_be_bytes());
        reservation.extend_from_slice(&11_u64.to_be_bytes());
        reservation.extend_from_slice(&0_u32.to_be_bytes());
        write_frame(&mut socket, 202, &reservation).await?;
        let first = read_frame(&mut socket).await?;
        assert_eq!(first.0, 204);
        let mut rejection = vec![1];
        rejection.extend_from_slice(&4005_u32.to_be_bytes());
        rejection.extend_from_slice(&0_u32.to_be_bytes());
        write_frame(&mut socket, 204, &rejection).await?;
        let second = read_frame(&mut socket).await?;
        assert_eq!(second, first);
        write_frame(&mut socket, 204, &[0]).await?;
        let _ = read_frame(&mut socket).await;
        Ok::<_, std::io::Error>(())
    });
    client.connect().await?;
    let item = client
        .queue()?
        .reserve("queue://realm/app/jobs", 30, 1, None)
        .await?
        .remove(0);
    // Act
    let error = item.complete().await.unwrap_err();
    // Assert
    assert!(matches!(error, FitzError::Domain { code: 4005, .. }));
    item.complete().await?;
    assert!(matches!(item.complete().await, Err(FitzError::StaleHandle)));
    client.close().await?;
    server.await.expect("server task panicked")?;
    Ok(())
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
