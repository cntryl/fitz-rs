mod support;
use cntryl_fitz::{Client, FitzError, Result};
use std::time::Duration;
use support::Session;
use tokio::net::TcpListener;

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
        let (socket, _) = listener.accept().await?;
        let mut socket = Session::new(socket);
        assert_eq!(read_frame(&mut socket).await?.0, 1);
        write_frame(&mut socket, 2, &support::hello()).await?;
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
        rejection.extend_from_slice(&4005_u16.to_be_bytes());
        rejection.extend_from_slice(&[5, 0]);
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
    assert!(matches!(error, FitzError::DomainWire { code: 4005, .. }));
    item.complete().await?;
    assert!(matches!(item.complete().await, Err(FitzError::StaleHandle)));
    client.close().await?;
    server.await.expect("server task panicked")?;
    Ok(())
}

async fn read_frame(stream: &mut Session) -> std::io::Result<(u16, Vec<u8>)> {
    stream.read_frame().await
}
async fn write_frame(stream: &mut Session, kind: u16, payload: &[u8]) -> std::io::Result<()> {
    stream.write_frame(kind, payload).await
}
