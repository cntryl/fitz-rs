mod support;
use cntryl_fitz::client_domains::stream::StreamCommitMode;
use cntryl_fitz::{Client, KvDurability, Result, TransactionMode};
use support::Session;
use tokio::net::TcpListener;

#[tokio::test]
async fn should_encode_explicit_commit_persistence() -> Result<()> {
    // Arrange
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let client = Client::anonymous(format!("tcp://{}", listener.local_addr()?)).build()?;
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await?;
        let mut socket = Session::new(socket);
        assert_eq!(read_frame(&mut socket).await?.0, 1);
        write_frame(&mut socket, 2, &support::hello()).await?;
        for wire in [0, 1] {
            let mut begin = 10_u32.to_be_bytes().to_vec();
            begin.extend_from_slice(b"kv://r/a/x");
            begin.push(1);
            assert_eq!(read_frame(&mut socket).await?, (100, begin.clone()));
            let mut response = vec![0];
            response.extend_from_slice(&7_u64.to_be_bytes());
            write_frame(&mut socket, 100, &response).await?;
            let mut commit = 7_u64.to_be_bytes().to_vec();
            begin.pop();
            commit.extend_from_slice(&begin);
            commit.push(wire);
            assert_eq!(read_frame(&mut socket).await?, (101, commit));
            write_frame(&mut socket, 101, &[0]).await?;
            assert_eq!(read_frame(&mut socket).await?.0, 600);
            response.extend_from_slice(&0_u32.to_be_bytes());
            write_frame(&mut socket, 600, &response).await?;
            let mut stream_commit = 7_u64.to_be_bytes().to_vec();
            stream_commit.push(wire);
            assert_eq!(read_frame(&mut socket).await?, (602, stream_commit));
            write_frame(&mut socket, 602, &[0, 0, 0, 0, 0]).await?;
        }
        Ok::<_, std::io::Error>(())
    });
    client.connect().await?;
    // Act
    for (kv_mode, stream_mode) in [
        (KvDurability::Buffered, StreamCommitMode::Buffered),
        (KvDurability::Sync, StreamCommitMode::Sync),
    ] {
        client
            .kv()?
            .begin("kv://r/a/x", TransactionMode::ReadWrite)
            .await?
            .commit(kv_mode)
            .await?;
        client
            .stream()?
            .begin("stream://r/a/x", None)
            .await?
            .commit(stream_mode)
            .await?;
    }
    // Assert
    server.await.expect("server task panicked")?;
    client.close().await?;
    Ok(())
}

async fn read_frame(stream: &mut Session) -> std::io::Result<(u16, Vec<u8>)> {
    stream.read_frame().await
}
async fn write_frame(stream: &mut Session, kind: u16, payload: &[u8]) -> std::io::Result<()> {
    stream.write_frame(kind, payload).await
}
