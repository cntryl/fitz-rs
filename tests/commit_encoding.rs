use cntryl_fitz::client_domains::stream::StreamCommitMode;
use cntryl_fitz::{Client, KvDurability, Result, TransactionMode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
async fn should_encode_explicit_commit_persistence() -> Result<()> {
    // Arrange
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let client = Client::anonymous(format!("tcp://{}", listener.local_addr()?)).build()?;
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        assert_eq!(read_frame(&mut socket).await?.0, 1);
        write_frame(&mut socket, 4, &[0, 1, 0, 0, 0, 0]).await?;
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
