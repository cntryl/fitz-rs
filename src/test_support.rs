//! Broker-side transport fixture that echoes each request's wire-2 correlation.

use crate::codec::{decode_frame_header, try_encode_frame};
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

type Pending = Arc<Mutex<HashMap<u16, VecDeque<u64>>>>;

pub(crate) struct TestSession {
    stream: TcpStream,
    pending: Pending,
}

impl TestSession {
    pub(crate) fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            pending: Pending::default(),
        }
    }

    pub(crate) async fn read_frame(&mut self) -> (u16, Vec<u8>) {
        read(&mut self.stream, &self.pending).await
    }

    pub(crate) async fn write_frame(&mut self, kind: u16, payload: &[u8]) {
        write(&mut self.stream, &self.pending, kind, payload).await;
    }

    pub(crate) fn into_split(self) -> (TestReadHalf, TestWriteHalf) {
        let (reader, writer) = self.stream.into_split();
        (
            TestReadHalf {
                reader,
                pending: self.pending.clone(),
            },
            TestWriteHalf {
                writer,
                pending: self.pending,
            },
        )
    }
}

impl Deref for TestSession {
    type Target = TcpStream;
    fn deref(&self) -> &Self::Target {
        &self.stream
    }
}

impl DerefMut for TestSession {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.stream
    }
}

pub(crate) struct TestReadHalf {
    reader: OwnedReadHalf,
    pending: Pending,
}

impl TestReadHalf {
    pub(crate) async fn read_frame(&mut self) -> (u16, Vec<u8>) {
        read(&mut self.reader, &self.pending).await
    }
}

pub(crate) struct TestWriteHalf {
    writer: OwnedWriteHalf,
    pending: Pending,
}

impl Deref for TestWriteHalf {
    type Target = OwnedWriteHalf;
    fn deref(&self) -> &Self::Target {
        &self.writer
    }
}

impl DerefMut for TestWriteHalf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.writer
    }
}

impl TestWriteHalf {
    pub(crate) async fn write_frame(&mut self, kind: u16, payload: &[u8]) {
        write(&mut self.writer, &self.pending, kind, payload).await;
    }
}

async fn read<R: AsyncRead + Unpin>(reader: &mut R, pending: &Pending) -> (u16, Vec<u8>) {
    let length = usize::try_from(reader.read_u32().await.unwrap()).unwrap();
    let mut frame = vec![0; length];
    reader.read_exact(&mut frame).await.unwrap();
    let header = decode_frame_header(&frame).unwrap();
    if header.correlation_id != 0 {
        pending
            .lock()
            .entry(header.message_type)
            .or_default()
            .push_back(header.correlation_id);
    }
    (header.message_type, frame[header.payload_start..].to_vec())
}

async fn write<W: AsyncWrite + Unpin>(
    writer: &mut W,
    pending: &Pending,
    kind: u16,
    payload: &[u8],
) {
    let correlation = {
        let mut pending = pending.lock();
        pending
            .get_mut(&kind)
            .and_then(|requests| {
                if kind == 400 && payload.starts_with(&[0, 2]) {
                    requests.front().copied()
                } else {
                    requests.pop_front()
                }
            })
            .unwrap_or(0)
    };
    let response_final = correlation != 0 && !(kind == 400 && payload.starts_with(&[0, 2]));
    let flags = if response_final {
        crate::codec::RESPONSE_FINAL
    } else {
        0
    };
    let frame = try_encode_frame(kind, flags, correlation, payload).unwrap();
    writer
        .write_u32(u32::try_from(frame.len()).unwrap())
        .await
        .unwrap();
    writer.write_all(&frame).await.unwrap();
    writer.flush().await.unwrap();
}
