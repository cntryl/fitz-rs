use std::collections::{HashMap, VecDeque};
use std::ops::{Deref, DerefMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub fn hello() -> Vec<u8> {
    let mut payload = 2_u16.to_be_bytes().to_vec();
    payload.extend_from_slice(&0_u32.to_be_bytes());
    payload.extend_from_slice(&1_048_576_u32.to_be_bytes());
    payload.extend_from_slice(&64_u32.to_be_bytes());
    payload.extend_from_slice(&1_u64.to_be_bytes());
    payload
}

pub struct Session {
    stream: TcpStream,
    pending: HashMap<u16, VecDeque<u64>>,
}
impl Session {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            pending: HashMap::new(),
        }
    }
    pub async fn read_frame(&mut self) -> std::io::Result<(u16, Vec<u8>)> {
        let length = usize::try_from(self.stream.read_u32().await?).unwrap();
        let mut frame = vec![0; length];
        self.stream.read_exact(&mut frame).await?;
        let (kind, correlation, payload) = decode(&frame);
        if correlation != 0 {
            self.pending.entry(kind).or_default().push_back(correlation);
        }
        Ok((kind, payload))
    }
    pub async fn write_frame(&mut self, kind: u16, payload: &[u8]) -> std::io::Result<()> {
        let correlation = self
            .pending
            .get_mut(&kind)
            .and_then(VecDeque::pop_front)
            .unwrap_or(0);
        let frame = encode(kind, correlation, payload);
        self.stream
            .write_u32(u32::try_from(frame.len()).unwrap())
            .await?;
        self.stream.write_all(&frame).await?;
        self.stream.flush().await
    }
}
impl Deref for Session {
    type Target = TcpStream;
    fn deref(&self) -> &TcpStream {
        &self.stream
    }
}
impl DerefMut for Session {
    fn deref_mut(&mut self) -> &mut TcpStream {
        &mut self.stream
    }
}

pub fn encode(kind: u16, correlation: u64, payload: &[u8]) -> Vec<u8> {
    let mut frame = kind.to_be_bytes().to_vec();
    frame.push(0);
    frame.extend_from_slice(&correlation.to_be_bytes());
    frame.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}
pub fn decode(frame: &[u8]) -> (u16, u64, Vec<u8>) {
    assert!(frame.len() >= 15);
    assert_eq!(frame[2], 0);
    let kind = u16::from_be_bytes(frame[..2].try_into().unwrap());
    let correlation = u64::from_be_bytes(frame[3..11].try_into().unwrap());
    let length = usize::try_from(u32::from_be_bytes(frame[11..15].try_into().unwrap())).unwrap();
    assert_eq!(frame.len(), 15 + length);
    (kind, correlation, frame[15..].to_vec())
}
