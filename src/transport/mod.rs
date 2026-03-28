//! Abstract transport interface supporting TCP and WebSocket

pub mod tcp;
pub mod websocket;

use std::io;
use std::time::Duration;

/// Abstract transport trait - implemented by TCP and WebSocket
pub trait Transport: Send + Sync {
    /// Send a single frame (TLV payload without session framing)
    fn send_frame(&mut self, frame: &[u8]) -> io::Result<()>;

    /// Receive a single frame
    fn recv_frame(&mut self) -> io::Result<Vec<u8>>;

    /// Update transport-level read/write timeouts.
    fn set_timeouts(
        &mut self,
        read_timeout: Option<Duration>,
        write_timeout: Option<Duration>,
    ) -> io::Result<()>;

    /// Close the connection gracefully
    fn close(&mut self) -> io::Result<()>;
}

/// Runtime-selected transport
pub enum AnyTransport {
    Tcp(tcp::TcpTransport),
    WebSocket(Box<websocket::WebSocketTransport>),
}

impl Transport for AnyTransport {
    fn send_frame(&mut self, frame: &[u8]) -> io::Result<()> {
        match self {
            AnyTransport::Tcp(t) => t.send_frame(frame),
            AnyTransport::WebSocket(t) => t.send_frame(frame),
        }
    }

    fn recv_frame(&mut self) -> io::Result<Vec<u8>> {
        match self {
            AnyTransport::Tcp(t) => t.recv_frame(),
            AnyTransport::WebSocket(t) => t.recv_frame(),
        }
    }

    fn set_timeouts(
        &mut self,
        read_timeout: Option<Duration>,
        write_timeout: Option<Duration>,
    ) -> io::Result<()> {
        match self {
            AnyTransport::Tcp(t) => t.set_timeouts(read_timeout, write_timeout),
            AnyTransport::WebSocket(t) => t.set_timeouts(read_timeout, write_timeout),
        }
    }

    fn close(&mut self) -> io::Result<()> {
        match self {
            AnyTransport::Tcp(t) => t.close(),
            AnyTransport::WebSocket(t) => t.close(),
        }
    }
}
