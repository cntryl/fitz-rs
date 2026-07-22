//! WebSocket transport with binary message frames

use super::Transport;
use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;
use std::io;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::tungstenite::Message;

pub struct WebSocketTransport {
    ws: tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
    rt: tokio::runtime::Runtime,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
}

impl WebSocketTransport {
    /// Connect to a WebSocket server
    pub fn connect(url: &str) -> io::Result<Self> {
        let rt = tokio::runtime::Runtime::new()?;

        let ws = rt.block_on(async {
            match tokio_tungstenite::connect_async(url).await {
                Ok((ws, _)) => Ok(ws),
                Err(e) => Err(io::Error::new(io::ErrorKind::ConnectionRefused, e)),
            }
        })?;

        Ok(Self {
            ws,
            rt,
            read_timeout: Some(Duration::from_secs(30)),
            write_timeout: Some(Duration::from_secs(10)),
        })
    }

    pub fn set_timeouts(
        &mut self,
        read_timeout: Option<Duration>,
        write_timeout: Option<Duration>,
    ) {
        self.read_timeout = read_timeout;
        self.write_timeout = write_timeout;
    }
}

impl Transport for WebSocketTransport {
    fn send_frame(&mut self, frame: &[u8]) -> io::Result<()> {
        let timeout = self.write_timeout;
        let payload = frame.to_vec();
        self.rt.block_on(async {
            // WebSocket: send as binary message (no length prefix)
            match timeout {
                Some(duration) => {
                    time::timeout(duration, self.ws.send(Message::Binary(payload.into())))
                        .await
                        .map_err(|_| {
                            io::Error::new(io::ErrorKind::TimedOut, "WebSocket send timed out")
                        })?
                        .map_err(io::Error::other)
                }
                None => self
                    .ws
                    .send(Message::Binary(payload.into()))
                    .await
                    .map_err(io::Error::other),
            }
        })
    }

    fn recv_frame(&mut self) -> io::Result<Vec<u8>> {
        let timeout = self.read_timeout;
        self.rt.block_on(async {
            let next_message = async {
                loop {
                    match self.ws.next().await {
                        Some(Ok(Message::Binary(data))) => return Ok(data.to_vec()),
                        Some(Ok(Message::Text(_))) => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "Text frames not supported",
                            ));
                        }
                        Some(Ok(Message::Close(_))) => {
                            return Err(io::Error::new(
                                io::ErrorKind::ConnectionAborted,
                                "Server closed connection",
                            ));
                        }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => return Err(io::Error::other(e)),
                        None => {
                            return Err(io::Error::new(
                                io::ErrorKind::ConnectionReset,
                                "Connection closed",
                            ));
                        }
                    }
                }
            };

            match timeout {
                Some(duration) => time::timeout(duration, next_message).await.map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "WebSocket receive timed out")
                })?,
                None => next_message.await,
            }
        })
    }

    fn set_timeouts(
        &mut self,
        read_timeout: Option<Duration>,
        write_timeout: Option<Duration>,
    ) -> io::Result<()> {
        WebSocketTransport::set_timeouts(self, read_timeout, write_timeout);
        Ok(())
    }

    fn close(&mut self) -> io::Result<()> {
        self.rt
            .block_on(async { self.ws.close(None).await.map_err(io::Error::other) })
    }
}
