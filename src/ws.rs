//! Frames over a WebSocket, one text message each: what a page's
//! `plexus-rt` speaks.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use futures::{SinkExt, StreamExt, future};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::{self, Message};

use crate::{Channel, Server};

// A channel stamped by `trace` as its frames cross the socket, or the
// channel alone.
#[cfg(feature = "trace")]
macro_rules! traced {
    ($channel:expr, $clock:expr) => { traced::channel($channel, $clock) };
}
#[cfg(not(feature = "trace"))]
macro_rules! traced {
    ($channel:expr, $clock:expr) => { $channel };
}

/// A channel to the WebSocket at `url` (`ws://127.0.0.1:4455/`).
pub async fn connect(url: &str) -> io::Result<Channel> {
    // No Nagle, as `accept`: a credit written while the last write is not
    // yet acknowledged would otherwise wait for the delayed ack (40 ms).
    #[cfg(not(feature = "trace"))]
    let (socket, _) = tokio_tungstenite::connect_async_with_config(url, None, true).await.map_err(broken)?;
    #[cfg(feature = "trace")]
    let ((socket, _), clock) = traced::connect(url).await?;
    Ok(traced!(channel(socket), clock))
}

/// A channel over a connection just accepted: the WebSocket handshake, then frames.
pub async fn accept(stream: TcpStream) -> io::Result<Channel> {
    stream.set_nodelay(true)?;
    #[cfg(feature = "trace")]
    let (stream, clock) = crate::trace::Socket::new(stream);
    Ok(traced!(channel(tokio_tungstenite::accept_async(stream).await.map_err(broken)?), clock))
}

/// Serve `server` to every connection `listener` accepts, each on a task of its own.
pub async fn listen(listener: TcpListener, server: Arc<Server>) -> io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let server = server.clone();
        tokio::spawn(async move {
            if let Ok(channel) = accept(stream).await {
                let _ = server.serve(channel).await;
            }
        });
    }
}

/// Bind `addr` and serve there in the background; the address bound
/// (port 0 picks one).
pub async fn spawn(addr: SocketAddr, server: Arc<Server>) -> io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tokio::spawn(listen(listener, server));
    Ok(bound)
}

fn channel<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(socket: WebSocketStream<S>) -> Channel {
    let (send, receive) = socket.split();
    let send = send.sink_map_err(broken).with(|frame: Vec<u8>| {
        future::ready(String::from_utf8(frame).map(|text| Message::Text(text.into())).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e)))
    });
    let receive = receive.filter_map(|message| {
        future::ready(match message {
            Ok(Message::Text(text)) => Some(Ok(text.as_bytes().to_vec())),
            Ok(Message::Binary(bytes)) => Some(Ok(bytes.to_vec())),
            Ok(_) => None,
            Err(tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed) => None,
            Err(e) => Some(Err(broken(e))),
        })
    });
    Channel::new(send, receive)
}

fn broken(e: tungstenite::Error) -> io::Error {
    match e {
        tungstenite::Error::Io(e) => e,
        e => io::Error::new(io::ErrorKind::BrokenPipe, e),
    }
}

/// A WebSocket's frames stamped as they are written to its socket and read
/// from it.
#[cfg(feature = "trace")]
mod traced {
    use std::io;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};

    use futures::{Sink, StreamExt};
    use tokio_tungstenite::WebSocketStream;
    use tokio_tungstenite::tungstenite::handshake::client::Response;

    use super::broken;
    use crate::Channel;
    use crate::trace::{self, Batch, Clock, Frames, Socket, Stage};

    pub async fn connect(url: &str) -> io::Result<((WebSocketStream<Socket>, Response), Arc<Clock>)> {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let request = url.into_client_request().map_err(broken)?;
        let uri = request.uri();
        let address = format!("{}:{}", uri.host().unwrap_or("127.0.0.1"), uri.port_u16().unwrap_or(80));
        let stream = tokio::net::TcpStream::connect(address).await?;
        stream.set_nodelay(true)?;
        let (socket, clock) = Socket::new(stream);
        Ok((tokio_tungstenite::client_async(request, socket).await.map_err(broken)?, clock))
    }

    pub fn channel(channel: Channel, clock: Arc<Clock>) -> Channel {
        let Channel { send, receive } = channel;
        let read = clock.clone();
        let mut frames = Frames::default();
        let receive = receive.inspect(move |frame| {
            if let Ok(frame) = frame
                && trace::on()
                && let Some(key) = frames.key(frame)
            {
                if let Some((entered, returned)) = read.read() {
                    trace::stamp_at(key, Stage::WsReadEntry, entered);
                    trace::stamp_at(key, Stage::WsReadReturn, returned);
                }
                trace::stamp(key, Stage::WsMessage);
            }
        });
        Channel::new(Written { send, clock, batch: Batch::default() }, receive)
    }

    /// The sending half: a frame's write to the socket is stamped once the
    /// flush that writes it is done.
    struct Written {
        send: Pin<Box<dyn Sink<Vec<u8>, Error = io::Error> + Send>>,
        clock: Arc<Clock>,
        batch: Batch,
    }

    impl Sink<Vec<u8>> for Written {
        type Error = io::Error;

        fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.send.as_mut().poll_ready(cx)
        }

        fn start_send(mut self: Pin<&mut Self>, frame: Vec<u8>) -> io::Result<()> {
            // Keyed again here, not stamped: the pump stamped it taken.
            self.batch.keep(&frame);
            self.send.as_mut().start_send(frame)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            let flushed = self.send.as_mut().poll_flush(cx);
            if flushed.is_ready() {
                let (entered, returned) = self.clock.written();
                self.batch.flushed(|stage| if stage == Stage::WsWrite { entered } else { returned }, &[Stage::WsWrite, Stage::WsWritten]);
            }
            flushed
        }

        fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.send.as_mut().poll_close(cx)
        }
    }
}
