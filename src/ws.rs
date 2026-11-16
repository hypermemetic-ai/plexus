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

/// A channel to the WebSocket at `url` (`ws://127.0.0.1:4455/`).
pub async fn connect(url: &str) -> io::Result<Channel> {
    let (socket, _) = tokio_tungstenite::connect_async(url).await.map_err(broken)?;
    Ok(channel(socket))
}

/// A channel over a connection just accepted: the WebSocket handshake, then frames.
pub async fn accept(stream: TcpStream) -> io::Result<Channel> {
    stream.set_nodelay(true)?;
    Ok(channel(tokio_tungstenite::accept_async(stream).await.map_err(broken)?))
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
