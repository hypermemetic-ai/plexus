//! A channel: whole frames, as bytes, both ways.

use std::io;
use std::pin::Pin;

use futures::channel::mpsc;
use futures::{Sink, SinkExt, Stream, StreamExt};

/// One end of something that carries frames: a pair in memory, a pipe, a
/// WebSocket. Each `Vec<u8>` is one whole frame; the channel keeps them
/// whole and in order.
pub struct Channel {
    pub send: Pin<Box<dyn Sink<Vec<u8>, Error = io::Error> + Send>>,
    pub receive: Pin<Box<dyn Stream<Item = io::Result<Vec<u8>>> + Send>>,
}

impl Channel {
    pub fn new(
        send: impl Sink<Vec<u8>, Error = io::Error> + Send + 'static,
        receive: impl Stream<Item = io::Result<Vec<u8>>> + Send + 'static,
    ) -> Channel {
        Channel { send: Box::pin(send), receive: Box::pin(receive) }
    }
}

/// Two ends of a channel in one process. What one sends the other receives.
pub fn pair() -> (Channel, Channel) {
    let (a_send, b_receive) = mpsc::unbounded();
    let (b_send, a_receive) = mpsc::unbounded();
    (end(a_send, a_receive), end(b_send, b_receive))
}

/// Write what `frames` gives, flushing whenever nothing more is waiting.
/// Ends, closing the channel, when every sender of `frames` is gone.
pub(crate) async fn pump(
    mut send: Pin<Box<dyn Sink<Vec<u8>, Error = io::Error> + Send>>,
    mut frames: mpsc::UnboundedReceiver<Vec<u8>>,
) -> io::Result<()> {
    while let Some(frame) = frames.next().await {
        send.feed(frame).await?;
        while let Ok(frame) = frames.try_recv() {
            send.feed(frame).await?;
        }
        send.flush().await?;
    }
    send.close().await
}

fn end(send: mpsc::UnboundedSender<Vec<u8>>, receive: mpsc::UnboundedReceiver<Vec<u8>>) -> Channel {
    let send = send.sink_map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe));
    Channel::new(send, receive.map(Ok))
}
