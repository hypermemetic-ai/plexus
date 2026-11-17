//! A channel: whole frames, as bytes, both ways.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

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

/// Two ends of a channel in one process. What one sends the other receives,
/// once it is flushed (`send` flushes).
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
    #[cfg(feature = "trace")]
    let mut batch = crate::trace::Batch::default();
    while let Some(frame) = frames.next().await {
        #[cfg(feature = "trace")]
        batch.taken(&frame);
        send.feed(frame).await?;
        while let Ok(frame) = frames.try_recv() {
            #[cfg(feature = "trace")]
            batch.taken(&frame);
            send.feed(frame).await?;
        }
        send.flush().await?;
        #[cfg(feature = "trace")]
        batch.flushed(|_| Some(crate::trace::now()), &[crate::trace::Stage::Flushed]);
    }
    send.close().await
}

fn end(send: mpsc::UnboundedSender<Vec<Vec<u8>>>, receive: mpsc::UnboundedReceiver<Vec<Vec<u8>>>) -> Channel {
    let send = Held { send, held: Vec::new() };
    Channel::new(send, receive.flat_map(futures::stream::iter).map(Ok))
}

/// A pair's sending end: frames are held until a flush and go over
/// together, so the other end is woken once a flush, not once a frame.
struct Held {
    send: mpsc::UnboundedSender<Vec<Vec<u8>>>,
    held: Vec<Vec<u8>>,
}

impl Sink<Vec<u8>> for Held {
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, frame: Vec<u8>) -> io::Result<()> {
        self.held.push(frame);
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.held.is_empty() {
            let held = std::mem::take(&mut self.held);
            self.send.unbounded_send(held).map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;
        }
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let flushed = self.as_mut().poll_flush(cx);
        self.send.close_channel();
        flushed
    }
}
