//! A channel: whole frames, as shared bytes, both ways.

use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use futures::channel::mpsc;
use futures::{Sink, SinkExt, Stream, StreamExt};

use crate::lock;

/// One end of something that carries frames: a pair in memory, a pipe, a
/// WebSocket. Each `Bytes` is one whole frame; the channel keeps them whole
/// and in order. A frame sent may be a slice of a buffer that holds others,
/// and a frame received may be a slice of the buffer it was read into:
/// neither end copies a frame to make it its own.
pub struct Channel {
    pub send: Pin<Box<dyn Sink<Bytes, Error = io::Error> + Send>>,
    pub receive: Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send>>,
}

impl Channel {
    pub fn new(
        send: impl Sink<Bytes, Error = io::Error> + Send + 'static,
        receive: impl Stream<Item = io::Result<Bytes>> + Send + 'static,
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

/// Frames waiting for the pump, written one after another into one buffer,
/// so a frame costs no allocation of its own and the pump takes every frame
/// waiting at once. Clones are senders; the pump ends once every one is
/// gone and what they wrote is written.
pub(crate) struct Out(Arc<Mutex<Queue>>);

#[derive(Default)]
struct Queue {
    buf: Vec<u8>,
    /// Where each frame in `buf` ends.
    ends: Vec<usize>,
    senders: usize,
    /// The pump is gone: nothing more is taken.
    closed: bool,
    /// The pump, waiting for a frame.
    pump: Option<Waker>,
}

/// What a pump takes the frames from.
pub(crate) struct Taken(Arc<Mutex<Queue>>);

/// Senders, and what their pump takes from.
pub(crate) fn out() -> (Out, Taken) {
    let queue = Arc::new(Mutex::new(Queue { senders: 1, ..Queue::default() }));
    (Out(queue.clone()), Taken(queue))
}

/// The frames of one push, each written by its `put`.
pub(crate) struct Frames<'a> {
    buf: &'a mut Vec<u8>,
    ends: &'a mut Vec<usize>,
}

impl Frames<'_> {
    /// One frame, appended by `write`.
    pub(crate) fn put(&mut self, write: impl FnOnce(&mut Vec<u8>)) {
        write(self.buf);
        self.ends.push(self.buf.len());
    }
}

impl Out {
    /// Queue the frames `frames` puts, together: the pump takes all of them
    /// in one batch. `false` if the pump is gone.
    pub(crate) fn push(&self, frames: impl FnOnce(&mut Frames<'_>)) -> bool {
        let mut queue = lock(&self.0);
        if queue.closed {
            return false;
        }
        let Queue { buf, ends, .. } = &mut *queue;
        frames(&mut Frames { buf, ends });
        let pump = queue.pump.take();
        drop(queue);
        if let Some(pump) = pump {
            pump.wake();
        }
        true
    }

    /// One frame, written by `write`.
    pub(crate) fn send(&self, write: impl FnOnce(&mut Vec<u8>)) -> bool {
        self.push(|frames| frames.put(write))
    }
}

impl Clone for Out {
    fn clone(&self) -> Out {
        lock(&self.0).senders += 1;
        Out(self.0.clone())
    }
}

impl Drop for Out {
    fn drop(&mut self) {
        let mut queue = lock(&self.0);
        queue.senders -= 1;
        let pump = if queue.senders == 0 { queue.pump.take() } else { None };
        drop(queue);
        if let Some(pump) = pump {
            pump.wake();
        }
    }
}

impl Taken {
    /// Every frame waiting, and where each ends; `None` once every sender
    /// is gone and nothing waits.
    fn poll_take(&self, cx: &mut Context<'_>, ends: &mut Vec<usize>) -> Poll<Option<Vec<u8>>> {
        let mut queue = lock(&self.0);
        if queue.ends.is_empty() {
            if queue.senders == 0 {
                return Poll::Ready(None);
            }
            queue.pump = Some(cx.waker().clone());
            return Poll::Pending;
        }
        ends.clear();
        std::mem::swap(ends, &mut queue.ends);
        // The next batch's buffer is made as large as this one was, up to a
        // bound, so it is not grown frame by frame.
        let room = queue.buf.len().min(1 << 16);
        Poll::Ready(Some(std::mem::replace(&mut queue.buf, Vec::with_capacity(room))))
    }
}

impl Drop for Taken {
    fn drop(&mut self) {
        let mut queue = lock(&self.0);
        queue.closed = true;
        queue.buf = Vec::new();
        queue.ends = Vec::new();
    }
}

/// Write what `frames` gives, a batch at a time: every frame waiting is fed
/// to the channel, as a slice of the one buffer they were written into, then
/// flushed. Ends, closing the channel, when every sender is gone.
pub(crate) async fn pump(mut send: Pin<Box<dyn Sink<Bytes, Error = io::Error> + Send>>, frames: Taken) -> io::Result<()> {
    #[cfg(feature = "trace")]
    let mut batch = crate::trace::Batch::default();
    let mut ends = Vec::new();
    while let Some(buf) = futures::future::poll_fn(|cx| frames.poll_take(cx, &mut ends)).await {
        let mut buf = Bytes::from(buf);
        let mut at = 0;
        for &end in &ends {
            let frame = buf.split_to(end - at);
            at = end;
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

fn end(send: mpsc::UnboundedSender<Vec<Bytes>>, receive: mpsc::UnboundedReceiver<Vec<Bytes>>) -> Channel {
    let send = Held { send, held: Vec::new() };
    Channel::new(send, receive.flat_map(futures::stream::iter).map(Ok))
}

/// A pair's sending end: frames are held until a flush and go over
/// together, so the other end is woken once a flush, not once a frame.
struct Held {
    send: mpsc::UnboundedSender<Vec<Bytes>>,
    held: Vec<Bytes>,
}

impl Sink<Bytes> for Held {
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, frame: Bytes) -> io::Result<()> {
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
