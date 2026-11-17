//! A socket in memory for `ws.rs`'s reader: what it reads comes in chunks
//! of the input's choosing, at times not ready; what it writes is kept.
//! And the memory the reader holds, counted.

use std::alloc::{GlobalAlloc, Layout, System};
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::StreamExt;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pub struct Wire {
    pub input: Vec<u8>,
    at: usize,
    /// The size of each read in turn, 0 for not ready (once: the read
    /// after one is never 0).
    pub chunks: Vec<u8>,
    turn: usize,
    waited: bool,
    pub written: Vec<u8>,
}

impl Wire {
    pub fn new(input: Vec<u8>, chunks: Vec<u8>) -> Wire {
        Wire { input, at: 0, chunks: if chunks.is_empty() { vec![255] } else { chunks }, turn: 0, waited: false, written: Vec::new() }
    }
}

impl AsyncRead for Wire {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let c = self.chunks[self.turn % self.chunks.len()] as usize;
        self.turn += 1;
        if c == 0 && !std::mem::replace(&mut self.waited, true) {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.waited = false;
        let c = c.max(1);
        // Small reads mostly, and now and then large ones; a large input in
        // no more than a few thousand, so that a run stays short.
        let n = (if c < 200 { c } else { (c - 199) << 12 }).max(self.input.len() >> 12);
        let n = n.min(self.input.len() - self.at).min(buf.remaining());
        buf.put_slice(&self.input[self.at..self.at + n]);
        self.at += n;
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for Wire {
    fn poll_write(mut self: Pin<&mut Self>, _: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        self.written.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// The most the reader may hold: a frame of 16 MiB being read, a message
/// of 64 MiB in fragments, a turn's 1 MiB of messages not handed on, and a
/// read's 64 KiB, each once; and room for a buffer that grew by doubling.
pub const MOST_HELD: usize = (16 << 20) * 2 + (64 << 20) + (1 << 20) + (64 << 10) * 2;

struct Counted;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counted {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let live = LIVE.fetch_add(layout.size(), Relaxed) + layout.size();
        PEAK.fetch_max(live, Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if size > layout.size() {
            let live = LIVE.fetch_add(size - layout.size(), Relaxed) + size - layout.size();
            PEAK.fetch_max(live, Relaxed);
        } else {
            LIVE.fetch_sub(layout.size() - size, Relaxed);
        }
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[global_allocator]
static COUNTED: Counted = Counted;

static MOST_SEEN: AtomicUsize = AtomicUsize::new(0);

/// Read `wire` as one end would, each message handed to `each` and let go;
/// the error that ended the reading, if one did, and what was written.
/// Panics if the reader held more than [`MOST_HELD`].
pub fn read(wire: Wire, server: bool, mut each: impl FnMut(Bytes)) -> (Option<io::Error>, Vec<u8>) {
    let base = LIVE.load(Relaxed);
    PEAK.store(base, Relaxed);
    let wire = Shared::new(wire);
    let out = wire.clone();
    let mut channel = plexus::ws::handshaken(wire, server);
    let failed = futures::executor::block_on(async {
        loop {
            match channel.receive.next().await {
                Some(Ok(message)) => each(message),
                Some(Err(e)) => return Some(e),
                None => return None,
            }
        }
    });
    let held = PEAK.load(Relaxed) - base;
    if held > MOST_SEEN.fetch_max(held, Relaxed) {
        eprintln!("most held so far: {held} bytes");
    }
    assert!(held <= MOST_HELD, "the reader held {held} bytes");
    drop(channel);
    (failed, out.written())
}

/// The wire, kept by the reader and by whoever wants what was written.
#[derive(Clone)]
struct Shared(std::sync::Arc<std::sync::Mutex<Wire>>);

impl Shared {
    fn new(wire: Wire) -> Shared {
        Shared(std::sync::Arc::new(std::sync::Mutex::new(wire)))
    }
    fn written(&self) -> Vec<u8> {
        std::mem::take(&mut self.0.lock().unwrap().written)
    }
}

impl AsyncRead for Shared {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.0.lock().unwrap()).poll_read(cx, buf)
    }
}

impl AsyncWrite for Shared {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.0.lock().unwrap()).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
