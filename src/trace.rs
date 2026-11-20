//! A call stamped as it passes each stage, on both ends (feature `trace`).
//! A benchmark's tool, not an API: `benches/trace.rs` reads it.
//!
//! A stamp is a key, a stage, the thread and the time. The key is the call's
//! id and a slot: 0 for the call itself and its open, `n` for its `n`th item
//! (from 1), [`END`] for its end. Each thread keeps its own stamps, so a
//! stamp takes no lock another thread holds; [`stop`] gathers them.

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use crate::lock;

/// The slot of a call's end frame.
pub const END: u32 = u32::MAX;

/// A call, and which of its frames: see the module's doc.
pub type Key = (u32, u32);

/// Where a call or an item was when it was stamped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Stage {
    /// `Connection::call` entered.
    Called,
    /// Open written and queued to the driver.
    OpenQueued,
    /// Frame taken by the writing task.
    Taken,
    /// WebSocket frame made, first `write()` entered.
    WsWrite,
    /// The last `write()` returned.
    WsWritten,
    /// The flush that carries it returned.
    Flushed,
    /// `read()` entered by the woken reader.
    WsReadEntry,
    /// `read()` returned the bytes.
    WsReadReturn,
    /// TLS 1.3 application record encryption entered.
    TlsSealEntry,
    /// TLS 1.3 application record encryption finished.
    TlsSealReturn,
    /// TLS 1.3 application record decryption entered.
    TlsOpenEntry,
    /// TLS 1.3 application record decryption finished.
    TlsOpenReturn,
    /// One TCP write poll that accepted ciphertext entered.
    TlsTcpWriteEntry,
    /// One TCP write poll that accepted ciphertext finished.
    TlsTcpWriteReturn,
    /// One TCP read poll that returned ciphertext entered.
    TlsTcpReadEntry,
    /// One TCP read poll that returned ciphertext finished.
    TlsTcpReadReturn,
    /// WebSocket message read.
    WsMessage,
    /// Open read by the server loop.
    Read,
    /// Open decoded, call handed to run.
    Started,
    /// Call future first polled.
    Running,
    /// Credit in hand, next item asked for.
    Asked,
    /// Method found, parameters read, job boxed.
    JobBoxed,
    /// Job sent, activation thread woken.
    JobSent,
    /// Job taken on the activation thread.
    JobTaken,
    /// Job future first polled.
    JobPolled,
    /// Method ran, answer serialized.
    MethodDone,
    /// Answer sent, server task woken.
    AnswerSent,
    /// Item made by the method.
    ItemMade,
    /// Item serialized.
    ItemSerialized,
    /// Item put in the run shared with the server.
    ItemPushed,
    /// The server task woken, if it waited.
    ItemSent,
    /// Answer in hand on the server task.
    Answered,
    /// Frame written and queued to the pump.
    Queued,
    /// Frame read by the driver.
    Received,
    /// Event added to the call run.
    Delivered,
    /// Run handed into the call inbox.
    InInbox,
    /// Caller task woken.
    Woke,
    /// Item taken by the caller.
    Took,
    /// Call returns its answer.
    Returned,
}

/// One stamp.
#[derive(Debug, Clone, Copy)]
pub struct Record {
    pub key: Key,
    pub stage: Stage,
    /// The index of the thread it was made on: [`threads`] names it.
    pub thread: u16,
    pub at: Instant,
}

static ON: AtomicBool = AtomicBool::new(false);

type Kept = Arc<Mutex<Vec<Record>>>;

/// Every thread's stamps, and its name.
static THREADS: OnceLock<Mutex<Vec<(String, Kept)>>> = OnceLock::new();

fn threads_kept() -> &'static Mutex<Vec<(String, Kept)>> {
    THREADS.get_or_init(Mutex::default)
}

thread_local! {
    static MINE: (u16, Kept) = {
        let kept = Kept::default();
        let mut all = lock(threads_kept());
        let name = std::thread::current().name().unwrap_or("unnamed").to_owned();
        all.push((name, kept.clone()));
        ((all.len() - 1) as u16, kept)
    };
    static CURRENT: Cell<Key> = const { Cell::new((0, 0)) };
}

/// Whether stamps are being kept.
#[inline]
pub fn on() -> bool {
    ON.load(Ordering::Relaxed)
}

/// Forget every stamp kept so far and keep stamps from now.
pub fn start() {
    for (_, kept) in lock(threads_kept()).iter() {
        lock(kept).clear();
    }
    ON.store(true, Ordering::SeqCst);
}

/// Stop keeping stamps, and hand back every one kept since [`start`].
pub fn stop() -> Vec<Record> {
    ON.store(false, Ordering::SeqCst);
    let mut all = Vec::new();
    for (_, kept) in lock(threads_kept()).iter() {
        all.append(&mut lock(kept));
    }
    all
}

/// The names of the threads stamps were made on, by index.
pub fn threads() -> Vec<String> {
    lock(threads_kept()).iter().map(|(name, _)| name.clone()).collect()
}

#[inline]
pub fn now() -> Instant {
    Instant::now()
}

/// Stamp `key` at `stage`, now.
#[inline]
pub fn stamp(key: Key, stage: Stage) {
    if on() {
        stamp_at(key, stage, Instant::now());
    }
}

/// Stamp `key` at `stage`, at a time taken earlier.
pub fn stamp_at(key: Key, stage: Stage, at: Instant) {
    if on() {
        MINE.with(|(thread, kept)| lock(kept).push(Record { key, stage, thread: *thread, at }));
    }
}

/// Stamp, and say this is the call this thread works for now: what
/// [`current`] answers until another says so.
#[inline]
pub fn enter(key: Key, stage: Stage) {
    CURRENT.with(|c| c.set(key));
    stamp(key, stage);
}

/// The call this thread works for now.
#[inline]
pub fn current() -> Key {
    CURRENT.with(Cell::get)
}

/// A job for an activation thread, as `served` keeps them.
#[cfg(not(target_arch = "wasm32"))]
type Job<A> = Box<dyn FnOnce(&std::rc::Rc<A>) -> futures::future::LocalBoxFuture<'static, ()> + Send>;

/// A job for an activation thread that works for the call current now:
/// stamped when it is taken and first polled, and every poll of it says so.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn within<A: 'static>(job: Job<A>) -> Job<A> {
    use futures::FutureExt;
    let key = current();
    Box::new(move |a| {
        enter(key, Stage::JobTaken);
        let mut made = job(a);
        let mut first = true;
        futures::future::poll_fn(move |cx| {
            CURRENT.with(|c| c.set(key));
            if std::mem::take(&mut first) {
                stamp(key, Stage::JobPolled);
            }
            made.poll_unpin(cx)
        })
        .boxed_local()
    })
}

/// The frames the server writes for the call current when it is made,
/// each stamped as the server has it and as it is queued to the pump: an
/// item counted, the end as [`END`].
pub(crate) struct Sent {
    call: u32,
    items: std::sync::atomic::AtomicU32,
}

impl Sent {
    pub(crate) fn new() -> Sent {
        Sent { call: current().0, items: Default::default() }
    }

    pub(crate) fn send(&self, send: &impl Fn(plexus_proto::Kind, &[u8]), kind: plexus_proto::Kind, body: &[u8]) {
        let key = match kind {
            plexus_proto::Kind::Item => (self.call, self.items.fetch_add(1, Ordering::Relaxed) + 1),
            _ => (self.call, END),
        };
        stamp(key, Stage::Answered);
        send(kind, body);
        stamp(key, Stage::Queued);
    }
}

/// Counts each call's items as its frames pass one place, so that a frame
/// is keyed as the rest of its call is. Counted only while stamps are kept,
/// so tracing is turned on and off between calls, never during one.
#[derive(Default)]
pub struct Frames(std::collections::HashMap<u32, u32>);

impl Frames {
    /// The key of the frame `bytes`, read from its first bytes alone: an
    /// open, an item or an end; `None` for any other.
    pub fn key(&mut self, bytes: &[u8]) -> Option<Key> {
        let (call, kind) = head(bytes)?;
        self.of(call, kind)
    }

    pub fn of(&mut self, call: u32, kind: plexus_proto::Kind) -> Option<Key> {
        use plexus_proto::Kind;
        match kind {
            Kind::Open => Some((call, 0)),
            Kind::Item => {
                let n = self.0.entry(call).or_default();
                *n += 1;
                Some((call, *n))
            }
            Kind::End => {
                self.0.remove(&call);
                Some((call, END))
            }
            _ => None,
        }
    }
}

/// A frame's call and kind, from its first bytes: `[7]` an end, `[7,K,`
/// kind K, `[7,` anything else an item.
fn head(bytes: &[u8]) -> Option<(u32, plexus_proto::Kind)> {
    use plexus_proto::Kind;
    let rest = bytes.strip_prefix(b"[")?;
    let digits = rest.iter().take_while(|b| b.is_ascii_digit()).count();
    let call = std::str::from_utf8(&rest[..digits]).ok()?.parse().ok()?;
    let kind = match &rest[digits..] {
        [b']', ..] => Kind::End,
        [b',', d, b',', ..] if d.is_ascii_digit() => Kind::from_digit((d - b'0') as u64)?,
        [b',', ..] => Kind::Item,
        _ => return None,
    };
    Some((call, kind))
}

/// The frames a writer has taken and not yet flushed.
#[derive(Default)]
pub struct Batch {
    frames: Frames,
    keys: Vec<Key>,
}

impl Batch {
    /// Keep `frame` to stamp at the flush, and stamp it taken now.
    pub fn taken(&mut self, frame: &[u8]) {
        if let Some(key) = self.keep(frame) {
            stamp(key, Stage::Taken);
        }
    }

    /// Keep `frame` to stamp at the flush.
    pub fn keep(&mut self, frame: &[u8]) -> Option<Key> {
        if !on() {
            return None;
        }
        let key = self.frames.key(frame)?;
        self.keys.push(key);
        Some(key)
    }

    /// One call owns this write batch in the serial TLS benchmark. Stamp the
    /// batch's record work once, even if it carried both the item and end.
    #[cfg(feature = "tls")]
    pub(crate) fn stamp_seals(&self, clock: &Clock) {
        clock.stamp_tls(self.keys.first().copied().filter(|key| self.keys.iter().all(|other| key.0 == other.0)), tls::RecordOp::Seal);
    }

    /// Stamp every frame taken since the last flush at `stage`, each at the
    /// time `at` says, and forget them.
    pub fn flushed(&mut self, mut at: impl FnMut(Stage) -> Option<Instant>, stages: &[Stage]) {
        for &stage in stages {
            if let Some(t) = at(stage) {
                for &key in &self.keys {
                    stamp_at(key, stage, t);
                }
            }
        }
        self.keys.clear();
    }
}

#[cfg(feature = "tls")]
mod tls;
#[cfg(feature = "tls")]
pub(crate) use tls::{TlsIo, tracing_provider};

#[cfg(feature = "tokio")]
pub use socket::{Clock, Socket};

#[cfg(feature = "tokio")]
mod socket {
    use std::io;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    #[cfg(feature = "tls")]
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll};
    use std::time::Instant;

    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    use tokio::net::TcpStream;

    use crate::lock;
    #[cfg(feature = "tls")]
    use super::{Key, Stage, stamp_at, tls::RecordOp};

    /// When a socket last read, and wrote since it was last asked.
    #[derive(Default)]
    pub struct Clock {
        times: Mutex<Times>,
        #[cfg(feature = "tls")]
        tls: AtomicBool,
    }

    #[derive(Default)]
    struct Times {
        read: Option<(Instant, Instant)>,
        write_entered: Option<Instant>,
        write_returned: Option<Instant>,
        #[cfg(feature = "tls")]
        sealed: Vec<(Instant, Instant)>,
        #[cfg(feature = "tls")]
        opened: Vec<(Instant, Instant)>,
        #[cfg(feature = "tls")]
        writes: Vec<(Instant, Instant)>,
        #[cfg(feature = "tls")]
        reads: Vec<(Instant, Instant)>,
    }

    impl Clock {
        /// When the last read that got bytes was entered, and returned.
        pub fn read(&self) -> Option<(Instant, Instant)> {
            lock(&self.times).read
        }

        #[cfg(feature = "tls")]
        pub(crate) fn stamp_opens(&self, key: Option<Key>) {
            self.stamp_tls(key, RecordOp::Open);
        }

        #[cfg(feature = "tls")]
        pub(super) fn stamp_tls(&self, key: Option<Key>, op: RecordOp) {
            let mut times = lock(&self.times);
            fn stamp(key: Option<Key>, entries: &mut Vec<(Instant, Instant)>, began: Stage, ended: Stage) {
                for (start, end) in entries.drain(..) {
                    if let Some(key) = key {
                        stamp_at(key, began, start);
                        stamp_at(key, ended, end);
                    }
                }
            }
            match op {
                RecordOp::Seal => {
                    stamp(key, &mut times.sealed, Stage::TlsSealEntry, Stage::TlsSealReturn);
                    stamp(key, &mut times.writes, Stage::TlsTcpWriteEntry, Stage::TlsTcpWriteReturn);
                }
                RecordOp::Open => {
                    stamp(key, &mut times.opened, Stage::TlsOpenEntry, Stage::TlsOpenReturn);
                    stamp(key, &mut times.reads, Stage::TlsTcpReadEntry, Stage::TlsTcpReadReturn);
                }
            }
        }

        #[cfg(feature = "tls")]
        pub(super) fn mark_tls(&self) {
            self.tls.store(true, Ordering::Relaxed);
        }

        #[cfg(feature = "tls")]
        fn tracing_tls(&self) -> bool {
            self.tls.load(Ordering::Relaxed)
        }

        #[cfg(feature = "tls")]
        pub(super) fn record(&self, op: RecordOp, times: (Instant, Instant)) {
            let mut clock = lock(&self.times);
            match op {
                RecordOp::Seal => clock.sealed.push(times),
                RecordOp::Open => clock.opened.push(times),
            }
        }

        /// When the first write since the last ask was entered, and the
        /// last returned; forgotten once asked.
        pub fn written(&self) -> (Option<Instant>, Option<Instant>) {
            let mut t = lock(&self.times);
            (t.write_entered.take(), t.write_returned.take())
        }
    }

    /// A TCP stream whose reads and writes are timed on its [`Clock`].
    pub struct Socket {
        stream: TcpStream,
        clock: Arc<Clock>,
    }

    impl Socket {
        pub fn new(stream: TcpStream) -> (Socket, Arc<Clock>) {
            let clock = Arc::new(Clock::default());
            (Socket { stream, clock: clock.clone() }, clock)
        }
    }

    impl AsyncRead for Socket {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            if !super::on() {
                return Pin::new(&mut self.stream).poll_read(cx, buf);
            }
            let (entered, before) = (Instant::now(), buf.filled().len());
            let polled = Pin::new(&mut self.stream).poll_read(cx, buf);
            if matches!(polled, Poll::Ready(Ok(()))) && buf.filled().len() > before {
                let span = (entered, Instant::now());
                let mut times = lock(&self.clock.times);
                times.read = Some(span);
                #[cfg(feature = "tls")]
                if self.clock.tracing_tls() {
                    times.reads.push(span);
                }
            }
            polled
        }
    }

    impl AsyncWrite for Socket {
        fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
            if !super::on() {
                return Pin::new(&mut self.stream).poll_write(cx, buf);
            }
            let entered = Instant::now();
            let polled = Pin::new(&mut self.stream).poll_write(cx, buf);
            if matches!(polled, Poll::Ready(Ok(n)) if n > 0) {
                let mut t = lock(&self.clock.times);
                let returned = Instant::now();
                t.write_entered.get_or_insert(entered);
                t.write_returned = Some(returned);
                #[cfg(feature = "tls")]
                if self.clock.tracing_tls() {
                    t.writes.push((entered, returned));
                }
            }
            polled
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.stream).poll_flush(cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.stream).poll_shutdown(cx)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plexus_proto::Kind;

    #[test]
    fn a_frame_is_keyed_from_its_first_bytes() {
        assert_eq!(head(b"[7]"), Some((7, Kind::End)));
        assert_eq!(head(b"[7,{\"a\":1}]"), Some((7, Kind::Item)));
        assert_eq!(head(b"[7,4]"), Some((7, Kind::Item)));
        assert_eq!(head(b"[12,1,[\"kiln.fire\",{}]]"), Some((12, Kind::Open)));
        assert_eq!(head(b"[12,6,4095]"), Some((12, Kind::Credit)));
        let mut frames = Frames::default();
        assert_eq!(frames.key(b"[3,1,[\"m\"]]"), Some((3, 0)));
        assert_eq!(frames.key(b"[3,{}]"), Some((3, 1)));
        assert_eq!(frames.key(b"[3,{}]"), Some((3, 2)));
        assert_eq!(frames.key(b"[3]"), Some((3, END)));
        assert_eq!(frames.key(b"[3,6,10]"), None);
    }
}
