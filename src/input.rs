//! Caller-to-callee items. Input credit is independent of output credit.
//! A relay passes the downstream allowance upstream instead of opening a
//! second receive window at each mounted child.

use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::channel::mpsc;
use futures::stream::{BoxStream, Stream, StreamExt};
use futures::task::AtomicWaker;
use plexus_proto::{Body, CallId, Frame, Numbering};

use crate::channel::Out;
use crate::{Failure, Raw, TOP_UP, WINDOW, lock, server::failure};

/// Items the caller supplies while its call runs. Each item can fail decoding.
pub struct Input<T>(BoxStream<'static, Result<T, Failure>>);

impl<T: 'static> Input<T> {
    /// A finite input for calling a method directly.
    pub fn from_iter<I: IntoIterator<Item = T>>(items: I) -> Self
    where
        I::IntoIter: Send + 'static,
    {
        Self(Box::pin(futures::stream::iter(items.into_iter().map(Ok))))
    }

    pub(crate) fn receive(
        mut incoming: RawInput,
        decode: impl Fn(Raw) -> Result<T, Failure> + Send + 'static,
    ) -> Self {
        let mut remaining = 0;
        let mut finished = false;
        Self(Box::pin(futures::stream::poll_fn(move |cx| {
            if finished {
                return Poll::Ready(None);
            }
            if remaining == 0 {
                incoming.grant(WINDOW);
                remaining = WINDOW;
            } else if remaining == TOP_UP {
                incoming.grant(TOP_UP);
                remaining += TOP_UP;
            }
            let next = incoming.items.poll_next_unpin(cx);
            if let Some(f) = incoming.state.error() {
                finished = true;
                return Poll::Ready(Some(Err(f)));
            }
            next.map(|item| match item {
                Some(raw) => {
                    remaining -= 1;
                    let decoded = decode(raw);
                    if let Err(f) = &decoded {
                        incoming.state.fail(f.clone());
                        finished = true;
                    }
                    Some(decoded)
                }
                None => {
                    finished = true;
                    None
                }
            })
        })))
    }
}

impl<T> Stream for Input<T> {
    type Item = Result<T, Failure>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.as_mut().poll_next(cx)
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub fn decode_input<T: serde::de::DeserializeOwned>(raw: Raw) -> Result<T, Failure> {
    serde_json::from_str(raw.get())
        .map_err(|e| failure("invalid-params", &format!("invalid input item: {e}")))
}

/// Shared by the connection reader and the activation's input stream.
pub(crate) struct InputState {
    // Credits sent but not yet received as items.
    left: AtomicU32,
    failed: Mutex<Option<Failure>>,
    wake: AtomicWaker,
}

impl InputState {
    pub(crate) fn fail(&self, f: Failure) {
        lock(&self.failed).get_or_insert(f);
        self.wake.wake();
    }

    pub(crate) fn error(&self) -> Option<Failure> {
        lock(&self.failed).clone()
    }

    pub(crate) async fn failure(&self) -> Failure {
        futures::future::poll_fn(|cx| {
            self.wake.register(cx.waker());
            self.error().map_or(Poll::Pending, Poll::Ready)
        })
        .await
    }
}

/// The raw items a mounted child receives, paced by the downstream consumer.
/// Construction is private: each instance belongs to a single open call.
pub struct RawInput {
    items: mpsc::UnboundedReceiver<Raw>,
    state: Arc<InputState>,
    out: Out,
    id: CallId,
}

impl RawInput {
    fn grant(&self, n: u32) {
        self.state.left.fetch_add(n, Ordering::AcqRel);
        self.out.send(|buf| {
            Frame {
                call: self.id,
                seq: 0,
                body: Body::Credit(n),
            }
            .encode_into(Numbering::Counted, buf)
        });
    }

    pub(crate) fn state(&self) -> Arc<InputState> {
        self.state.clone()
    }
}

pub(crate) struct Ingress {
    send: Option<mpsc::UnboundedSender<Raw>>,
    state: Arc<InputState>,
}

pub(crate) fn incoming(out: Out, id: CallId) -> (Ingress, RawInput) {
    let (send, items) = mpsc::unbounded();
    let state = Arc::new(InputState {
        left: AtomicU32::new(0),
        failed: Mutex::default(),
        wake: AtomicWaker::new(),
    });
    (
        Ingress {
            send: Some(send),
            state: state.clone(),
        },
        RawInput {
            items,
            state,
            out,
            id,
        },
    )
}

impl Ingress {
    pub(crate) fn item(&self, raw: Raw) {
        let Some(send) = &self.send else {
            return self
                .state
                .fail(failure("invalid-params", "input item after input end"));
        };
        if self
            .state
            .left
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
            .is_err()
        {
            return self
                .state
                .fail(failure("invalid-params", "input item without credit"));
        }
        let _ = send.unbounded_send(raw);
    }

    pub(crate) fn end(&mut self, end: Result<(), Failure>) {
        if self.send.is_none() {
            return self
                .state
                .fail(failure("invalid-params", "duplicate input end"));
        }
        if let Err(f) = end {
            // Publish the failure before the receiver can observe EOF.
            self.state.fail(f);
        }
        self.send = None;
    }
}

impl Drop for Ingress {
    fn drop(&mut self) {
        // Losing an unfinished call (cancel or disconnect) must never look
        // like a successful end of the input to a concurrent activation.
        if self.send.is_some() {
            self.state
                .fail(failure("cancelled", "input was interrupted"));
        }
    }
}

pub(crate) enum Source {
    Local(BoxStream<'static, Result<Raw, Failure>>),
    Forward(RawInput),
}

/// Pump the producer by the credit received on this call. Both source kinds
/// are polled only with credit. Closing or cancelling the call drops it.
pub(crate) struct Upload {
    source: Source,
    credit: Arc<crate::server::Credit>,
    batch: u32,
}

impl Upload {
    pub(crate) fn new(source: Source, credit: Arc<crate::server::Credit>) -> Self {
        Self {
            source,
            credit,
            batch: 0,
        }
    }

    /// `true` when input has ended. Yield periodically to inbound replies.
    pub(crate) fn pump(&mut self, cx: &mut Context<'_>, mut send: impl FnMut(Body)) -> bool {
        for _ in 0..64 {
            if self.batch == 0 {
                let Poll::Ready(n) = self.credit.poll_take(cx, WINDOW) else {
                    return false;
                };
                if let Source::Forward(input) = &self.source {
                    input.grant(n);
                }
                self.batch = n;
            }
            let next = match &mut self.source {
                Source::Local(s) => s.poll_next_unpin(cx),
                Source::Forward(input) => {
                    let next = input.items.poll_next_unpin(cx);
                    if let Some(f) = input.state.error() {
                        Poll::Ready(Some(Err(f)))
                    } else {
                        next.map(|v| v.map(Ok))
                    }
                }
            };
            match next {
                Poll::Pending => return false,
                Poll::Ready(Some(Ok(raw))) => {
                    self.batch -= 1;
                    send(Body::Item(raw));
                }
                Poll::Ready(end) => {
                    send(Body::End(end.transpose().map(|_| ())));
                    return true;
                }
            }
        }
        cx.waker().wake_by_ref();
        false
    }
}
