//! Time the ring provider's TLS 1.3 record operations. The active clock is
//! scoped to a poll of one connection, including when a task changes threads.

use std::cell::RefCell;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Instant;

use rustls::crypto::CryptoProvider;
use rustls::crypto::cipher::{
    AeadKey, InboundOpaqueMessage, InboundPlainMessage, Iv, MessageDecrypter, MessageEncrypter,
    OutboundOpaqueMessage, OutboundPlainMessage, Tls13AeadAlgorithm, UnsupportedOperationError,
};
use rustls::{
    CipherSuiteCommon, ConnectionTrafficSecrets, ContentType, Error, SupportedCipherSuite,
    Tls13CipherSuite,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::{Clock, on};

pub(crate) fn tracing_provider(mut provider: CryptoProvider) -> CryptoProvider {
    // The suites require static references. Wrap ring's suite list once.
    static SUITES: OnceLock<Vec<SupportedCipherSuite>> = OnceLock::new();
    provider.cipher_suites = SUITES
        .get_or_init(|| {
            provider
                .cipher_suites
                .iter()
                .map(|suite| match suite.tls13() {
                    Some(suite) => {
                        let wrapped: &'static Tls13CipherSuite =
                            Box::leak(Box::new(Tls13CipherSuite {
                                common: CipherSuiteCommon {
                                    suite: suite.common.suite,
                                    hash_provider: suite.common.hash_provider,
                                    confidentiality_limit: suite.common.confidentiality_limit,
                                },
                                hkdf_provider: suite.hkdf_provider,
                                aead_alg: Box::leak(Box::new(Algorithm(suite.aead_alg))),
                                quic: suite.quic,
                            }));
                        SupportedCipherSuite::Tls13(wrapped)
                    }
                    None => *suite,
                })
                .collect()
        })
        .clone();
    provider
}

struct Algorithm(&'static dyn Tls13AeadAlgorithm);

impl Tls13AeadAlgorithm for Algorithm {
    fn encrypter(&self, key: AeadKey, iv: Iv) -> Box<dyn MessageEncrypter> {
        Box::new(Seal(self.0.encrypter(key, iv)))
    }

    fn decrypter(&self, key: AeadKey, iv: Iv) -> Box<dyn MessageDecrypter> {
        Box::new(Open(self.0.decrypter(key, iv)))
    }

    fn key_len(&self) -> usize {
        self.0.key_len()
    }

    fn extract_keys(
        &self,
        key: AeadKey,
        iv: Iv,
    ) -> Result<ConnectionTrafficSecrets, UnsupportedOperationError> {
        self.0.extract_keys(key, iv)
    }

    fn fips(&self) -> bool {
        self.0.fips()
    }
}

struct Seal(Box<dyn MessageEncrypter>);

impl MessageEncrypter for Seal {
    fn encrypt(
        &mut self,
        msg: OutboundPlainMessage<'_>,
        seq: u64,
    ) -> Result<OutboundOpaqueMessage, Error> {
        if msg.typ != ContentType::ApplicationData || !active() {
            return self.0.encrypt(msg, seq);
        }
        let began = Instant::now();
        let result = self.0.encrypt(msg, seq);
        let ended = Instant::now();
        if result.is_ok() {
            observe(RecordOp::Seal, began, ended);
        }
        result
    }

    fn encrypted_payload_len(&self, payload_len: usize) -> usize {
        self.0.encrypted_payload_len(payload_len)
    }
}

struct Open(Box<dyn MessageDecrypter>);

impl MessageDecrypter for Open {
    fn decrypt<'a>(
        &mut self,
        msg: InboundOpaqueMessage<'a>,
        seq: u64,
    ) -> Result<InboundPlainMessage<'a>, Error> {
        if !active() {
            return self.0.decrypt(msg, seq);
        }
        let began = Instant::now();
        let result = self.0.decrypt(msg, seq);
        let ended = Instant::now();
        if result
            .as_ref()
            .is_ok_and(|plain| plain.typ == ContentType::ApplicationData)
        {
            observe(RecordOp::Open, began, ended);
        }
        result
    }
}

#[derive(Clone, Copy)]
pub(super) enum RecordOp {
    Seal,
    Open,
}

thread_local! {
    static ACTIVE: RefCell<Option<Arc<Clock>>> = const { RefCell::new(None) };
}

fn active() -> bool {
    ACTIVE.with(|clock| clock.borrow().is_some())
}

fn observe(op: RecordOp, began: Instant, ended: Instant) {
    ACTIVE.with(|active| {
        if let Some(clock) = active.borrow().as_ref() {
            clock.record(op, (began, ended));
        }
    });
}

fn with_clock<T>(clock: &Arc<Clock>, f: impl FnOnce() -> T) -> T {
    if !on() {
        return f();
    }

    struct Restore(Option<Arc<Clock>>);
    impl Drop for Restore {
        fn drop(&mut self) {
            ACTIVE.replace(self.0.take());
        }
    }

    let _restore = Restore(ACTIVE.replace(Some(clock.clone())));
    f()
}

pub(crate) struct TlsIo<S> {
    stream: S,
    clock: Arc<Clock>,
}

impl<S> TlsIo<S> {
    pub(crate) fn new(stream: S, clock: Arc<Clock>) -> Self {
        clock.mark_tls();
        Self { stream, clock }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for TlsIo<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        with_clock(&this.clock, || {
            Pin::new(&mut this.stream).poll_read(cx, buf)
        })
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for TlsIo<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        with_clock(&this.clock, || {
            Pin::new(&mut this.stream).poll_write(cx, buf)
        })
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        with_clock(&this.clock, || Pin::new(&mut this.stream).poll_flush(cx))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}
