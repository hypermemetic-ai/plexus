//! `wss://`: a WebSocket inside TLS 1.3, each end proving an Ed25519 key.
//!
//! An end's certificate is its own key, signed by itself; there is
//! no authority. The handshake proves each end holds the key its
//! certificate names (TLS 1.3's CertificateVerify), and each end checks the
//! other's key against the pins before starting its WebSocket handshake.
//! The key is the principal: `key:` and its 32 bytes in hex.

use std::collections::HashSet;
use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use base64::Engine as _;
use base64::alphabet::{STANDARD, URL_SAFE};
use base64::engine::DecodePaddingMode;
use base64::engine::general_purpose::{GeneralPurpose, GeneralPurposeConfig};
use rustls::client::Resumption;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{
    CryptoProvider, WebPkiSupportedAlgorithms, verify_tls13_signature_with_raw_key,
};
use rustls::pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, SubjectPublicKeyInfoDer,
    UnixTime,
};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::{NoServerSessionStorage, ParsedCertificate};
use rustls::{
    ClientConfig, DigitallySignedStruct, DistinguishedName, ServerConfig, SignatureScheme,
};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tungstenite::client::IntoClientRequest;
use zeroize::Zeroizing;

use crate::{Channel, Server};

/// The DER of an Ed25519 key's SubjectPublicKeyInfo, before its 32 bytes.
const SPKI: [u8; 12] = [
    0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
];
/// A PKCS #8 v1 Ed25519 private key, before its 32-byte seed.
const PKCS8: [u8; 16] = [
    0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
];

/// An end's key, and the certificate that is nothing but the key.
pub struct Identity {
    cert: CertificateDer<'static>,
    key: Zeroizing<PrivatePkcs8KeyDer<'static>>,
    principal: String,
}

impl Identity {
    /// From the 32-byte Ed25519 seed encoded in an agent's key file.
    pub fn from_seed(seed: [u8; 32]) -> io::Result<Identity> {
        let seed = Zeroizing::new(seed);
        let mut pkcs8 = Zeroizing::new(Vec::with_capacity(PKCS8.len() + seed.len()));
        pkcs8.extend_from_slice(&PKCS8);
        pkcs8.extend_from_slice(seed.as_ref());
        let pair = rcgen::KeyPair::try_from(pkcs8.as_slice()).map_err(other)?;
        Identity::of(pair)
    }

    /// Read a Synapse key file, usually `~/.plexus/keys/NAME.key`.
    /// Synapse writes base64 text and also accepts legacy 32-byte seed files.
    pub fn from_key_file(path: impl AsRef<Path>) -> io::Result<Identity> {
        const CONFIG: GeneralPurposeConfig =
            GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent);
        const ENGINES: [GeneralPurpose; 2] = [
            GeneralPurpose::new(&STANDARD, CONFIG),
            GeneralPurpose::new(&URL_SAFE, CONFIG),
        ];
        let bytes = Zeroizing::new(std::fs::read(path)?);
        if let Ok(text) = std::str::from_utf8(&bytes) {
            for engine in ENGINES {
                let mut decoded = Zeroizing::new([0; 32]);
                if engine.decode_slice(text.trim(), decoded.as_mut()) == Ok(decoded.len()) {
                    return Self::from_seed(*decoded);
                }
            }
        }
        if let Ok(seed) = bytes.as_slice().try_into() {
            return Self::from_seed(seed);
        }
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid Ed25519 seed file",
        ))
    }

    /// A new key.
    pub fn generate() -> io::Result<Identity> {
        let pair = rcgen::KeyPair::generate_for(&rcgen::PKCS_ED25519).map_err(other)?;
        Identity::of(pair)
    }

    fn of(pair: rcgen::KeyPair) -> io::Result<Identity> {
        let cert = rcgen::CertificateParams::default()
            .self_signed(&pair)
            .map_err(other)?;
        let principal = principal(pair.public_key_raw());
        let key = Zeroizing::new(PrivatePkcs8KeyDer::from(pair.serialized_der().to_vec()));
        Ok(Identity {
            cert: cert.der().clone(),
            key,
            principal,
        })
    }

    /// `key:` and the public key in hex.
    pub fn principal(&self) -> &str {
        &self.principal
    }

    fn key(&self) -> PrivateKeyDer<'static> {
        self.key.clone_key().into()
    }
}

/// What dials out: who this end is, and the one key it expects of the far end.
pub struct Dial {
    config: Arc<ClientConfig>,
}

impl Dial {
    /// Build a dialer pinned to a server's `key:` principal.
    pub fn new(me: &Identity, expect: &str) -> io::Result<Dial> {
        Self::with_provider(me, expect, provider())
    }

    #[cfg(feature = "trace")]
    #[doc(hidden)]
    pub fn new_untraced(me: &Identity, expect: &str) -> io::Result<Dial> {
        Self::with_provider(me, expect, rustls::crypto::ring::default_provider())
    }

    fn with_provider(me: &Identity, expect: &str, provider: CryptoProvider) -> io::Result<Dial> {
        validate_pin(expect)?;
        let verifier = Arc::new(Expect {
            key: expect.to_owned(),
            algs: provider.signature_verification_algorithms,
        });
        let mut config = ClientConfig::builder_with_provider(Arc::new(provider))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(other)?
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_client_auth_cert(vec![me.cert.clone()], me.key())
            .map_err(other)?;
        // Each connection must prove fresh possession of both keys.
        config.resumption = Resumption::disabled();
        Ok(Dial {
            config: Arc::new(config),
        })
    }

    /// A channel to the WebSocket at `url` (`wss://127.0.0.1:4455/`), and the
    /// principal the far end proved. A URL without a port uses 443.
    pub async fn connect(&self, url: &str) -> io::Result<(Channel, String)> {
        if !url.starts_with("wss://") {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "TLS dials wss:// only",
            ));
        }
        let request = url.into_client_request().map_err(other)?;
        let uri = request.uri();
        let host = uri
            .host()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing WebSocket host"))?;
        let name = ServerName::try_from(
            host.trim_start_matches('[')
                .trim_end_matches(']')
                .to_owned(),
        )
        .map_err(other)?;
        let tcp = TcpStream::connect(format!("{host}:{}", uri.port_u16().unwrap_or(443))).await?;
        tcp.set_nodelay(true)?;
        #[cfg(feature = "trace")]
        let (tcp, clock) = crate::trace::Socket::new(tcp);
        let tls = TlsConnector::from(self.config.clone())
            .connect(name, tcp)
            .await?;
        let peer = peer(tls.get_ref().1.peer_certificates())?;
        #[cfg(feature = "trace")]
        let tls = crate::trace::TlsIo::new(tls, clock.clone());
        let channel = crate::ws::client_over(
            tls,
            request,
            #[cfg(feature = "trace")]
            clock,
        )
        .await?;
        Ok((channel, peer))
    }
}

/// What accepts: who this end is, and the keys it accepts.
pub struct Listen {
    acceptor: TlsAcceptor,
}

impl Listen {
    /// Build an acceptor pinned to the listed callers' `key:` principals.
    pub fn new<'a>(
        me: &Identity,
        accepts: impl IntoIterator<Item = &'a str>,
    ) -> io::Result<Listen> {
        Self::with_provider(me, accepts, provider())
    }

    #[cfg(feature = "trace")]
    #[doc(hidden)]
    pub fn new_untraced<'a>(
        me: &Identity,
        accepts: impl IntoIterator<Item = &'a str>,
    ) -> io::Result<Listen> {
        Self::with_provider(me, accepts, rustls::crypto::ring::default_provider())
    }

    fn with_provider<'a>(
        me: &Identity,
        accepts: impl IntoIterator<Item = &'a str>,
        provider: CryptoProvider,
    ) -> io::Result<Listen> {
        let mut keys = HashSet::new();
        for pin in accepts {
            validate_pin(pin)?;
            keys.insert(pin.to_owned());
        }
        let verifier = Arc::new(Accepts {
            keys,
            algs: provider.signature_verification_algorithms,
        });
        let mut config = ServerConfig::builder_with_provider(Arc::new(provider))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(other)?
            .with_client_cert_verifier(verifier)
            .with_single_cert(vec![me.cert.clone()], me.key())
            .map_err(other)?;
        config.send_tls13_tickets = 0;
        config.session_storage = Arc::new(NoServerSessionStorage {});
        Ok(Listen {
            acceptor: TlsAcceptor::from(Arc::new(config)),
        })
    }

    /// A channel over a connection just accepted, and the principal the
    /// caller proved, which is one this end accepts.
    pub async fn accept(&self, tcp: TcpStream) -> io::Result<(Channel, String)> {
        tcp.set_nodelay(true)?;
        #[cfg(feature = "trace")]
        let (tcp, clock) = crate::trace::Socket::new(tcp);
        let tls = self.acceptor.accept(tcp).await?;
        let peer = peer(tls.get_ref().1.peer_certificates())?;
        #[cfg(feature = "trace")]
        let tls = crate::trace::TlsIo::new(tls, clock.clone());
        let channel = crate::ws::server_over(
            tls,
            #[cfg(feature = "trace")]
            clock,
        )
        .await?;
        Ok((channel, peer))
    }

    /// Bind `addr` and serve `server` there in the background, each
    /// connection on a task of its own. Returns the bound address.
    /// Use [`Self::accept`] to inspect the peer or a handshake error.
    pub async fn spawn(self, addr: SocketAddr, server: Arc<Server>) -> io::Result<SocketAddr> {
        let listener = TcpListener::bind(addr).await?;
        let bound = listener.local_addr()?;
        let me = Arc::new(self);
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                let (me, server) = (me.clone(), server.clone());
                tokio::spawn(async move {
                    if let Ok((channel, _caller)) = me.accept(tcp).await {
                        let _ = server.serve(channel).await;
                    }
                });
            }
        });
        Ok(bound)
    }
}

fn provider() -> CryptoProvider {
    let provider = rustls::crypto::ring::default_provider();
    #[cfg(feature = "trace")]
    let provider = crate::trace::tracing_provider(provider);
    provider
}

fn validate_pin(pin: &str) -> io::Result<()> {
    if pin.strip_prefix("key:").is_some_and(|key| {
        key.len() == 64
            && key
                .bytes()
                .all(|b| b.is_ascii_digit() || matches!(b, b'a'..=b'f'))
    }) {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected key: and 64 lowercase hex digits",
        ))
    }
}

/// Parse the certificate's subject key through rustls. The exact same SPKI
/// supplies the pin and verifies CertificateVerify.
fn subject_key(
    cert: &CertificateDer<'_>,
) -> Result<SubjectPublicKeyInfoDer<'static>, rustls::Error> {
    let spki = ParsedCertificate::try_from(cert)?.subject_public_key_info();
    if spki.as_ref().len() == SPKI.len() + 32 && spki.as_ref().starts_with(&SPKI) {
        Ok(spki)
    } else {
        Err(rustls::Error::General(
            "the certificate names no Ed25519 key".into(),
        ))
    }
}

fn principal_of(cert: &CertificateDer<'_>) -> Result<String, rustls::Error> {
    Ok(principal(&subject_key(cert)?.as_ref()[SPKI.len()..]))
}

fn verify_proof(
    message: &[u8],
    cert: &CertificateDer<'_>,
    dss: &DigitallySignedStruct,
    algs: &WebPkiSupportedAlgorithms,
) -> Result<HandshakeSignatureValid, rustls::Error> {
    if dss.scheme != SignatureScheme::ED25519 {
        return Err(rustls::Error::General(
            "the handshake did not use Ed25519".into(),
        ));
    }
    verify_tls13_signature_with_raw_key(message, &subject_key(cert)?, dss, algs)
}

fn peer(certs: Option<&[CertificateDer<'_>]>) -> io::Result<String> {
    let cert = certs.and_then(<[_]>::first).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            "the other end proved no key",
        )
    })?;
    principal_of(cert).map_err(other)
}

/// The caller's check of the server: its key is the one expected.
#[derive(Debug)]
struct Expect {
    key: String,
    algs: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for Expect {
    fn verify_server_cert(
        &self,
        cert: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let offered = principal_of(cert)?;
        if offered == self.key {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General(format!(
                "the server offered {offered}, but this caller expects {}",
                self.key
            )))
        }
    }
    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &CertificateDer<'_>,
        _: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::General("TLS 1.3 only".into()))
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_proof(message, cert, dss, &self.algs)
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![SignatureScheme::ED25519]
    }
}

/// The server's check of a caller: its key is one of those accepted.
#[derive(Debug)]
struct Accepts {
    keys: HashSet<String>,
    algs: WebPkiSupportedAlgorithms,
}

impl ClientCertVerifier for Accepts {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }
    fn verify_client_cert(
        &self,
        cert: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        let offered = principal_of(cert)?;
        if self.keys.contains(&offered) {
            Ok(ClientCertVerified::assertion())
        } else {
            Err(rustls::Error::General(format!(
                "the caller offered {offered}, which this server does not accept"
            )))
        }
    }
    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &CertificateDer<'_>,
        _: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::General("TLS 1.3 only".into()))
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_proof(message, cert, dss, &self.algs)
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![SignatureScheme::ED25519]
    }
}

fn principal(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut principal = String::with_capacity(4 + bytes.len() * 2);
    principal.push_str("key:");
    for &byte in bytes {
        principal.push(HEX[(byte >> 4) as usize] as char);
        principal.push(HEX[(byte & 0xf) as usize] as char);
    }
    principal
}

fn other(e: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::other(e)
}

/// For `pace`: TLS records sealed by one end and opened by the other in
/// memory, no socket, `write` bytes at a time until `total` have crossed.
/// The rate in MB/s, and the suite the two ends chose.
#[doc(hidden)]
pub fn records(write: usize, total: usize) -> io::Result<(f64, String)> {
    use std::io::{Read, Write};

    if write == 0 || total == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "record sizes must be nonzero",
        ));
    }
    let (a, b) = (Identity::generate()?, Identity::generate()?);
    let (dial, listen) = (
        Dial::new(&a, b.principal())?,
        Listen::new(&b, [a.principal()])?,
    );
    let mut c = rustls::Connection::from(
        rustls::ClientConnection::new(
            dial.config.clone(),
            ServerName::try_from("plexus").expect("static DNS name"),
        )
        .map_err(other)?,
    );
    let mut s = rustls::Connection::from(
        rustls::ServerConnection::new(listen.acceptor.config().clone()).map_err(other)?,
    );
    let mut sink = vec![0u8; 1 << 17];
    let mut cross =
        |from: &mut rustls::Connection, to: &mut rustls::Connection| -> io::Result<usize> {
            let mut wire = Vec::new();
            while from.wants_write() {
                from.write_tls(&mut wire)?;
            }
            let (mut rest, mut read) = (&wire[..], 0);
            while !rest.is_empty() {
                to.read_tls(&mut rest)?;
                to.process_new_packets().map_err(other)?;
                loop {
                    match to.reader().read(&mut sink) {
                        Ok(0) => break,
                        Ok(n) => read += n,
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                        Err(e) => return Err(e),
                    }
                }
            }
            Ok(read)
        };
    while c.is_handshaking() || s.is_handshaking() {
        cross(&mut c, &mut s)?;
        cross(&mut s, &mut c)?;
    }
    let data = vec![b'x'; write];
    let (started, mut moved) = (std::time::Instant::now(), 0);
    while moved < total {
        c.writer().write_all(&data[..write.min(total - moved)])?;
        moved += cross(&mut c, &mut s)?;
    }
    let suite = match &c {
        rustls::Connection::Client(c) => c
            .negotiated_cipher_suite()
            .map(|s| format!("{:?}", s.suite()))
            .unwrap_or_default(),
        rustls::Connection::Server(_) => String::new(),
    };
    Ok((moved as f64 / started.elapsed().as_secs_f64() / 1e6, suite))
}
