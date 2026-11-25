//! Two independent RPC callers sharing one authenticated, ordered channel.
use std::{future::Future, io, sync::Arc};

use bytes::Bytes;
use futures::{FutureExt, SinkExt, StreamExt, TryStreamExt, channel::mpsc, future};
use plexus_proto::{Head, Kind, binary_input};
use serde::{Deserialize, Serialize};

use crate::{Caller, Channel, Connection, Server};

/// The transport opener allocates odd call IDs. The acceptor allocates even IDs.
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Dial,
    Accept,
}

#[derive(Serialize, Deserialize)]
struct Offer<'a> {
    protocol: &'a str,
    side: Side,
    interface: &'a str,
    expects: &'a str,
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

/// Exchange peer offers before serving calls. Pass the principal verified by
/// the transport (for pinned WSS this is the second value from dial or accept).
/// Each end declares the far interface it requires and the server it exposes.
/// Drive the returned future for the link's lifetime. A disconnect interrupts
/// outstanding calls. A replacement channel starts a new session.
pub async fn connect(
    mut wire: Channel,
    side: Side,
    principal: Caller,
    server: Arc<Server>,
    expects: &str,
) -> io::Result<(
    Connection,
    impl Future<Output = io::Result<()>> + Send + 'static + use<>,
)> {
    let interface = server
        .interface()
        .ok_or_else(|| invalid("peer server requires an interface hello"))?;
    let offer = Offer {
        protocol: "plexus-peer/1",
        side,
        interface,
        expects,
    };
    let send = wire.send.send(Bytes::from(serde_json::to_vec(&offer)?));
    let receive = async {
        let bytes = wire
            .receive
            .try_next()
            .await?
            .ok_or_else(|| invalid("missing peer offer"))?;
        let remote: Offer =
            serde_json::from_slice(&bytes).map_err(|_| invalid("invalid peer offer"))?;
        if remote.protocol != offer.protocol || remote.side == side {
            return Err(invalid("incompatible peer protocol or connection side"));
        }
        if remote.interface != expects {
            return Err(invalid(format!(
                "expected interface {expects}, peer offered {}",
                remote.interface
            )));
        }
        if remote.expects != interface {
            return Err(invalid(format!(
                "peer expects {}, local interface is {interface}",
                remote.expects
            )));
        }
        Ok(())
    };
    future::try_join(send, receive).await?;

    let first = if side == Side::Dial { 1 } else { 2 };
    let (out, sending) = mpsc::channel::<Bytes>(32);
    let (mut replies, caller) = endpoint(out.clone(), wire.binary_input);
    let (mut requests, callee) = endpoint(out, wire.binary_input);
    let (connection, calling) = Connection::numbered(caller, Some(expects), first, 2);
    let drive = async move {
        let serving = server.serve_as(callee, principal);
        let read = async move {
            while let Some(bytes) = wire.receive.try_next().await? {
                let to_caller = if bytes.first() == Some(&binary_input::TAG) {
                    let (id, _) = binary_input::header(&bytes)?;
                    if id.0 % 2 == first % 2 {
                        return Err(invalid("binary input used a locally owned ID"));
                    }
                    false
                } else {
                    let head = Head::read(&bytes).map_err(|e| invalid(e.to_string()))?;
                    let local = head.call.0 % 2 == first % 2;
                    if (head.call.0 == 0) != (head.kind == Kind::Hello)
                        || (head.kind == Kind::Open && local)
                    {
                        return Err(invalid("invalid peer frame or call ID ownership"));
                    }
                    head.kind == Kind::Hello || local
                };
                let target = if to_caller {
                    &mut replies
                } else {
                    &mut requests
                };
                // A locally closed session may already be draining its wire close.
                if !target.is_closed() {
                    let _ = target.send(bytes).await;
                }
            }
            Ok(())
        };
        // Clean EOF closes both inboxes. Let the caller consume queued terminal
        // frames before returning. Errors immediately drop all four drivers.
        let serve_until_error = async move {
            serving.await?;
            future::pending::<io::Result<()>>().await
        };
        let lifetime = async move {
            match future::select(calling.boxed(), serve_until_error.boxed()).await {
                future::Either::Left((end, _)) | future::Either::Right((end, _)) => end,
            }
        };
        futures::try_join!(read, lifetime, sending.map(Ok).forward(wire.send))?;
        Ok(())
    };
    Ok((connection, drive))
}

fn endpoint(out: mpsc::Sender<Bytes>, binary: bool) -> (mpsc::Sender<Bytes>, Channel) {
    let (input, receive) = mpsc::channel(32);
    let mut channel = Channel::new(
        out.sink_map_err(|_| io::ErrorKind::BrokenPipe.into()),
        receive.map(Ok),
    );
    channel.binary_input = binary;
    (input, channel)
}
