//! Listener selection for an activation's native entry point.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::Server;

pub(crate) enum Listener {
    Stdio,
    Ws(SocketAddr),
    #[cfg(feature = "tls")]
    Wss(SocketAddr, crate::tls::Listen),
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

impl Listener {
    pub(crate) fn from_args(mut args: impl Iterator<Item = String>) -> io::Result<Self> {
        let mut plain = None;
        #[cfg(feature = "tls")]
        let (mut secure, mut key_file, mut pins) = (None, None, Vec::new());
        #[cfg(feature = "store")]
        let mut data = None;
        while let Some(flag) = args.next() {
            let value = args
                .next()
                .ok_or_else(|| invalid(format!("missing value for {flag}")))?;
            let addr = || {
                value
                    .parse::<SocketAddr>()
                    .map_err(|e| invalid(format!("{flag}: {e}")))
            };
            match flag.as_str() {
                "--ws" => plain = Some(addr()?),
                #[cfg(feature = "store")]
                "--data" => data = Some(value.into()),
                #[cfg(feature = "tls")]
                "--wss" => secure = Some(addr()?),
                #[cfg(feature = "tls")]
                "--key-file" => key_file = Some(value),
                #[cfg(feature = "tls")]
                "--allow-key" => pins.push(value),
                #[cfg(not(feature = "tls"))]
                "--wss" | "--key-file" | "--allow-key" => {
                    return Err(invalid(format!("{flag} requires the tls feature")));
                }
                _ => return Err(invalid(format!("unknown listener option {flag}"))),
            }
        }
        let listener = plain.map_or(Self::Stdio, Self::Ws);
        #[cfg(feature = "tls")]
        let listener = match secure {
            Some(_) if plain.is_some() => return Err(invalid("choose either --ws or --wss")),
            Some(addr) => {
                let path = key_file.ok_or_else(|| invalid("--wss requires --key-file PATH"))?;
                if pins.is_empty() {
                    return Err(invalid("--wss requires at least one --allow-key PRINCIPAL"));
                }
                let identity = crate::tls::Identity::from_key_file(path)?;
                Self::Wss(
                    addr,
                    crate::tls::Listen::new(&identity, pins.iter().map(String::as_str))?,
                )
            }
            None if key_file.is_some() || !pins.is_empty() => {
                return Err(invalid("--key-file and --allow-key require --wss"));
            }
            None => listener,
        };
        #[cfg(feature = "store")]
        if let Some(dir) = data {
            crate::store::set_data(dir);
        }
        Ok(listener)
    }

    pub(crate) async fn serve(self, server: Arc<Server>) -> io::Result<()> {
        match self {
            Self::Stdio => return server.serve(crate::stdio::stdio()).await,
            Self::Ws(addr) => println!("ws://{}/", crate::ws::spawn(addr, server).await?),
            #[cfg(feature = "tls")]
            Self::Wss(addr, listener) => println!("wss://{}/", listener.spawn(addr, server).await?),
        }
        std::future::pending().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> io::Result<Listener> {
        Listener::from_args(args.iter().map(|s| s.to_string()))
    }

    #[test]
    fn plain_listeners_keep_their_entry_points() {
        assert!(matches!(parse(&[]).unwrap(), Listener::Stdio));
        assert!(matches!(
            parse(&["--ws", "127.0.0.1:0"]).unwrap(),
            Listener::Ws(_)
        ));
        assert!(parse(&["--ws"]).is_err());
        assert!(parse(&["--ws", "invalid"]).is_err());
    }

    #[test]
    fn partial_or_mixed_tls_configuration_fails_before_binding() {
        for args in [
            &["--wss", "127.0.0.1:0"][..],
            &["--wss", "127.0.0.1:0", "--key-file", "missing"],
            &[
                "--ws",
                "127.0.0.1:0",
                "--wss",
                "127.0.0.1:0",
                "--key-file",
                "missing",
                "--allow-key",
                "unused",
            ],
            &["--key-file", "missing"],
            &["--ws", "127.0.0.1:0", "--allow-key", "unused"],
        ] {
            assert_eq!(
                parse(args).err().expect("invalid listener").kind(),
                io::ErrorKind::InvalidInput
            );
        }
    }
}
