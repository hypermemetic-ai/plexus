//! Serve the application's live native registry over MCP.
use std::{future::Future, io, net::SocketAddr};

use axum::http::{header::AUTHORIZATION, request::Parts};
use plexus::{Application, Caller};
use subtle::ConstantTimeEq;
use tokio::net::TcpListener;

use crate::http::{self, HttpOrigins};

/// A listener address, or a socket the caller has already bound.
pub enum McpListener {
    Address(SocketAddr),
    Bound(TcpListener),
}

impl From<SocketAddr> for McpListener {
    fn from(address: SocketAddr) -> Self {
        Self::Address(address)
    }
}

impl From<TcpListener> for McpListener {
    fn from(listener: TcpListener) -> Self {
        Self::Bound(listener)
    }
}

/// Add an MCP HTTP listener to a native Plexus application.
pub trait Mcp: Sized {
    /// Serve /mcp on 127.0.0.1:3000. Clients send the configured
    /// PLEXUS_MCP_TOKEN in the Authorization header using the Bearer scheme.
    /// A verified request runs as local:mcp-operator under the native grants.
    fn mcp(self) -> Self;

    /// Serve /mcp using the host's verifier. A pre-bound Tokio listener can
    /// be supplied to reserve a port, including an OS-assigned port.
    fn mcp_with<L, F, A>(self, listener: L, origins: HttpOrigins, resolve: F) -> Self
    where
        L: Into<McpListener>,
        F: Fn(Parts) -> A + Send + Sync + 'static,
        A: Future<Output = Option<Caller>> + Send + 'static;
}

impl Mcp for Application {
    fn mcp(self) -> Self {
        let token = match std::env::var("PLEXUS_MCP_TOKEN") {
            Ok(token) if valid_token(&token) => token,
            _ => {
                return self.with_startup_error(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "PLEXUS_MCP_TOKEN must contain a nonempty HTTP bearer token",
                ));
            }
        };
        let address = SocketAddr::from(([127, 0, 0, 1], 3000));
        let origins = HttpOrigins::default()
            .allow_origin("http://127.0.0.1:3000".parse().expect("static URI"))
            .expect("static origin");
        self.mcp_with(address, origins, move |parts| {
            let authenticated = verify_bearer(&parts, token.as_bytes());
            async move { authenticated.then(|| Caller::new("local:mcp-operator")) }
        })
    }

    fn mcp_with<L, F, A>(self, listener: L, origins: HttpOrigins, resolve: F) -> Self
    where
        L: Into<McpListener>,
        F: Fn(Parts) -> A + Send + Sync + 'static,
        A: Future<Output = Option<Caller>> + Send + 'static,
    {
        let listener = listener.into();
        self.with_listener(move |server| async move {
            let listener = match listener {
                McpListener::Address(address) => TcpListener::bind(address).await?,
                McpListener::Bound(listener) => listener,
            };
            axum::serve(listener, http::router(server, origins, resolve)).await
        })
    }
}

fn valid_token(token: &str) -> bool {
    // RFC 6750 b64token permits padding only after the last non-padding byte.
    let core = token.trim_end_matches('=');
    !core.is_empty()
        && core
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-._~+/".contains(&b))
}

fn verify_bearer(parts: &Parts, expected: &[u8]) -> bool {
    let mut headers = parts.headers.get_all(AUTHORIZATION).iter();
    let Some(value) = headers.next().and_then(|v| v.to_str().ok()) else {
        return false;
    };
    let Some((scheme, token)) = value.split_once(' ') else {
        return false;
    };
    headers.next().is_none()
        && scheme.eq_ignore_ascii_case("bearer")
        && bool::from(token.trim_start_matches(' ').as_bytes().ct_eq(expected))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;

    #[test]
    fn local_operator_requires_one_valid_bearer() {
        for token in ["", " ", "a b", "=tail", "a=b", "a\n"] {
            assert!(!valid_token(token));
        }
        let expected = "aZ9-._~+/==";
        assert!(valid_token(expected));
        for (headers, allowed) in [
            (vec!["Bearer aZ9-._~+/=="], true),
            (vec!["bearer   aZ9-._~+/=="], true),
            (vec!["Basic aZ9-._~+/=="], false),
            (vec!["Bearer wrong"], false),
            (vec!["Bearer aZ9-._~+/==", "Bearer aZ9-._~+/=="], false),
            (vec![], false),
        ] {
            let mut request = Request::builder();
            for header in headers {
                request = request.header(AUTHORIZATION, header);
            }
            let parts = request.body(()).unwrap().into_parts().0;
            assert_eq!(verify_bearer(&parts, expected.as_bytes()), allowed);
        }
    }
}
