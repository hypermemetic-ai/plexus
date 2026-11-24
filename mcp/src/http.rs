//! Stateless MCP over HTTP. The host authenticates each HTTP request before a
//! connection is opened on behalf of its verified native caller.
use std::{future::Future, sync::Arc};

use axum::{
    Router,
    body::Body,
    http::{HeaderMap, Request, StatusCode, Uri, header, request::Parts, uri::Authority},
    response::IntoResponse,
    routing::any,
};
use plexus::{Caller, Connection, Server};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::never::NeverSessionManager,
};

use crate::LayersMcp;

/// Loopback HTTP clients can connect by default. Browser clients must present
/// an explicitly configured origin. Missing Origin is normal for native MCP clients.
#[derive(Clone, Default)]
pub struct HttpOrigins {
    origins: Vec<Uri>,
}

fn valid_authority(authority: &Authority) -> bool {
    !authority.as_str().contains('@')
        && !authority.host().is_empty()
        && !authority.host().contains('*')
        && (authority.as_str() == authority.host() || authority.port_u16().is_some())
}

impl HttpOrigins {
    /// Admit an HTTP(S) origin and its listener authority. The port defaults
    /// to the scheme's standard port and always participates in origin matching.
    pub fn allow_origin(mut self, origin: Uri) -> Result<Self, &'static str> {
        let authority = origin.authority().ok_or("expected origin authority")?;
        if !matches!(origin.scheme_str(), Some("http" | "https"))
            || origin.path() != "/"
            || origin.query().is_some()
            || !valid_authority(authority)
        {
            return Err("expected an exact HTTP or HTTPS origin");
        }
        self.origins.push(origin);
        Ok(self)
    }

    fn port(origin: &Uri) -> u16 {
        origin
            .port_u16()
            .unwrap_or(if origin.scheme_str() == Some("https") {
                443
            } else {
                80
            })
    }

    fn permits(&self, parts: &Parts) -> bool {
        let mut hosts = parts.headers.get_all(header::HOST).iter();
        let authority = match hosts.next() {
            Some(host) => match host.to_str().ok().and_then(|s| s.parse::<Authority>().ok()) {
                Some(host) if hosts.next().is_none() => host,
                _ => return false,
            },
            None => match parts.uri.authority() {
                Some(host) => host.clone(),
                None => return false,
            },
        };
        if !valid_authority(&authority) {
            return false;
        }
        let host = authority.host();
        let local = ["localhost", "127.0.0.1", "[::1]"]
            .iter()
            .any(|name| host.eq_ignore_ascii_case(name));
        let configured = self.origins.iter().any(|origin| {
            host.eq_ignore_ascii_case(origin.host().unwrap_or_default())
                && authority
                    .port_u16()
                    .unwrap_or(if origin.scheme_str() == Some("https") {
                        443
                    } else {
                        80
                    })
                    == Self::port(origin)
        });
        if !local && !configured {
            return false;
        }
        let mut origins = parts.headers.get_all(header::ORIGIN).iter();
        let Some(origin) = origins.next() else {
            return true;
        };
        let Ok(source) = origin.to_str().unwrap_or("").parse::<Uri>() else {
            return false;
        };
        origins.next().is_none()
            && source.authority().is_some_and(valid_authority)
            && source.path() == "/"
            && source.query().is_none()
            && self.origins.iter().any(|allowed| {
                allowed.scheme() == source.scheme()
                    && allowed
                        .host()
                        .unwrap_or_default()
                        .eq_ignore_ascii_case(source.host().unwrap_or_default())
                    && Self::port(allowed) == Self::port(&source)
            })
    }
}

/// Exposes `/mcp`. The resolver receives the original HTTP parts, including
/// credentials and transport extensions. Return None for an unauthenticated
/// request. Only protocol headers reach the MCP handler.
pub fn router<F, A>(server: Arc<Server>, origins: HttpOrigins, resolve: F) -> Router
where
    F: Fn(Parts) -> A + Send + Sync + 'static,
    A: Future<Output = Option<Caller>> + Send + 'static,
{
    let resolve = Arc::new(resolve);
    let mut config = StreamableHttpServerConfig::default()
        .with_legacy_session_mode(false)
        .with_json_response(true)
        .enforce_origin_validation();
    for origin in &origins.origins {
        let host = origin.host().expect("validated origin");
        config.allowed_hosts.push(host.to_owned());
        config.allowed_origins.push(format!(
            "{}://{}:{}",
            origin.scheme_str().unwrap(),
            host,
            HttpOrigins::port(origin)
        ));
    }
    Router::new().route(
        "/mcp",
        any(move |req: Request<Body>| {
            let (parts, body) = req.into_parts();
            let allowed = origins.permits(&parts);
            let resolve = resolve.clone();
            let server = server.clone();
            let config = config.clone();
            async move {
                if !allowed {
                    return StatusCode::FORBIDDEN.into_response();
                }
                let mut protocol = Request::new(body);
                *protocol.method_mut() = parts.method.clone();
                *protocol.uri_mut() = parts.uri.clone();
                *protocol.version_mut() = parts.version;
                *protocol.headers_mut() = protocol_headers(&parts.headers);
                let Some(caller) = resolve(parts).await else {
                    return StatusCode::UNAUTHORIZED.into_response();
                };
                let service = StreamableHttpService::new(
                    move || {
                        let (near, far) = plexus::pair();
                        tokio::spawn(server.serve_as(far, caller.clone()));
                        let (connection, drive) = Connection::new(near, None);
                        tokio::spawn(drive);
                        Ok(LayersMcp::new(connection))
                    },
                    Arc::new(NeverSessionManager::default()),
                    config,
                );
                service.handle(protocol).await.into_response()
            }
        }),
    )
}

fn protocol_headers(headers: &HeaderMap) -> HeaderMap {
    let mut kept = HeaderMap::new();
    for (name, value) in headers {
        if matches!(
            name.as_str(),
            "host"
                | "origin"
                | "accept"
                | "content-type"
                | "content-length"
                | "last-event-id"
                | "mcp-session-id"
                | "mcp-protocol-version"
                | "mcp-method"
                | "mcp-name"
        ) || name.as_str().starts_with("mcp-param-")
        {
            kept.append(name.clone(), value.clone());
        }
    }
    kept
}
