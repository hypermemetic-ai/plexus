//! Start a local kiln with an authenticated /mcp endpoint.
use std::sync::Arc;

use ::kiln::kiln;
use axum::http::{HeaderValue, header::AUTHORIZATION};
use axum::{Router, routing::get};
use plexus::{Caller, Server};
use plexus_layers_mcp::http::{self as mcp, HttpOrigins};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut server = Server::new();
    kiln::serve(kiln::Kiln::default(), &mut server, "kiln");

    // Single-operator example. An application can use its session verifier here.
    let token = std::env::var("KILN_MCP_TOKEN")?;
    if token.trim().is_empty() {
        return Err("KILN_MCP_TOKEN must contain a bearer token".into());
    }
    let expected = HeaderValue::from_str(&format!("Bearer {token}"))?;
    let origins = HttpOrigins::default().allow_origin("http://127.0.0.1:3000".parse()?)?;
    let mcp = mcp::router(Arc::new(server), origins, move |request| {
        let valid = request.headers.get(AUTHORIZATION) == Some(&expected);
        async move { valid.then(|| Caller::new("kiln-operator")) }
    });

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .merge(mcp);
    let listener = TcpListener::bind("127.0.0.1:3000").await?;
    axum::serve(listener, app).await?;
    Ok(())
}
