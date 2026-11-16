//! The kiln, served over stdio, or with `--ws ADDR` over a WebSocket (the
//! address bound is printed on stdout, one line: `ws://127.0.0.1:PORT/`).

mod activation;

use std::sync::Arc;

fn main() -> std::io::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let server = Arc::new(activation::kiln().0);
    let args: Vec<String> = std::env::args().skip(1).collect();
    runtime.block_on(async move {
        match args.as_slice() {
            [] => server.serve(plexus::stdio::stdio()).await,
            [flag, addr] if flag == "--ws" => {
                let addr = addr.parse().map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
                let bound = plexus::ws::spawn(addr, server).await?;
                println!("ws://{bound}/");
                std::future::pending().await
            }
            _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "usage: kiln [--ws ADDR]")),
        }
    })
}
