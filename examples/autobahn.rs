//! An echo peer for the Autobahn test suite, below the plexus protocol:
//! each message `ws.rs` reads is sent back as it came.
//!
//! `autobahn server 127.0.0.1:9002` answers `wstest -m fuzzingclient`;
//! `autobahn client ws://127.0.0.1:9001 AGENT` runs every case of
//! `wstest -m fuzzingserver` and has it write its report.

use futures::{SinkExt, StreamExt};
use plexus::Channel;

async fn echo(mut channel: Channel) {
    while let Some(Ok(frame)) = channel.receive.next().await {
        if channel.send.send(frame).await.is_err() {
            break;
        }
    }
    let _ = channel.send.close().await;
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("server") => {
            let listener = tokio::net::TcpListener::bind(&args[2]).await?;
            loop {
                let (stream, _) = listener.accept().await?;
                tokio::spawn(async move {
                    if let Ok(channel) = plexus::ws::accept(stream).await {
                        echo(channel).await;
                    }
                });
            }
        }
        Some("client") => {
            let (url, agent) = (&args[2], &args[3]);
            let mut count = plexus::ws::connect(&format!("{url}/getCaseCount")).await?;
            let cases: u32 = String::from_utf8_lossy(&count.receive.next().await.expect("a count")?).parse().expect("a number");
            drop(count);
            for case in 1..=cases {
                if let Ok(channel) = plexus::ws::connect(&format!("{url}/runCase?case={case}&agent={agent}")).await {
                    echo(channel).await;
                }
            }
            let mut report = plexus::ws::connect(&format!("{url}/updateReports?agent={agent}")).await?;
            while report.receive.next().await.is_some() {}
            Ok(())
        }
        _ => panic!("autobahn server ADDR | autobahn client URL AGENT"),
    }
}
