//! MCP over stdio for a native Layers child process or a pinned WSS service.
use plexus::Connection;
use plexus::tls::{Dial, Identity};
use plexus_layers_mcp::LayersMcp;
use rmcp::ServiceExt;
use std::error::Error;
use std::ffi::OsString;
use std::time::Duration;
use tokio::process::Command;

const USAGE: &str = "usage: plexus-layers-mcp [--place MOUNT] -- COMMAND [ARG ...]\n       plexus-layers-mcp [--place MOUNT] --wss URL --key-file FILE --server-key PRINCIPAL";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args_os().skip(1).peekable();
    if args
        .peek()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        println!("{USAGE}");
        return Ok(());
    }
    let mut place = None;
    let mut wss = None;
    let mut key_file = None;
    let mut pin = None;
    let mut command: Vec<OsString> = Vec::new();
    while let Some(option) = args.next() {
        if option == "--" {
            command.extend(args.by_ref());
            if command.is_empty() {
                return Err(USAGE.into());
            }
            break;
        }
        let value = args.next().ok_or(USAGE)?;
        match option.to_str() {
            Some("--place") if place.is_none() => {
                place = Some(value.into_string().map_err(|_| USAGE)?)
            }
            Some("--wss") if wss.is_none() => wss = Some(value.into_string().map_err(|_| USAGE)?),
            Some("--key-file") if key_file.is_none() => key_file = Some(value),
            Some("--server-key") if pin.is_none() => {
                pin = Some(value.into_string().map_err(|_| USAGE)?)
            }
            _ => return Err(USAGE.into()),
        }
    }
    let (channel, mut child) = match (command.split_first(), wss, key_file, pin) {
        (Some((program, argv)), None, None, None) => {
            let (child, channel) =
                plexus::stdio::child(Command::new(program).args(argv).kill_on_drop(true))?;
            (channel, Some(child))
        }
        (None, Some(url), Some(key), Some(pin)) if url.starts_with("wss://") => {
            let identity = Identity::from_key_file(std::path::Path::new(&key))?;
            let (channel, _) = Dial::new(&identity, &pin)?.connect(&url).await?;
            (channel, None)
        }
        _ => return Err(USAGE.into()),
    };
    let (connection, driver) = Connection::new(channel, None);
    let driver = tokio::spawn(driver);
    let service = match place {
        Some(place) => LayersMcp::at(connection, &place)?,
        None => LayersMcp::new(connection),
    };
    let result = service
        .serve(rmcp::transport::stdio())
        .await?
        .waiting()
        .await;
    // Drop the native channel before waiting on a child reading its stdin.
    driver.abort();
    let _ = driver.await;
    if let Some(ref mut child) = child
        && tokio::time::timeout(Duration::from_secs(2), child.wait())
            .await
            .is_err()
    {
        child.kill().await?;
    }
    result?;
    Ok(())
}
