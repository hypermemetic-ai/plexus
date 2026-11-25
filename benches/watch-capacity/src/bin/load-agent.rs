//! Foreground, pinned control service for one locally bounded benchmark generator.
#[path = "load_agent/supervisor.rs"]
mod supervisor;

use anyhow::{Context, Result, bail, ensure};
use plexus::tls::{Dial, Identity, Listen};
use serde_json::{Value, json};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    net::SocketAddr,
    os::unix::fs::OpenOptionsExt,
    path::Path,
    sync::Arc,
    time::Duration,
};
use supervisor::{Supervisor, unix_us};

#[plexus::activation]
pub mod worker {
    use super::{Supervisor, unix_us};
    use std::sync::Arc;

    pub enum Refused {
        Invalid(String),
    }
    pub struct RunStatus {
        pub phase: String,
        pub keys: u32,
        pub sessions: u32,
        pub result: Option<String>,
        pub error: Option<String>,
    }
    pub struct Worker(pub Arc<Supervisor>);
    impl Worker {
        pub fn prepare(
            &self,
            first: u32,
            count: u32,
            total: u32,
            orgs: u32,
            seconds: u32,
        ) -> Result<u64, Refused> {
            self.0
                .prepare(first, count, total, orgs, seconds)
                .map_err(reject)
        }
        pub fn keys(&self, run: u64, offset: u32, limit: u32) -> Result<Vec<String>, Refused> {
            self.0.keys(run, offset, limit).map_err(reject)
        }
        pub fn connect(
            &self,
            run: u64,
            ports: Vec<u16>,
            pin: String,
            profile: String,
            rate: u32,
            read_interval_ms: u32,
        ) -> Result<(), Refused> {
            self.0
                .connect(run, ports, pin, profile, rate, read_interval_ms)
                .map_err(reject)
        }
        pub fn start(&self, run: u64, unix_us: u64) -> Result<(), Refused> {
            self.0.start(run, unix_us).map_err(reject)
        }
        pub fn clock(&self, run: u64) -> Result<u64, Refused> {
            self.0.status(run).map_err(reject)?;
            Ok(unix_us())
        }
        pub fn status(&self, run: u64) -> Result<RunStatus, Refused> {
            self.0.status(run).map_err(reject)
        }
        pub fn drop(&self, run: u64) -> Result<(), Refused> {
            self.0.finish(run).map_err(reject)
        }
        pub fn stop(&self, run: u64) -> Result<(), Refused> {
            self.0.stop(run).map_err(reject)
        }
    }
    fn reject(e: anyhow::Error) -> Refused {
        Refused::Invalid(e.to_string())
    }
}

fn keygen(path: &Path) -> Result<()> {
    let mut seed = [0; 32];
    File::open("/dev/urandom")?.read_exact(&mut seed)?;
    let mut out = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    out.write_all(&seed)?;
    out.sync_all()?;
    println!("{}", Identity::from_seed(seed)?.principal());
    Ok(())
}
async fn serve(
    key: &str,
    bind: SocketAddr,
    controller: &str,
    target: std::net::IpAddr,
    max: u32,
    outbound: bool,
) -> Result<()> {
    ensure!(
        !bind.ip().is_unspecified() && !bind.ip().is_multicast(),
        "bind an explicit interface"
    );
    let identity = Identity::from_key_file(key)?;
    let listener = Listen::new(&identity, [controller])?;
    let executable = std::env::current_exe()?.with_file_name("watch-capacity");
    let supervisor = Supervisor::new(executable, target, max)?;
    let mut server = plexus::Server::new();
    plexus::serve(worker::Worker(supervisor.clone()), &mut server);
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let server = Arc::new(server);
    if outbound {
        ensure!(
            bind.ip() == target && bind.port() != 0,
            "control rendezvous must be on the configured fixture host"
        );
        let listener = Arc::new(listener);
        // Each fixed socket serves one mutually authenticated control session.
        // Reconnect after the controller closes it or the tailnet path changes.
        for _ in 0..2 {
            let (listener, server) = (listener.clone(), server.clone());
            tokio::spawn(async move {
                loop {
                    if let Ok(tcp) = tokio::net::TcpStream::connect(bind).await {
                        if let Ok((channel, peer)) = listener.accept(tcp).await {
                            let _ = server.serve_as(channel, plexus::Caller::new(peer)).await;
                            continue;
                        }
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            });
        }
        println!(
            "{}",
            json!({"rendezvous":bind.to_string(),"principal":identity.principal(),"max_users":max,"target":target})
        );
    } else {
        let addr = listener.spawn(bind, server).await?;
        println!(
            "{}",
            json!({"url":format!("wss://{addr}/"),"principal":identity.principal(),"max_users":max,"target":target})
        );
    }
    std::io::stdout().flush()?;
    tokio::select! { _=tokio::signal::ctrl_c() => (), _=term.recv() => () }
    supervisor.shutdown();
    Ok(())
}

/// Pair a controller socket on loopback with a fixed worker's outbound socket.
/// TLS is end-to-end between the native controller and the worker activation.
async fn rendezvous(bind: SocketAddr, local: SocketAddr, worker: std::net::IpAddr) -> Result<()> {
    ensure!(
        local.ip().is_loopback() && !bind.ip().is_unspecified() && !bind.ip().is_multicast(),
        "use loopback for control and an explicit worker-facing interface"
    );
    let external = tokio::net::TcpListener::bind(bind).await?;
    let internal = tokio::net::TcpListener::bind(local).await?;
    println!(
        "{}",
        json!({"worker":external.local_addr()?.to_string(),"controller":internal.local_addr()?.to_string()})
    );
    std::io::stdout().flush()?;
    loop {
        let (mut peer, address) = external.accept().await?;
        if address.ip() != worker {
            continue;
        }
        let (mut operator, _) = internal.accept().await?;
        tokio::spawn(async move {
            let _ = tokio::io::copy_bidirectional(&mut peer, &mut operator).await;
        });
    }
}

async fn call(key: &str, url: &str, pin: &str, method: &str, params: &str) -> Result<()> {
    let name = method.strip_prefix("worker.").unwrap_or(method);
    ensure!(
        matches!(
            name,
            "prepare" | "keys" | "connect" | "start" | "clock" | "status" | "drop" | "stop"
        ),
        "unknown worker method"
    );
    let params: Value = serde_json::from_str(params)?;
    ensure!(params.is_object(), "expected a JSON parameter object");
    tokio::time::timeout(Duration::from_secs(30), async {
        let identity = Identity::from_key_file(key)?;
        let (channel, _) = Dial::new(&identity, pin)?.connect(url).await?;
        let (conn, drive) = plexus::Connection::new(channel, None);
        let _driving = tokio::spawn(drive);
        conn.ready().await?;
        let value = conn.call(&format!("worker.{name}"), params).await?;
        println!("{}", value.get());
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("control deadline elapsed")?
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [mode, path] if mode == "keygen" => keygen(Path::new(path)),
        [mode, path] if mode == "principal" => {
            println!("{}", Identity::from_key_file(path)?.principal());
            Ok(())
        }
        [mode, key, bind, peer, target, max] if mode == "serve" || mode == "dial-back" => {
            serve(
                key,
                bind.parse()?,
                peer,
                target.parse()?,
                max.parse()?,
                mode == "dial-back",
            )
            .await
        }
        [mode, bind, local, worker] if mode == "rendezvous" => {
            rendezvous(bind.parse()?, local.parse()?, worker.parse()?).await
        }
        [mode, key, url, pin, method, params] if mode == "call" => {
            call(key, url, pin, method, params).await
        }
        _ => bail!(
            "load-agent keygen PATH | principal PATH | serve KEY BIND_SOCKET CONTROLLER_KEY TARGET_IP MAX_USERS | dial-back KEY RYZEN_SOCKET CONTROLLER_KEY TARGET_IP MAX_USERS | rendezvous BIND_SOCKET LOOPBACK_SOCKET WORKER_IP | call KEY WSS_URL WORKER_KEY METHOD JSON"
        ),
    }
}
