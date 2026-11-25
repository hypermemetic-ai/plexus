//! One worker-initiated, mutually pinned WebSocket, with calls in both directions.
use std::{
    error::Error,
    io,
    process::Stdio,
    sync::{Arc, OnceLock},
    time::Duration,
};

use futures::{StreamExt, stream};
use plexus::{
    Caller, Channel, Connection, Server,
    peer::{self, Side},
    tls::{Dial, Identity, Listen},
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
type Authenticated = (Channel, String);
const CONTROLLER: &str = "demo-controller/1";
const WORKER: &str = "demo-worker/1";

async fn controller((wire, pin): Authenticated) -> Result<()> {
    let expected = pin.clone();
    let mut service = Server::new();
    service.hello(CONTROLLER);
    service.method_with_caller("controller.identify", move |_, context| {
        let allowed = context
            .caller()
            .is_some_and(|who| who.principal() == expected);
        async move {
            if !allowed {
                return Err(plexus::fail("forbidden", "unknown worker", ()));
            }
            plexus::raw(&"controller acknowledged worker")
        }
    });
    let (worker, drive) = peer::connect(
        wire,
        Side::Accept,
        Caller::new(pin),
        Arc::new(service),
        WORKER,
    )
    .await?;
    let driver = tokio::spawn(drive);
    worker.ready().await?;
    let mut watch = worker.stream("worker.watch", ())?;
    let first = watch.next().await.ok_or("watch ended early")??;
    if first.get() != "1" {
        return Err("wrong watch value".into());
    }
    println!("controller received worker.watch = 1 (stream open)");
    let answer = worker.call("worker.check_in", ()).await?;
    let message: String = serde_json::from_str(answer.get())?;
    if message != "controller acknowledged worker" {
        return Err("wrong callback result".into());
    }
    println!("worker called controller.identify over the same connection: {message}");
    drop(watch);
    let answer = worker.call("worker.status", ()).await?;
    if answer.get() != r#""ready""# {
        return Err("wrong worker status".into());
    }
    println!("controller cancelled the watch; worker.status = ready");
    drop(worker);
    driver.await??;
    Ok(())
}

async fn worker((wire, pin): Authenticated) -> Result<()> {
    let parent = Arc::new(OnceLock::<Connection>::new());
    let callback = parent.clone();
    let mut service = Server::new();
    service.hello(WORKER);
    service.stream("worker.watch", |_| {
        stream::once(async { plexus::raw(&1) }).chain(stream::pending())
    });
    service.method("worker.status", |_| async { plexus::raw(&"ready") });
    service.method("worker.check_in", move |_| {
        let conn = callback.get().expect("installed before dispatch").clone();
        async move {
            conn.call("controller.identify", ())
                .await
                .map_err(|e| plexus::fail("callback", &e.to_string(), ()))
        }
    });
    let (conn, drive) = peer::connect(
        wire,
        Side::Dial,
        Caller::new(pin),
        Arc::new(service),
        CONTROLLER,
    )
    .await?;
    parent.set(conn).ok().expect("first connection");
    drive.await?;
    Ok(())
}

// The self-contained demo provisions an ephemeral worker pin through the child
// pipe it owns. Two-machine mode takes both pins from operator configuration.
async fn local() -> Result<()> {
    let me = Identity::generate()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("wss://{}/peer", listener.local_addr()?);
    println!("starting worker process, dialing {url}");
    let mut child = Command::new(std::env::current_exe()?)
        .args(["local-worker", me.principal(), &url])
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let mut read = BufReader::new(child.stdout.take().ok_or("missing child stdout")?);
    let mut pin = String::new();
    read.read_line(&mut pin).await?;
    let listen = Listen::new(&me, [pin.trim_end()])?;
    controller(listen.accept(listener.accept().await?.0).await?).await?;
    if !child.wait().await?.success() {
        return Err("worker process failed".into());
    }
    Ok(())
}

async fn run(args: &[String]) -> Result<()> {
    match args {
        [] => local().await,
        [mode, pin, url] if mode == "local-worker" => {
            let me = Identity::generate()?;
            println!("{}", me.principal());
            worker(Dial::new(&me, pin)?.connect(url).await?).await
        }
        [mode, key] if mode == "principal" => {
            println!("{}", Identity::from_key_file(key)?.principal());
            Ok(())
        }
        [mode, key, peer, bind] if mode == "controller" => {
            let listen = Listen::new(&Identity::from_key_file(key)?, [peer.as_str()])?;
            let listener = tokio::net::TcpListener::bind(bind).await?;
            eprintln!("listening on {}", listener.local_addr()?);
            controller(listen.accept(listener.accept().await?.0).await?).await
        }
        [mode, key, peer, url] if mode == "worker" => {
            worker(Dial::new(&Identity::from_key_file(key)?, peer)?.connect(url).await?).await
        }
        _ => Err(io::Error::other("expected no arguments, principal KEY, controller KEY WORKER_PIN ADDR, or worker KEY CONTROLLER_PIN WSS_URL").into()),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.is_empty() {
        tokio::time::timeout(Duration::from_secs(20), run(&args)).await?
    } else {
        run(&args).await
    }
}
