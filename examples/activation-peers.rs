//! A worker behind an outbound connection supplies Studio's named kiln.
use plexus::{Caller, Connection, PeerHost, PeerKeys, RunningPeers, tls::Identity};
use std::{error::Error, io, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Command,
};

#[plexus::activation]
pub mod kiln {
    use plexus::Caller;
    pub struct Kiln;
    impl Kiln {
        pub fn fire(&self, degrees: u32, caller: &Caller) -> String {
            format!("{degrees} degrees for {}", caller.principal())
        }
    }
}

#[plexus::activation]
pub mod studio {
    use super::kiln::Kiln;
    use plexus::Peer;
    pub struct Studio;
    impl Studio {
        pub async fn fire(
            &self,
            #[peer("north")] kiln: Peer<Kiln>,
            degrees: u32,
        ) -> Result<String, String> {
            kiln.fire(degrees).await.map_err(|e| e.to_string())
        }
        pub fn status(&self) -> String {
            "ready".into()
        }
    }
}

type Result<T> = std::result::Result<T, Box<dyn Error>>;
fn rpc(host: &RunningPeers) -> Connection {
    let (client, wire) = plexus::pair();
    tokio::spawn(host.server().serve_as(wire, Caller::new("user:ada")));
    let (client, drive) = Connection::new(client, None);
    tokio::spawn(drive);
    client
}

async fn fire(client: &Connection, studio_pin: &str) -> Result<()> {
    let answer = client
        .call("studio.fire", serde_json::json!({"degrees":900}))
        .await?;
    let fired: String = serde_json::from_str(answer.get())?;
    if fired != format!("900 degrees for {studio_pin}") {
        return Err(io::Error::other("unexpected peer identity or result").into());
    }
    println!("studio.fire(degrees=900) → {fired}");
    Ok(())
}

async fn configured(mode: &str, url: &str, dir: &str, identity: &str, pin: &str) -> Result<()> {
    let keys = PeerKeys::new(dir);
    let me = keys.identity(identity)?;
    let pin = keys.pin(pin)?;
    match mode {
        "studio" => {
            let own = me.principal().to_owned();
            let mut studio = PeerHost::new(studio::Studio, me);
            studio.peer::<kiln::Kiln>("north").accept(url, pin)?;
            studio.requires_peer("north")?;
            let studio = studio.start().await?;
            println!("Studio is listening at {url}, waiting for north.");
            studio.ready().await;
            fire(&rpc(&studio), &own).await
        }
        "kiln" => {
            let mut kiln = PeerHost::new(kiln::Kiln, me);
            kiln.peer::<studio::Studio>("studio").dial(url, pin)?;
            kiln.requires_peer("studio")?;
            let kiln = kiln.start().await?;
            kiln.ready().await;
            println!("Kiln is connected. Press Ctrl-C to stop.");
            std::future::pending().await
        }
        _ => Err("expected studio or kiln".into()),
    }
}

async fn local(controller: Option<&str>) -> Result<()> {
    let me = Identity::generate()?;
    if let Some(controller_pin) = controller {
        println!("{}", me.principal());
        let mut input = BufReader::new(tokio::io::stdin()).lines();
        let url = input
            .next_line()
            .await?
            .ok_or("missing controller address")?;
        let mut kiln = PeerHost::new(kiln::Kiln, me);
        kiln.peer::<studio::Studio>("studio")
            .dial(&url, controller_pin)?;
        kiln.requires_peer("studio")?;
        let host = kiln.start().await?;
        host.ready().await;
        input.next_line().await?;
        return Ok(());
    }
    let studio_pin = me.principal().to_string();
    let mut worker = Command::new(std::env::current_exe()?)
        .args(["local-worker", &studio_pin])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let worker_pin = BufReader::new(worker.stdout.take().ok_or("worker stdout")?)
        .lines()
        .next_line()
        .await?
        .ok_or("missing worker pin")?;
    let mut studio = PeerHost::new(studio::Studio, me);
    studio
        .peer::<kiln::Kiln>("north")
        .accept("wss://127.0.0.1:0", worker_pin)?;
    studio.requires_peer("north")?;
    let host = studio.start().await?;
    let client = rpc(&host);
    let offline = client.call("studio.status", ()).await.unwrap_err();
    if offline.code() != Some("unavailable") {
        return Err(offline.into());
    }
    println!("Studio is waiting for its required north kiln.");
    let addr = host.listen_addr("127.0.0.1:0").ok_or("missing listener")?;
    let mut command = worker.stdin.take().ok_or("worker stdin")?;
    command
        .write_all(format!("wss://{addr}/\n").as_bytes())
        .await?;
    host.ready().await;
    fire(&client, &studio_pin).await?;
    drop(command);
    if !worker.wait().await?.success() {
        return Err("worker failed".into());
    }
    Ok(())
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [] => tokio::time::timeout(Duration::from_secs(15), local(None)).await?,
        [mode, pin] if mode == "local-worker" => {
            tokio::time::timeout(Duration::from_secs(15), local(Some(pin))).await?
        }
        [mode, url, dir, identity, pin] => configured(mode, url, dir, identity, pin).await,
        _ => Err("expected no arguments, or studio|kiln WSS_URL KEYSTORE IDENTITY PEER_PIN".into()),
    }
}
