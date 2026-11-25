//! Linux loopback connection-stage benchmark. Run `connection-cost STAGE COUNT`.
//! Each endpoint runs in its own process and reports readiness after COUNT handshakes.
//! Run in a dedicated cgroup v2 scope (e.g. `systemd-run --user --scope`) for
//! aggregate charged kernel memory. Socket buffer counters come from iproute2 ss.
use anyhow::{Context, Result, ensure};
use plexus::{
    Connection, Server,
    tls::{Dial, Identity, Listen},
    ws,
};
use serde_json::{Value, json};
use std::{
    any::Any,
    env, fs,
    io::{BufRead, BufReader},
    net::SocketAddr,
    process::{Child, Command, Stdio},
    sync::Arc,
};
use tokio::net::{TcpListener, TcpStream};

fn pss(pid: u32) -> Result<u64> {
    let text = fs::read_to_string(format!("/proc/{pid}/smaps_rollup"))?;
    Ok(text
        .lines()
        .find_map(|line| line.strip_prefix("Pss:"))
        .context("missing Pss")?
        .split_whitespace()
        .next()
        .context("missing Pss value")?
        .parse()?)
}

// A shared cgroup can include unrelated activity, so suppress its counters.
fn cgroup_kernel(pids: [u32; 3]) -> Option<i64> {
    let membership = fs::read_to_string("/proc/self/cgroup").ok()?;
    let group = membership
        .lines()
        .find_map(|line| line.strip_prefix("0::"))?;
    let dir = std::path::Path::new("/sys/fs/cgroup").join(group.trim_start_matches('/'));
    let members = fs::read_to_string(dir.join("cgroup.procs")).ok()?;
    let members: Vec<u32> = members
        .split_whitespace()
        .map(str::parse)
        .collect::<Result<_, _>>()
        .ok()?;
    if members.len() != pids.len() || !pids.iter().all(|p| members.contains(p)) {
        return None;
    }
    let children = fs::read_to_string(dir.join("cgroup.stat")).ok()?;
    if !children.lines().any(|line| line == "nr_descendants 0") {
        return None;
    }
    let stat = fs::read_to_string(dir.join("memory.stat")).ok()?;
    stat.lines()
        .find_map(|line| line.strip_prefix("kernel "))?
        .parse()
        .ok()
}

fn kernel(port: u16) -> Result<Value> {
    let output = Command::new("ss")
        .args([
            "-Htnm",
            "state",
            "established",
            &format!("( sport = :{port} or dport = :{port} )"),
        ])
        .output()?;
    ensure!(output.status.success(), "ss failed");
    let mut memory = [0u64; 5];
    let mut sockets = 0;
    for part in std::str::from_utf8(&output.stdout)?
        .split("skmem:(")
        .skip(1)
    {
        let fields: Vec<_> = part.split(')').next().unwrap().split(',').collect();
        for (sum, prefix) in memory.iter_mut().zip(["r", "t", "f", "w", "o"]) {
            let value = fields
                .iter()
                .filter_map(|field| field.strip_prefix(prefix))
                .find_map(|value| value.parse::<u64>().ok())
                .context("missing ss memory counter")?;
            *sum += value;
        }
        sockets += 1;
    }
    Ok(
        json!({"endpoints":sockets,"rmem_alloc":memory[0],"wmem_alloc":memory[1],
        "fwd_alloc":memory[2],"wmem_queued":memory[3],"optmem":memory[4]}),
    )
}

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn launch(
    role: &str,
    stage: &str,
    n: usize,
    addr: &str,
) -> Result<(Process, BufReader<std::process::ChildStdout>)> {
    let mut child = Command::new(env::current_exe()?)
        .args([stage, &n.to_string(), role, addr])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    let out = BufReader::new(child.stdout.take().unwrap());
    Ok((Process(child), out))
}
fn line(reader: &mut impl BufRead) -> Result<String> {
    let mut s = String::new();
    ensure!(reader.read_line(&mut s)? > 0, "endpoint exited");
    Ok(s.trim().to_owned())
}
fn measure(stage: &str, n: usize) -> Result<()> {
    let (server, mut srv) = launch("server", stage, n, "127.0.0.1:0")?;
    let addr: SocketAddr = line(&mut srv)?.parse()?;
    let (client, mut cli) = launch("client", stage, n, &addr.to_string())?;
    ensure!(line(&mut cli)? == "ready", "generator initialization");
    let server_base = pss(server.0.id())?;
    let client_base = pss(client.0.id())?;
    let pids = [std::process::id(), server.0.id(), client.0.id()];
    let kernel_base = cgroup_kernel(pids);
    use std::io::Write;
    let mut client = client;
    client.0.stdin.as_mut().unwrap().write_all(b"go\n")?;
    ensure!(
        line(&mut cli)? == "full" && line(&mut srv)? == "full",
        "endpoint handshake failure"
    );
    let cost = |pid, base| -> Result<Value> {
        let full = pss(pid)?;
        Ok(
            json!({"base_pss_kib":base,"full_pss_kib":full,"delta_kib_per_connection":(full as f64-base as f64)/n as f64}),
        )
    };
    // Allow deferred socket accounting and final handshake ACKs to settle.
    std::thread::sleep(std::time::Duration::from_secs(1));
    let kernel_delta = kernel_base
        .zip(cgroup_kernel(pids))
        .map(|(before, after)| after - before);
    let kernel = kernel(addr.port())?;
    ensure!(
        kernel["endpoints"].as_u64() == Some(2 * n as u64),
        "wrong established socket count"
    );
    println!(
        "{}",
        json!({"stage":stage,"connections":n,"server":cost(server.0.id(),server_base)?,
        "generator":cost(client.0.id(),client_base)?,"socket_memory_bytes":kernel,"both_endpoints_cgroup_kernel_delta_bytes":kernel_delta})
    );
    Ok(())
}
async fn endpoint(stage: &str, n: usize, role: &str, addr: SocketAddr) -> Result<()> {
    let host = Identity::from_seed([1; 32])?;
    let guest = Identity::from_seed([2; 32])?;
    let listen = Listen::new(&host, [guest.principal()])?;
    let dial = Dial::new(&guest, host.principal())?;
    let mut service = Server::new();
    service.hello("benchmark");
    let service = Arc::new(service);
    let mut held: Vec<Box<dyn Any>> = Vec::new();
    if role == "server" {
        let listener = TcpListener::bind(addr).await?;
        println!("{}", listener.local_addr()?);
        for _ in 0..n {
            let tcp = listener.accept().await?.0;
            let item: Box<dyn Any> = match stage {
                "tcp" => Box::new(tcp),
                "tls" => Box::new(listen.accept_tls(tcp).await?.0),
                "ws" => Box::new(ws::accept(tcp).await?),
                "wss" => Box::new(listen.accept(tcp).await?.0),
                "idle" => Box::new(tokio::spawn(service.serve(listen.accept(tcp).await?.0))),
                _ => unreachable!(),
            };
            held.push(item);
        }
    } else {
        println!("ready");
        line(&mut std::io::stdin().lock())?;
        let url = format!("wss://{addr}/");
        for _ in 0..n {
            let item: Box<dyn Any> = match stage {
                "tcp" => Box::new(TcpStream::connect(addr).await?),
                "tls" => Box::new(
                    dial.connect_tls("localhost".try_into()?, TcpStream::connect(addr).await?)
                        .await?
                        .0,
                ),
                "ws" => Box::new(ws::connect(&format!("ws://{addr}/")).await?),
                "wss" => Box::new(dial.connect(&url).await?.0),
                "idle" => {
                    let (connection, drive) = Connection::new(dial.connect(&url).await?.0, None);
                    let driver = tokio::spawn(drive);
                    connection.ready().await?;
                    Box::new((connection, driver))
                }
                _ => unreachable!(),
            };
            held.push(item);
        }
    }
    println!("full");
    line(&mut std::io::stdin().lock())?;
    drop(held);
    Ok(())
}
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let args: Vec<_> = env::args().collect();
    let stage = args
        .get(1)
        .context("expected tcp|tls|ws|wss|idle and a connection count")?;
    ensure!(
        matches!(&**stage, "tcp" | "tls" | "ws" | "wss" | "idle"),
        "invalid stage"
    );
    let n = args.get(2).context("missing count")?.parse()?;
    ensure!(n > 0, "count must be positive");
    if args.len() == 3 {
        measure(stage, n)
    } else {
        endpoint(stage, n, &args[3], args[4].parse()?).await
    }
}
