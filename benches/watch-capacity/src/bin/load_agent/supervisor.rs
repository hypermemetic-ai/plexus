use super::worker::RunStatus;
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    fs::File,
    io::{BufRead, BufReader, Read, Write},
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    sync::{Arc, Mutex, MutexGuard, Weak},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub fn unix_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_micros() as u64
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Preparing,
    KeysReady,
    Connecting,
    Ready,
    Running,
    Result,
    Closing,
    Closed,
    Stopped,
    Failed,
}
impl Phase {
    fn name(self) -> &'static str {
        match self {
            Self::Preparing => "preparing",
            Self::KeysReady => "keys_ready",
            Self::Connecting => "connecting",
            Self::Ready => "ready",
            Self::Running => "running",
            Self::Result => "result",
            Self::Closing => "closing",
            Self::Closed => "closed",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
        }
    }
    fn done(self) -> bool {
        matches!(self, Self::Closed | Self::Stopped | Self::Failed)
    }
}

struct Run {
    id: u64,
    first: u32,
    count: u32,
    total: u32,
    seconds: u32,
    phase: Phase,
    deadline: Instant,
    keys: Vec<String>,
    sessions: u32,
    command: Option<ChildStdin>,
    child: Option<Child>,
    result: Option<String>,
    error: Option<String>,
    start_us: Option<u64>,
    closed: bool,
}
impl Run {
    fn transition(&mut self, phase: Phase, seconds: u64) {
        self.phase = phase;
        self.deadline = Instant::now() + Duration::from_secs(seconds);
    }
    fn send(&mut self, command: Value) -> Result<()> {
        let result = (|| {
            let pipe = self.command.as_mut().context("generator input closed")?;
            serde_json::to_writer(&mut *pipe, &command)?;
            writeln!(pipe)?;
            pipe.flush()?;
            Ok::<_, anyhow::Error>(())
        })();
        if let Err(err) = &result {
            self.fail(format!("generator command failed: {err}"));
        }
        result
    }
    fn terminate(&mut self) {
        self.command.take();
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
    fn fail(&mut self, message: String) {
        self.terminate();
        self.phase = Phase::Failed;
        self.error = Some(message);
    }
    fn event(&mut self, event: Value) -> Result<()> {
        match (self.phase, event["event"].as_str()) {
            (Phase::Preparing, Some("principal")) => {
                let index = u64::from(self.first) + self.keys.len() as u64;
                ensure!(
                    self.keys.len() < self.count as usize && event["index"].as_u64() == Some(index),
                    "invalid principal index"
                );
                let key = event["principal"].as_str().context("missing public key")?;
                ensure!(valid_pin(key), "invalid generator public key");
                self.keys.push(key.to_owned());
            }
            (Phase::Preparing, Some("keys_ready")) => {
                ensure!(
                    self.keys.len() == self.count as usize,
                    "incomplete user key set"
                );
                self.transition(Phase::KeysReady, 900);
            }
            (Phase::Connecting, Some("ready")) => {
                ensure!(
                    event["sessions"].as_u64() == Some(u64::from(self.count)),
                    "incomplete authenticated sessions"
                );
                self.sessions = self.count;
                self.transition(Phase::Ready, 900);
            }
            (Phase::Running, Some("result")) => {
                ensure!(
                    event["first"].as_u64() == Some(u64::from(self.first))
                        && event["sessions"].as_u64() == Some(u64::from(self.count))
                        && event["expected_deliveries"].as_u64().is_some()
                        && event["expected_deliveries"] == event["deliveries"],
                    "invalid generator result"
                );
                self.result = Some(serde_json::to_string(&event)?);
                self.transition(Phase::Result, 900);
            }
            (Phase::Closing, Some("closed")) => {
                self.closed = true;
            }
            _ => bail!("unexpected generator event in {}", self.phase.name()),
        }
        Ok(())
    }
}
impl Drop for Run {
    fn drop(&mut self) {
        self.terminate();
    }
}

pub struct Supervisor {
    executable: PathBuf,
    target: IpAddr,
    max: u32,
    slot: Mutex<Option<Run>>,
}
impl Supervisor {
    pub fn new(executable: PathBuf, target: IpAddr, max: u32) -> Result<Arc<Self>> {
        ensure!(
            (1..=1_000_000).contains(&max),
            "local MAX_USERS must be 1..=1000000"
        );
        ensure!(
            !target.is_unspecified()
                && !target.is_multicast()
                && !matches!(target,IpAddr::V4(v) if v.is_broadcast()),
            "expected explicit unicast target"
        );
        ensure!(
            executable.is_file(),
            "watch-capacity must be installed beside load-agent"
        );
        Ok(Arc::new(Self {
            executable,
            target,
            max,
            slot: Mutex::new(None),
        }))
    }
    fn slot(&self) -> MutexGuard<'_, Option<Run>> {
        self.slot.lock().expect("worker lock")
    }
    fn with<T>(&self, id: u64, op: impl FnOnce(&mut Run) -> Result<T>) -> Result<T> {
        let mut slot = self.slot();
        let run = slot
            .as_mut()
            .filter(|r| r.id == id)
            .context("unknown run")?;
        op(run)
    }
    pub fn prepare(
        self: &Arc<Self>,
        first: u32,
        count: u32,
        total: u32,
        orgs: u32,
        seconds: u32,
    ) -> Result<u64> {
        ensure!(
            (1..=self.max).contains(&count)
                && total <= 1_000_000
                && first.checked_add(count).is_some_and(|end| end <= total),
            "invalid user range or local limit exceeded"
        );
        ensure!(
            (1..=total.min(10_000)).contains(&orgs) && (1..=3600).contains(&seconds),
            "invalid organization count or duration"
        );
        let mut slot = self.slot();
        ensure!(
            slot.as_ref().is_none_or(|r| r.phase.done()),
            "another run is active"
        );
        let mut random = [0; 8];
        File::open("/dev/urandom")?.read_exact(&mut random[..6])?;
        let id = u64::from_le_bytes(random);
        let mut child = Command::new(&self.executable)
            .args([
                "generator",
                &first.to_string(),
                &count.to_string(),
                &total.to_string(),
                &orgs.to_string(),
                &seconds.to_string(),
            ])
            .env("WATCH_GENERATOR_TARGETS", self.target.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let stdout = child.stdout.take().expect("piped generator output");
        let command = child.stdin.take();
        *slot = Some(Run {
            id,
            first,
            count,
            total,
            seconds,
            phase: Phase::Preparing,
            deadline: Instant::now() + Duration::from_secs(900),
            keys: Vec::new(),
            sessions: 0,
            command,
            child: Some(child),
            result: None,
            error: None,
            start_us: None,
            closed: false,
        });
        drop(slot);
        let weak = Arc::downgrade(self);
        let reader = thread::Builder::new()
            .name("load-generator-output".into())
            .spawn(move || read_events(weak, id, stdout));
        if let Err(e) = reader {
            self.with(id, |r| {
                r.fail(e.to_string());
                Ok(())
            })?;
            return Err(e.into());
        }
        let weak = Arc::downgrade(self);
        let monitor = thread::Builder::new()
            .name("load-generator-lifetime".into())
            .spawn(move || {
                loop {
                    thread::sleep(Duration::from_millis(100));
                    let Some(supervisor) = weak.upgrade() else {
                        break;
                    };
                    let mut slot = supervisor.slot();
                    let Some(run) = slot.as_mut().filter(|r| r.id == id && !r.phase.done()) else {
                        break;
                    };
                    if run.closed {
                        match run.child.as_mut().unwrap().try_wait() {
                            Ok(Some(status)) if status.success() => {
                                run.phase = Phase::Closed;
                                run.child.take();
                                break;
                            }
                            Ok(Some(status)) => {
                                run.fail(format!("generator exited {status}"));
                                break;
                            }
                            Ok(None) => {}
                            Err(e) => {
                                run.fail(e.to_string());
                                break;
                            }
                        }
                    }
                    if Instant::now() >= run.deadline {
                        run.fail(format!("deadline in {}", run.phase.name()));
                        break;
                    }
                }
            });
        if let Err(e) = monitor {
            self.with(id, |r| {
                r.fail(e.to_string());
                Ok(())
            })?;
            return Err(e.into());
        }
        Ok(id)
    }

    pub fn keys(&self, id: u64, offset: u32, limit: u32) -> Result<Vec<String>> {
        ensure!(
            (1..=1024).contains(&limit),
            "key page limit must be 1..=1024"
        );
        self.with(id, |r| {
            let start = offset as usize;
            ensure!(start <= r.keys.len(), "offset beyond available keys");
            Ok(r.keys[start..(start + limit as usize).min(r.keys.len())].to_vec())
        })
    }
    pub fn connect(
        &self,
        id: u64,
        ports: Vec<u16>,
        pin: String,
        profile: String,
        rate: u32,
        interval: u32,
    ) -> Result<()> {
        ensure!(
            valid_pin(&pin) && matches!(profile.as_str(), "uniform" | "hot"),
            "invalid pinned fixture/profile"
        );
        ensure!(
            (1..=4).contains(&rate) && interval >= 1000,
            "fixture allows 1..=4 writes/s and >=1000ms read intervals"
        );
        self.with(id,|r|{
            ensure!(r.phase==Phase::KeysReady,"connect requires keys_ready");
            ensure!(!ports.is_empty() && ports.len()<=r.total.div_ceil(8000) as usize && !ports.contains(&0)
                && ports.iter().collect::<HashSet<_>>().len()==ports.len(),"invalid fixture listener ports");
            let urls:Vec<String>=ports.into_iter().map(|p|format!("wss://{}/",SocketAddr::new(self.target,p))).collect();
            r.send(json!({"op":"connect","urls":urls,"pin":pin,"profile":profile,"rate":rate,"read_interval_ms":interval}))?;
            r.transition(Phase::Connecting,900);
            Ok(())
        })
    }
    pub fn start(&self, id: u64, at: u64) -> Result<()> {
        self.with(id, |r| {
            if r.start_us == Some(at)
                && matches!(
                    r.phase,
                    Phase::Running | Phase::Result | Phase::Closing | Phase::Closed
                )
            {
                return Ok(());
            }
            ensure!(r.phase == Phase::Ready, "start requires ready");
            let now = unix_us();
            ensure!(
                at > now && at - now <= 120_000_000,
                "start timestamp must be within the next two minutes"
            );
            r.send(json!({"op":"start","unix_us":at}))?;
            r.start_us = Some(at);
            r.transition(
                Phase::Running,
                (at - now).div_ceil(1_000_000) + u64::from(r.seconds) + 90,
            );
            Ok(())
        })
    }
    pub fn status(&self, id: u64) -> Result<RunStatus> {
        self.with(id, |r| {
            Ok(RunStatus {
                phase: r.phase.name().into(),
                keys: r.keys.len() as u32,
                sessions: r.sessions,
                result: r.result.clone(),
                error: r.error.clone(),
            })
        })
    }
    pub fn finish(&self, id: u64) -> Result<()> {
        self.with(id, |r| {
            if matches!(r.phase, Phase::Closing | Phase::Closed) {
                return Ok(());
            }
            ensure!(r.phase == Phase::Result, "drop requires result");
            r.send(json!({"op":"drop"}))?;
            r.transition(Phase::Closing, 30);
            Ok(())
        })
    }
    pub fn stop(&self, id: u64) -> Result<()> {
        self.with(id, |r| {
            if !r.phase.done() {
                r.terminate();
                r.phase = Phase::Stopped;
            }
            Ok(())
        })
    }
    pub fn shutdown(&self) {
        self.slot().take();
    }
}
fn valid_pin(pin: &str) -> bool {
    pin.strip_prefix("key:").is_some_and(|raw| {
        raw.len() == 64
            && raw
                .bytes()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    })
}

fn read_events(owner: Weak<Supervisor>, id: u64, stdout: ChildStdout) {
    let mut stream = BufReader::new(stdout);
    let mut line = Vec::new();
    loop {
        line.clear();
        let event = read_event(&mut stream, &mut line);
        let Some(owner) = owner.upgrade() else { return };
        let mut slot = owner.slot();
        let Some(run) = slot
            .as_mut()
            .filter(|run| run.id == id && !run.phase.done())
        else {
            return;
        };
        match event {
            Ok(Some(event)) => {
                if let Err(error) = run.event(event) {
                    run.fail(error.to_string());
                    return;
                }
            }
            Err(error) => {
                run.fail(format!("generator output: {error}"));
                return;
            }
            Ok(None) => {
                run.command.take();
                if !run.closed {
                    let why = run
                        .child
                        .as_mut()
                        .and_then(|c| c.try_wait().ok().flatten())
                        .map(|status| format!("generator exited {status}"))
                        .unwrap_or_else(|| "generator closed its output".into());
                    run.fail(format!(
                        "{why} before completing its protocol (see local stderr)"
                    ));
                }
                return;
            }
        }
    }
}

fn read_event(stream: &mut impl BufRead, line: &mut Vec<u8>) -> Result<Option<Value>> {
    // Results contain histogram counts. Bound a malformed line before allocating it.
    if stream.take(4 * 1024 * 1024).read_until(b'\n', line)? == 0 {
        return Ok(None);
    }
    ensure!(
        line.last() == Some(&b'\n'),
        "generator output exceeds line limit"
    );
    Ok(Some(serde_json::from_slice(line)?))
}
