//! channels › Any channel › the page's runtime: plexus-rt, in Node, calls
//! the kiln binary over a WebSocket (`tests/page.test.mts`).
//!
//! It needs Node 22 or later (a global WebSocket), `../plexus-rt`, and the
//! codec's wasm, which `../plexus-rt/scripts/codec.sh` builds (or
//! `PLEXUS_CODEC_WASM` names). Without them the test is reported ignored,
//! with why, never passed.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use libtest_mimic::{Arguments, Trial};

#[path = "../src/bin/kiln/activation.rs"]
#[allow(dead_code)]
mod activation;

const NAME: &str = "plexus_rt_in_node_over_a_websocket";

fn main() {
    let args = Arguments::from_args();
    let trial = match prerequisites() {
        Ok(wasm) => Trial::test(NAME, move || run(&wasm).map_err(Into::into)),
        Err(why) => {
            eprintln!("SKIPPED {NAME}: {why}");
            Trial::test(NAME, || Err("not run: its prerequisites are missing".into())).with_ignored_flag(true)
        }
    };
    libtest_mimic::run(&args, vec![trial]).exit();
}

fn here() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

/// Node, plexus-rt and the codec's wasm, or why one is missing.
fn prerequisites() -> Result<PathBuf, String> {
    let version = Command::new("node").arg("--version").output().map_err(|e| format!("no node on PATH ({e})"))?;
    let version = String::from_utf8_lossy(&version.stdout).trim().to_owned();
    let major: u32 = version.trim_start_matches('v').split('.').next().and_then(|m| m.parse().ok()).unwrap_or(0);
    if major < 22 {
        return Err(format!("node {version}; the test needs 22 or later, for its WebSocket"));
    }
    let rt = here().join("../plexus-rt");
    if !rt.join("plexus-rt.ts").exists() {
        return Err(format!("no plexus-rt at {}", rt.display()));
    }
    if let Some(wasm) = std::env::var_os("PLEXUS_CODEC_WASM") {
        let wasm = PathBuf::from(wasm);
        return if wasm.exists() { Ok(wasm) } else { Err(format!("PLEXUS_CODEC_WASM names {}, which is not there", wasm.display())) };
    }
    // Built in a target of its own: the one this test runs from is in use.
    let built = Command::new(rt.join("scripts/codec.sh"))
        .env("CARGO_TARGET_DIR", Path::new(env!("CARGO_TARGET_TMPDIR")).join("codec"))
        .output()
        .map_err(|e| format!("could not run plexus-rt's scripts/codec.sh ({e})"))?;
    if !built.status.success() {
        return Err(format!("plexus-rt's scripts/codec.sh could not build the codec's wasm:\n{}", String::from_utf8_lossy(&built.stderr)));
    }
    Ok(PathBuf::from(String::from_utf8_lossy(&built.stdout).trim()))
}

struct Killed(Child);

impl Drop for Killed {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn run(wasm: &Path) -> Result<(), String> {
    let mut kiln = Killed(
        Command::new(env!("CARGO_BIN_EXE_kiln"))
            .args(["--ws", "127.0.0.1:0"])
            .stdout(Stdio::piped())
            .spawn()
            .map_err(|e| format!("the kiln did not start: {e}"))?,
    );
    let mut url = String::new();
    BufReader::new(kiln.0.stdout.take().expect("piped")).read_line(&mut url).map_err(|e| e.to_string())?;
    let node = Command::new("node")
        .current_dir(here())
        .args(["tests/page.test.mts", url.trim()])
        .arg(wasm)
        .arg(activation::HASH)
        .output()
        .map_err(|e| format!("node did not run: {e}"))?;
    if node.status.success() {
        return Ok(());
    }
    Err(format!("{}\n{}", String::from_utf8_lossy(&node.stdout), String::from_utf8_lossy(&node.stderr)))
}
