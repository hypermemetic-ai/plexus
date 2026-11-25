//! Optional rendezvous with a rendered Plexus UI page on the live fixture.
//! Only the public enrollment pin crosses this private, per-run directory.
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{fs, io::ErrorKind, path::{Path, PathBuf}, time::Duration};
use tokio::time::{sleep, timeout};

pub struct BrowserObserver {
    dir: PathBuf,
    pub principal: String,
}

async fn read_when_ready(path: &Path) -> Result<String> {
    timeout(Duration::from_secs(120), async {
        loop {
            match fs::read_to_string(path) {
                Ok(text) => return Ok(text),
                Err(e) if e.kind() == ErrorKind::NotFound => sleep(Duration::from_millis(25)).await,
                Err(e) => return Err(e.into()),
            }
        }
    }).await.with_context(|| format!("browser did not publish {}", path.display()))?
}

impl BrowserObserver {
    pub async fn configured() -> Result<Option<Self>> {
        let Some(dir) = std::env::var_os("WATCH_BROWSER_RUN_DIR").map(PathBuf::from) else {
            return Ok(None);
        };
        for file in ["browser-endpoint.json", "browser-ready.json", "browser-release.json", "browser-result.json"] {
            ensure!(!dir.join(file).exists(), "browser run directory contains stale {file}");
        }
        let principal = read_when_ready(&dir.join("browser-principal.txt")).await?.trim().to_owned();
        ensure!(principal.strip_prefix("key:").is_some_and(|key| key.len() == 64 && key.bytes().all(|x| x.is_ascii_hexdigit())), "invalid browser enrollment pin");
        Ok(Some(Self { dir, principal }))
    }

    fn publish(&self, name: &str, data: Value) -> Result<()> {
        let tmp = self.dir.join(format!("{name}.tmp"));
        fs::write(&tmp, serde_json::to_vec(&data)?)?;
        fs::rename(tmp, self.dir.join(name))?;
        Ok(())
    }

    pub async fn connect(&self, url: &str, pin: &str) -> Result<()> {
        self.publish("browser-endpoint.json", json!({"url":url, "pin":pin}))?;
        let ready: Value = serde_json::from_str(&read_when_ready(&self.dir.join("browser-ready.json")).await?)?;
        ensure!(ready["row"] == "Organization 0 ticket 0", "browser did not render its initial authorized row");
        Ok(())
    }

    pub async fn finish(&self, expected: u64) -> Result<Value> {
        self.publish("browser-release.json", json!({"expected_revision":expected}))?;
        let result: Value = serde_json::from_str(&read_when_ready(&self.dir.join("browser-result.json")).await?)?;
        ensure!(result["first_revision"] == 0 && result["final_revision"] == expected &&
            result["updates"].as_u64().is_some_and(|n| n > 0) && result["monotonic"] == true &&
            result["cancelled"] == 1, "browser did not verify the final rendered revision and cancellation");
        Ok(result)
    }
}
