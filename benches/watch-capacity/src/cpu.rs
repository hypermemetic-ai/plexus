//! Linux userspace retired instructions for the fixture only. One event per
//! existing thread, on any CPU; inheritance includes subsequently cloned tasks.
//! WATCH_CPU=1 requires functioning hardware counters and a stable worker set.
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{self, Read},
    os::{fd::FromRawFd, raw::c_long},
    time::Instant,
};

// Linux UAPI PERF_ATTR_SIZE_VER0. Reserved and unused fields are zero.
#[repr(C)]
#[derive(Default)]
struct Attr {
    kind: u32,
    size: u32,
    config: u64,
    sample_period: u64,
    sample_type: u64,
    read_format: u64,
    flags: u64,
    wakeup: u32,
    breakpoint_type: u32,
    config1: u64,
}

unsafe extern "C" {
    fn syscall(number: c_long, ...) -> c_long;
    fn clock_getcpuclockid(pid: i32, clock: *mut i32) -> i32;
    fn clock_gettime(clock: i32, result: *mut [c_long; 2]) -> i32;
}

fn instructions(tid: i32) -> Result<File> {
    #[cfg(target_arch = "x86_64")]
    const PERF_EVENT_OPEN: c_long = 298;
    #[cfg(any(target_arch = "aarch64", target_arch = "riscv64"))]
    const PERF_EVENT_OPEN: c_long = 241;
    #[cfg(not(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv64"
    )))]
    anyhow::bail!("WATCH_CPU requires Linux x86_64, aarch64 or riscv64");
    #[cfg(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv64"
    ))]
    {
        let attr = Attr {
            size: size_of::<Attr>() as u32,
            config: 1,      // PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS
            read_format: 3, // TOTAL_TIME_ENABLED | TOTAL_TIME_RUNNING
            flags: (1 << 1) | (1 << 5) | (1 << 6) | (1 << 11), // inherit, exclude_kernel, exclude_hv, inherit_stat
            ..Default::default()
        };
        let fd = unsafe {
            syscall(
                PERF_EVENT_OPEN,
                &attr,
                tid as c_long,
                -1 as c_long,
                -1 as c_long,
                8 as c_long,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error()).context("perf_event_open instructions:u");
        }
        Ok(unsafe { File::from_raw_fd(fd as i32) })
    }
}

fn tids(pid: u32) -> Result<Vec<i32>> {
    let mut ids = vec![];
    for entry in fs::read_dir(format!("/proc/{pid}/task"))? {
        ids.push(entry?.file_name().to_string_lossy().parse()?);
    }
    ids.sort_unstable();
    Ok(ids)
}

struct Thread {
    event: File,
    name: String,
}
pub struct Meter {
    pid: u32,
    clock: i32,
    threads: BTreeMap<i32, Thread>,
}

pub struct Sample {
    wall: Instant,
    cpu_ns: u64,
    threads: BTreeMap<i32, ([u64; 3], u64)>,
}

impl Meter {
    pub fn attach(pid: u32) -> Result<Self> {
        let mut threads = BTreeMap::new();
        for tid in tids(pid)? {
            threads.insert(
                tid,
                Thread {
                    name: fs::read_to_string(format!("/proc/{pid}/task/{tid}/comm"))?
                        .trim()
                        .into(),
                    event: instructions(tid)?,
                },
            );
        }
        let mut clock = 0;
        let rc = unsafe { clock_getcpuclockid(pid as i32, &mut clock) };
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc).into());
        }
        Ok(Self {
            pid,
            clock,
            threads,
        })
    }

    pub fn sample(&mut self) -> Result<Sample> {
        ensure!(
            self.threads.keys().copied().eq(tids(self.pid)?),
            "fixture worker set changed"
        );
        let mut time = [0; 2];
        if unsafe { clock_gettime(self.clock, &mut time) } != 0 {
            return Err(io::Error::last_os_error().into());
        }
        let mut threads = BTreeMap::new();
        for (&tid, thread) in &mut self.threads {
            let mut bytes = [0; 24];
            thread.event.read_exact(&mut bytes)?;
            let perf = std::array::from_fn(|i| {
                u64::from_ne_bytes(bytes[i * 8..i * 8 + 8].try_into().unwrap())
            });
            let sched = fs::read_to_string(format!("/proc/{}/task/{tid}/schedstat", self.pid))?;
            let runtime = sched
                .split_whitespace()
                .next()
                .context("missing task runtime")?
                .parse()?;
            threads.insert(tid, (perf, runtime));
        }
        Ok(Sample {
            wall: Instant::now(),
            cpu_ns: time[0] as u64 * 1_000_000_000 + time[1] as u64,
            threads,
        })
    }

    pub fn report(&self, a: &Sample, b: &Sample, units: &[(&str, u64)]) -> Result<Value> {
        let cpu_ns = b.cpu_ns - a.cpu_ns;
        let mut total = 0u64;
        let mut tasks = vec![];
        for (&tid, (end, runtime)) in &b.threads {
            let (start, prior_runtime) = a.threads[&tid];
            let [raw, enabled, running] = std::array::from_fn(|i| end[i] - start[i]);
            ensure!(
                running <= enabled && (running > 0 || (enabled == 0 && raw == 0)),
                "unscheduled hardware counter for thread {tid}: {raw}/{enabled}/{running}"
            );
            let scaled = if running == 0 {
                0
            } else {
                u64::try_from(
                    (raw as u128 * enabled as u128 + running as u128 / 2) / running as u128,
                )?
            };
            total += scaled;
            tasks.push(json!({"tid":tid,"name":self.threads[&tid].name,"instructions_raw":raw,"time_enabled_ns":enabled,"time_running_ns":running,"instructions_scaled":scaled,"scheduler_cpu_ns":runtime-prior_runtime}));
        }
        let wall = (b.wall - a.wall).as_secs_f64();
        let mut per = serde_json::Map::new();
        for &(unit, n) in units {
            if n > 0 {
                per.insert(unit.into(),json!({"operations":n,"instructions":total as f64/n as f64,"cpu_ns":cpu_ns as f64/n as f64}));
            }
        }
        Ok(
            json!({"wall_s":wall,"process_cpu_ns":cpu_ns,"instructions_scaled":total,"instructions_per_s":total as f64/wall,"cpu_ns_per_s":cpu_ns as f64/wall,"per":per,"threads":tasks}),
        )
    }
}
