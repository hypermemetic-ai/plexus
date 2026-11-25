"""Linux process CPU measurements from the fixture's scheduled load window."""
from pathlib import Path
import os
import threading
import time


def snapshot(pid):
    root = Path(f"/proc/{pid}")
    threads = {}
    for task in (root / "task").iterdir():
        try:
            cpu, waiting, _ = map(int, (task / "schedstat").read_text().split())
            threads[task.name] = ((task / "comm").read_text().strip(), cpu, waiting)
        except FileNotFoundError:
            continue
    # Fields 14 and 15 are user/system CPU ticks for the whole process.
    stat = (root / "stat").read_text().rsplit(") ", 1)[1].split()
    return time.monotonic_ns(), int(stat[11]), int(stat[12]), threads


class CpuWindow:
    """Snapshot cumulative counters twice, independently of a remote result's arrival."""

    def __init__(self, pid, start_unix_us, seconds):
        self.pid = pid
        self.start = time.monotonic_ns() + start_unix_us * 1000 - time.time_ns()
        self.end = self.start + int(seconds * 1_000_000_000)
        self.cancelled = threading.Event()
        self.samples = []
        self.error = None
        self.thread = threading.Thread(target=self._sample, daemon=True)

    def _sample(self):
        try:
            for due in (self.start, self.end):
                delay = (due - time.monotonic_ns()) / 1_000_000_000
                if self.cancelled.wait(max(delay, 0)):
                    return
                self.samples.append(snapshot(self.pid))
        except OSError as error:
            self.error = str(error)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, exc_type, *_):
        if exc_type is not None:
            self.cancelled.set()
        self.thread.join()

    def report(self):
        if len(self.samples) != 2:
            return {"available": False, "reason": self.error or "sample cancelled"}
        (t0, u0, s0, first), (t1, u1, s1, last) = self.samples
        elapsed = (t1 - t0) / 1_000_000_000
        ticks = os.sysconf("SC_CLK_TCK")
        user = (u1-u0)/ticks
        system = (s1-s0)/ticks
        return {
            "available": True, "wall_s": elapsed, "ticks_per_s": ticks,
            "start_late_us": (t0-self.start)/1000, "end_late_us": (t1-self.end)/1000,
            "user_cpu_s": user, "system_cpu_s": system,
            "busy_cores": (user+system)/elapsed,
            "thread_set_stable": first.keys() == last.keys(),
            "threads": [{"tid": int(tid), "name": last[tid][0],
                         "cpu_s": (last[tid][1]-first[tid][1])/1e9,
                         "runnable_wait_s": (last[tid][2]-first[tid][2])/1e9}
                        for tid in sorted(first.keys() & last.keys(), key=int)],
        }
