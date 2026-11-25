"""Classify a native fixture using its scheduled ingress and successful sink flushes."""
import argparse
import json
from pathlib import Path
from dataclasses import dataclass
import math

OPERATIONS = ("read", "write", "watch_update")


def positive(value):
    value = float(value)
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError("expected a positive finite number")
    return value


def fraction(value):
    value = positive(value)
    if value > 1:
        raise argparse.ArgumentTypeError("expected a fraction in (0, 1]")
    return value


def add_arguments(parser):
    parser.add_argument("--server-read-p99-ms", "--read-p99-ms", type=positive, default=100,
                        help="native read ingress to successful socket flush, in milliseconds")
    parser.add_argument("--server-write-p99-ms", type=positive, default=250,
                        help="native write ingress to successful acknowledgement flush")
    parser.add_argument("--server-fanout-p99-ms", "--update-p99-ms", type=positive, default=250,
                        help="native commit to successful watch update flush")
    parser.add_argument("--min-load-fraction", type=fraction, default=.95,
                        help="minimum fraction of scheduled operations admitted and flushed on time")


def budgets(args):
    return {"read": args.server_read_p99_ms, "write": args.server_write_p99_ms,
            "watch_update": args.server_fanout_p99_ms, "min_load_fraction": args.min_load_fraction}


@dataclass(frozen=True)
class Schedule:
    users: int
    orgs: int
    seconds: int
    interval_ms: int
    writes_per_second: int
    profile: str
    workload: str = "mixed"
    port_probes: int = 0

    def counts(self):
        if not (0 < self.orgs <= self.users and min(self.seconds, self.interval_ms, self.writes_per_second) > 0
                and 0 <= self.port_probes <= self.users and self.profile in ("uniform", "hot")
                and self.workload in ("reads", "updates", "mixed")):
            raise ValueError("invalid schedule")
        reads = (self.users*self.seconds*1000 + self.interval_ms-1)//self.interval_ms
        writes = self.seconds*self.writes_per_second if self.workload != "reads" else 0
        population = [0]*self.orgs
        hot = self.profile == "hot"
        for i in range(self.users):
            org = 0 if hot and i >= self.orgs and i % 5 == 0 else i % self.orgs
            population[org] += 1
        deliveries = 0
        for w in range(writes):
            org = (0 if w % 5 != 4 else w//5 % self.orgs) if hot else w % self.orgs
            deliveries += population[org]
        return {"read": reads if self.workload != "updates" else 0, "write": writes, "watch_update": deliveries}


def count(value):
    if type(value) is not int or value < 0:
        raise ValueError(f"invalid nonnegative count {value!r}")
    return value


def classify(result, schedule, limits, *, remote=False, start_unix_us=None, clock_uncertainty_us=0):
    expected = schedule.counts()
    receipt = {"expected": expected, "sessions": schedule.users, "port_probe_reads": schedule.port_probes,
               "server_budgets": limits}

    def verdict(status, code, reasons):
        return {**receipt, "status": status, "code": code, "reasons": reasons}

    def compare(errors, field, actual, want):
        if count(actual) != want:
            errors.append(f"{field}: observed {actual}, expected {want}")

    errors = []
    try:
        if remote:
            compare(errors, "first user", result["first"], 0)
            compare(errors, "sessions", result["sessions"], schedule.users)
            compare(errors, "port probes", result["port_probe_reads"], schedule.port_probes)
            hist = {"read": result["reads"], "write": result["writes"], "watch_update": result.get("commit_to_receive")}
            client_deliveries = result["deliveries"]
        else:
            compare(errors, "authenticated WSS", result["authenticated_wss"], schedule.users)
            compare(errors, "initial watches", result["initial_watches"], schedule.users)
            hist = {"read": result["read_scheduled_to_receive"], "write": result["scheduled_write_to_ack"],
                    "watch_update": result["commit_to_receive"]}
            compare(errors, "writes", result["writes"], expected["write"])
            client_deliveries = hist["watch_update"]["n"]
        for op in ("read", "write"):
            compare(errors, f"client {op}", hist[op]["n"], expected[op])
        compare(errors, "planned deliveries", result["expected_deliveries"], expected["watch_update"])
        compare(errors, "verified deliveries", client_deliveries, expected["watch_update"])
        if hist["watch_update"] is not None:
            compare(errors, "delivery latency observations", hist["watch_update"]["n"], expected["watch_update"])
        receipt["client_diagnostics_p99_us"] = {k: v.get("p99_us") if v is not None else None for k,v in hist.items()}
        if errors:
            return verdict("fail", "correctness", errors)
        timing = result["server_counters"]["server_timing"]
        if timing["version"] != 1:
            raise ValueError("unsupported timing version")
        compare(errors, "skipped probe reads", timing["skipped_probe_reads"], schedule.port_probes)
        compare(errors, "initial page flushes", timing["watch_initial_flushed"], schedule.users)
        compare(errors, "server failures", timing["failures"], 0)
        if count(timing["missing_commit_stamps"]):
            raise ValueError("missing commit timestamps")
        for op in OPERATIONS:
            for key in ("ingress", "flushed"):
                compare(errors, f"total {op} {key}", timing[op][key], expected[op])
            compare(errors, f"timed {op}", timing[op]["latency_us"]["n"], expected[op])
        if errors:
            return verdict("fail", "correctness", errors)
        if timing.get("window_error") or count(timing["unclassified_early_events"]):
            raise ValueError("fixture could not classify all events against the scheduled window")
        window = timing["scheduled"]
        us = count(window["duration_us"])
        if us != schedule.seconds*1_000_000 or window["end_us"]-window["start_us"] != us:
            raise ValueError("fixture window differs from requested duration")
        if start_unix_us is not None:
            mapped = count(timing["origin_unix_us"]) + count(window["start_us"])
            if abs(mapped-start_unix_us) > count(timing["origin_uncertainty_us"])+2:
                raise ValueError("fixture window differs from controller start")
        if clock_uncertainty_us < 0 or 2*clock_uncertainty_us >= us:
            raise ValueError("controller clock uncertainty spans the load window")
        bucket_us = count(timing["bucket_us"])
        buckets = timing["buckets"]
        if not bucket_us or not isinstance(buckets, list):
            raise ValueError("invalid fixture ingress buckets")
        # A sink is accountable for on-time flushes only after admission has
        # left it at least its configured server latency budget. Boundary
        # buckets are treated as possibly late, including clock uncertainty.
        eligible = {}
        for op in OPERATIONS:
            cutoff = window["end_us"] - limits[op]*1000 - clock_uncertainty_us
            late = sum(count(b[op]["ingress"]) for b in buckets
                       if b["start_us"] < window["end_us"] and b["start_us"] + bucket_us > cutoff)
            eligible[op] = max(0, count(window[op]["ingress"]) - late)

        insufficient, backlog, slow = [], [], []
        # A last-moment burst can drain inside the exact window. Compare the
        # first three quarters with the actual staggered generator schedule.
        # One bucket plus clock uncertainty absorbs quantization; one sample
        # absorbs cadence rounding on sparse workloads.
        for op in ("read", "write"):
            if not expected[op]:
                continue
            for quarter in (1,2,3):
                elapsed = us * quarter // 4
                due_us = elapsed - bucket_us - clock_uncertainty_us
                if due_us <= 0:
                    continue
                if op == "read":
                    due = math.ceil(schedule.users*due_us/(1000*schedule.interval_ms))
                else:
                    due = math.ceil(schedule.writes_per_second*due_us/1_000_000)
                threshold = max(0, math.ceil(due*limits["min_load_fraction"])-1)
                admitted = sum(count(b[op]["ingress"]) for b in buckets if
                               b["start_us"] + bucket_us > window["start_us"]-clock_uncertainty_us and
                               b["start_us"] < window["start_us"]+elapsed)
                if admitted < threshold:
                    insufficient.append(f"{op}: only {admitted} admitted by quarter {quarter}, minimum {threshold}")
                    break
        observations = {}
        for op in OPERATIONS:
            total = expected[op]
            incoming, flushed = (count(window[op][k]) for k in ("ingress", "flushed"))
            if max(incoming, flushed) > total:
                raise ValueError(f"window {op} count exceeds full-run total")
            # Round the fractional operation target to the nearest whole sample.
            # Never accept zero in-window witnesses for scheduled work.
            minimum = max(1, math.floor(total*limits["min_load_fraction"]+.5)) if total else 0
            p99 = count(timing[op]["latency_us"]["p99"]) if total else None
            observations[op] = {"ingress": incoming, "flushed": flushed, "minimum": minimum,
                                "eligible_ingress_lower_bound": eligible[op],
                                "achieved_flushes_per_s": flushed/schedule.seconds, "server_p99_us": p99}
            if incoming < minimum:
                if op == "watch_update" and eligible["write"] == expected["write"]:
                    backlog.append("admitted writes did not produce the scheduled fanout within the window")
                else:
                    insufficient.append(f"{op}: {incoming} admitted in window, minimum {minimum}")
            elif flushed < minimum and eligible[op] < minimum and eligible[op]-flushed <= total-minimum:
                insufficient.append(f"{op}: insufficient timely admissions to attribute the end-of-window shortfall")
            elif flushed < minimum:
                backlog.append(f"{op}: {incoming} admitted but only {flushed} flushed in window, minimum {minimum}")
            if p99 is not None and p99 > 1000*limits[op]:
                slow.append(f"{op}: server p99 {p99} us exceeds {limits[op]} ms")
        receipt.update(window=observations, duration_us=us, controller_clock_uncertainty_us=clock_uncertainty_us)
        if backlog:
            return verdict("fail", "server_backlog", backlog + slow + insufficient)
        if slow:
            return verdict("fail", "server_latency", slow + insufficient)
        if insufficient:
            return verdict("inconclusive", "underfed", insufficient)
        return verdict("pass", "pass", [])
    except (KeyError, TypeError, ValueError) as exc:
        return verdict("inconclusive", "missing_telemetry", [str(exc)])


def exit_code(verdict):
    if verdict["status"] == "pass":
        return 0
    return 1 if verdict["code"] in ("correctness", "runner_error") else 2

def main():
    parser = argparse.ArgumentParser(description="Evaluate one native rung or Mac worker result as JSON.")
    parser.add_argument("result", type=Path, help="single JSON result, optionally wrapped as a Mac result event")
    parser.add_argument("--users", type=int, required=True)
    parser.add_argument("--orgs", type=int, required=True)
    parser.add_argument("--seconds", type=int, required=True)
    parser.add_argument("--profile", choices=("uniform","hot"), default="uniform")
    parser.add_argument("--workload", choices=("reads","updates","mixed"), default="mixed")
    parser.add_argument("--read-interval-ms", type=int, default=10000)
    parser.add_argument("--writes-per-second", type=int, default=4)
    parser.add_argument("--remote", action="store_true", help="decode a distributed worker result")
    parser.add_argument("--port-probes", type=int, default=0, help="independently expected fixture port probes")
    parser.add_argument("--start-unix-us", type=int, help="independently planned fixture-clock deadline")
    parser.add_argument("--clock-uncertainty-us", type=int, default=0)
    add_arguments(parser)
    args = parser.parse_args()
    result = json.loads(args.result.read_text())
    schedule = Schedule(args.users,args.orgs,args.seconds,args.read_interval_ms,args.writes_per_second,
                        args.profile,args.workload,args.port_probes)
    if args.remote and args.port_probes < 1:
        parser.error("--remote requires a positive --port-probes")
    verdict = classify(result.get("worker",result), schedule, budgets(args), remote=args.remote,
                       start_unix_us=args.start_unix_us, clock_uncertainty_us=args.clock_uncertainty_us)
    print(json.dumps(verdict))
    return exit_code(verdict)


if __name__ == "__main__":
    raise SystemExit(main())
