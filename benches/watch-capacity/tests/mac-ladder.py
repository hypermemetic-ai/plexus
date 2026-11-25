"""Bounded ladder orchestration tests with synthetic evidence and a child process."""
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

path = Path(__file__).resolve().parents[1] / "mac-ladder.py"
sys.path.insert(0, str(path.parent))
spec = importlib.util.spec_from_file_location("mac_ladder", path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class Ladder(unittest.TestCase):
    def test_stops_apply_to_individual_profiles(self):
        visits, events = [], []
        def run(profile, users, _):
            visits.append((profile, users))
            if profile == "hot" and users == 2:
                return "underfed", ["generator issued fewer scheduled requests"], 2
            return "pass", [], 0
        result = module.ladder(["hot", "uniform"], [1, 2, 3], run,
                               lambda event, **data: events.append((event, data)))
        self.assertEqual(result, 2)
        self.assertNotIn(("hot", 3), visits)
        self.assertIn(("uniform", 3), visits)
        self.assertEqual(events[-1][1]["highest_passed"], {"hot": 1, "uniform": 3})
        self.assertEqual(events[-1][1]["stops"]["hot"]["code"], "underfed")

    def test_cost_projection_counts_marginal_rows_and_sockets(self):
        previous = [{"rows": 2000, "users": 20, "seeded": 100_000, "peak_pss": 125_000, "host_delta": 130_000},
                    {"rows": 4000, "users": 40, "seeded": 120_000, "peak_pss": 170_000, "host_delta": 180_000}]
        self.assertEqual(module.projected_bytes(previous, 60, 6000), 287_500)

    def test_real_child_slow_rtt_does_not_override_verdict(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            # Run a short local child through the same process and evidence path.
            script = root / "mac-rung.py"
            fixture = [
                {"event":"run","run":1},
                *[{"event":e, "server":{"Pss": 256, "units":"KiB"}} for e in ("seeded","connected")],
                {"event":"result","server":{"Pss":260,"units":"KiB"},
                 "worker":{"reads":{"p99_us":10000000},"clock":{"valid":False}}},
                {"event":"closed"},
                {"event":"verdict","verdict":{"code":"pass","status":"pass","reasons":[]}},
            ]
            script.write_text("import sys\nprint(" + repr("\n".join(map(json.dumps, fixture))) + ")\n")
            args = types.SimpleNamespace(rows=50, reserve_gib=.001, out_dir=root, key_file=root/"key",
                bin_dir=root, mac="127.0.0.1", ryzen="127.0.0.1", mac_pin="key:test", control_port=12345,
                control_host=None, read_interval_ms=10000, writes_per_second=4,
                server_read_p99_ms=100, server_write_p99_ms=250, server_fanout_p99_ms=250, min_load_fraction=.95)
            events, history = [], []
            with patch.object(module, "HERE", root):
                verdict = module.run_one(args, "uniform", 2, 1, history,
                                         lambda event, **data: events.append((event, data)))
            self.assertEqual(verdict, ("pass", [], 0))
            self.assertEqual(history[0]["peak_pss"], 260*1024)
            self.assertEqual(json.loads((root/"2-uniform.jsonl").read_text().splitlines()[-1])["event"], "verdict")
            self.assertTrue((root/"2-uniform.stderr").exists())

    def test_wrong_exit_or_duplicate_verdict_cannot_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/"raw"
            event = {"event":"verdict","verdict":{"code":"underfed","status":"inconclusive","reasons":["late"]}}
            path.write_text(json.dumps(event) + "\n")
            self.assertEqual(module.judge(path, 2)[0]["code"], "underfed")
            with self.assertRaises(ValueError):
                module.judge(path, 0)
            path.write_text(path.read_text()*2)
            with self.assertRaises(ValueError):
                module.judge(path, 2)

    def test_resource_guard_prevents_launch(self):
        events = []
        args = types.SimpleNamespace(rows=50, reserve_gib=1)
        with patch.object(module, "memory_available", return_value=module.GIB):
            code, _, status = module.run_one(args, "hot", 10, 1, [],
                                      lambda kind, **fields: events.append(kind))
        self.assertEqual((code, status), ("resource_guard", 2))
        self.assertEqual(events, ["preflight"])


if __name__ == "__main__":
    unittest.main()
