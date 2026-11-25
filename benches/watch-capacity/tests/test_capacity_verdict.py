import sys
import unittest
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from capacity_verdict import OPERATIONS, Schedule, classify, exit_code


class VerdictTests(unittest.TestCase):
    def setUp(self):
        self.s = Schedule(10,2,2,100,2,"uniform",port_probes=1)
        self.l = dict(read=10,write=20,watch_update=30,min_load_fraction=.95)
        n = self.s.counts()
        hist = lambda k: {"n":n[k],"p99_us":900000}
        self.t = {"version":1,"skipped_probe_reads":1,"watch_initial_flushed":10,"failures":0,
                  "missing_commit_stamps":0,"origin_unix_us":100000000,"origin_uncertainty_us":2,
                  "window_error":None,"unclassified_early_events":0,
                  "scheduled":{"start_us":2000000,"end_us":4000000,"duration_us":2000000}}
        for op in OPERATIONS:
            self.t[op] = dict(ingress=n[op],flushed=n[op],latency_us={"n":n[op],"p99":500})
            self.t["scheduled"][op] = dict(ingress=n[op],flushed=n[op])
        self.t["bucket_us"]=100_000
        self.t["buckets"]=[]
        for i in range(20):
            admissions={"read":10,"write":int(i%5==0),"watch_update":5*int(i%5==0)}
            self.t["buckets"].append({"start_us":2000000+i*100000,
                                      **{op:{"ingress":n,"flushed":n} for op,n in admissions.items()}})
        self.r = {"first":0,"sessions":10,"port_probe_reads":1,"reads":hist("read"),"writes":hist("write"),
                  "commit_to_receive":hist("watch_update"),"expected_deliveries":20,"deliveries":20,
                  "server_counters":{"server_timing":self.t}}

    def verdict(self, **kwargs):
        return classify(self.r,self.s,self.l,remote=True,start_unix_us=102000000,**kwargs)

    def test_client_rtt_does_not_reject_server(self):
        d=self.verdict()
        self.assertEqual(("pass",0),(d["code"],exit_code(d)))
        self.assertEqual(900000,d["client_diagnostics_p99_us"]["read"])

    def test_uncertain_client_one_way_timing_keeps_verified_delivery_count(self):
        self.r["commit_to_receive"]=None
        self.assertEqual("pass",self.verdict()["code"])

    def test_correctness_counts_include_port_probes_separately(self):
        for field in ("skipped_probe_reads","watch_initial_flushed","failures"):
            before=self.t[field];self.t[field]+=1
            with self.subTest(field=field):
                self.assertEqual("correctness",self.verdict()["code"])
            self.t[field]=before
        self.t["read"]["ingress"]+=1
        self.assertEqual("correctness",self.verdict()["code"])

    def test_client_report_cannot_choose_its_own_expected_deliveries(self):
        self.r.update(deliveries=19,expected_deliveries=19)
        self.assertEqual("correctness",self.verdict()["code"])

    def test_underfed_fully_drained_is_inconclusive(self):
        self.t["scheduled"]["read"]={"ingress":25,"flushed":25}
        d=self.verdict()
        self.assertEqual(("inconclusive","underfed",2),(d["status"],d["code"],exit_code(d)))

    def test_final_bucket_burst_cannot_establish_steady_capacity(self):
        for b in self.t["buckets"]:
            b["read"]["ingress"]=0
        self.t["buckets"][-1]["read"]["ingress"]=200
        self.assertEqual("underfed",self.verdict()["code"])
        self.t["scheduled"]["read"]["flushed"]=150
        self.assertEqual("underfed",self.verdict()["code"])

    def test_delayed_ingress_burst_cannot_pass(self):
        self.t["scheduled"]["read"]={"ingress":0,"flushed":0}
        self.assertEqual("underfed",self.verdict()["code"])

    def test_backlog_during_window_fails_despite_eventual_drain(self):
        self.t["scheduled"]["read"]["flushed"]=170
        self.assertEqual("server_backlog",self.verdict()["code"])

    def test_low_fanout_with_low_write_arrivals(self):
        self.t["scheduled"]["write"]={"ingress":1,"flushed":1}
        self.t["scheduled"]["watch_update"]={"ingress":5,"flushed":5}
        self.assertEqual("underfed",self.verdict()["code"])

    def test_admitted_writes_missing_fanout(self):
        self.t["scheduled"]["watch_update"]={"ingress":2,"flushed":2}
        self.assertEqual("server_backlog",self.verdict()["code"])

    def test_server_p99_breach(self):
        self.t["read"]["latency_us"]["p99"]=10001
        self.assertEqual("server_latency",self.verdict()["code"])

    def test_missing_clock_or_partial_window_is_inconclusive(self):
        self.t["unclassified_early_events"]=1
        self.assertEqual("missing_telemetry",self.verdict()["code"])
        self.t["unclassified_early_events"]=0
        self.t["scheduled"]["start_us"]+=1_000_000
        self.assertEqual("missing_telemetry",self.verdict()["code"])

    def test_old_fixture_cannot_pass(self):
        self.r["server_counters"]={}
        self.assertEqual("missing_telemetry",self.verdict()["code"])

    def test_sparse_schedule_and_exact_global_counts(self):
        self.assertEqual({"read":1,"write":0,"watch_update":0},
                         Schedule(1,1,1,10000,4,"uniform","reads").counts())
        self.assertEqual({"read":34,"write":10,"watch_update":77},
                         Schedule(17,3,1,500,10,"hot").counts())
