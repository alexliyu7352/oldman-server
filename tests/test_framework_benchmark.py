"""Pure benchmark contract tests; no Seastar process or network server."""
import io
import copy
from email.utils import formatdate
from pathlib import Path
import subprocess
import sys
import unittest
from unittest import mock

from probes.framework_json import run
from probes.framework_json import summarize


class FrameworkBenchmarkTests(unittest.TestCase):
    def cpu_evidence(self):
        cpu = {"version": 1, "mount": "/cpu", "membership": "/leaf", "levels": [
            {"path": "/cpu/leaf", "quota_us": None, "period_us": 100000, "bandwidth": True},
            {"path": "/cpu", "quota_us": None, "period_us": 100000, "bandwidth": True}]}
        budget = {"affinity": list(range(16)), "effective_cpus": list(range(16)),
                  "available_cores": 16, "quota_cores": None, "cpu": cpu, "cpuset": None}
        snapshot = {"cpu": cpu, "cpuset": None, "counters": {
            path: {"nr_periods": 20, "nr_throttled": 0, "throttled_nsec": 0}
            for path in ("/cpu/leaf", "/cpu")}}
        return {"cpu_budget": budget,
                "cpu_window": {"before": snapshot, "after": copy.deepcopy(snapshot)}}

    def test_script_entrypoint_adds_project_root_before_manifest(self):
        script = Path(run.__file__).resolve()
        root = script.parents[2]
        code = ("import runpy, sys; runpy.run_path(sys.argv[1]); "
                "assert sys.argv[2] in sys.path")
        result = subprocess.run([sys.executable, "-I", "-c", code, str(script), str(root)],
                                text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_cpu_layout_keeps_server_and_client_disjoint(self):
        topology = [{"cpu": cpu, "package_id": "0", "core_id": str(cpu)} for cpu in range(11, 20)]
        layout = run.cpu_layout(tuple(range(11, 20)), topology)
        self.assertEqual(layout["candidate1"], [11])
        self.assertEqual(layout["candidate3"], [11, 12, 13])
        self.assertEqual(layout["multiprocess"], [11, 12, 13])
        self.assertEqual(layout["native3"], [11, 12, 13])
        self.assertEqual(layout["client"], [14, 15, 16, 17])
        self.assertEqual(layout["unused"], 19)
        self.assertEqual(layout["controller"], 18)
        with self.assertRaises(ValueError):
            run.cpu_layout(tuple(range(8)), topology)

    def test_cpu_layout_keeps_controller_off_server_smt_sibling(self):
        topology = [{"cpu": cpu, "package_id": "0", "core_id": str(cpu % 8)} for cpu in range(16)]
        layout = run.cpu_layout(range(16), topology)
        self.assertEqual(layout["candidate3"], [0, 1, 2])
        self.assertEqual(layout["client"], [3, 4, 5, 6])
        self.assertEqual(layout["controller"], 7)
        self.assertEqual(layout["unused"], 8)
        # Interleaved logical numbering must not place sibling threads in different roles.
        topology = [{"cpu": cpu, "package_id": "0", "core_id": str(cpu // 2)} for cpu in range(16)]
        layout = run.cpu_layout(range(16), topology)
        self.assertEqual(layout["candidate3"], [0, 2, 4])
        self.assertEqual(layout["client"], [6, 8, 10, 12])
        self.assertEqual(layout["controller"], 14)

    def test_cpu_layout_rejects_insufficient_or_unknown_physical_cores(self):
        topology = [{"cpu": cpu, "package_id": "0", "core_id": str(cpu % 4)} for cpu in range(16)]
        with self.assertRaisesRegex(ValueError, "physical"):
            run.cpu_layout(range(16), topology)
        with self.assertRaisesRegex(ValueError, "topology"):
            run.cpu_layout(range(16), topology[:-1])

    def test_json_workload_invokes_serializer_for_every_response(self):
        calls = []
        original = run.json.dumps
        with mock.patch.object(run.json, "dumps", side_effect=lambda obj, **kw: (
                calls.append(obj), original(obj, **kw))[1]):
            self.assertEqual(run.json_body(), run.EXPECTED)
            self.assertEqual(run.json_body(), run.EXPECTED)
        self.assertIsNot(calls[0], calls[1])
        self.assertEqual(len(run.EXPECTED), 27)

    def test_response_parser_and_preflight_headers(self):
        date = formatdate(usegmt=True).encode("ascii")
        response = (b"HTTP/1.1 200 OK\r\nDate: " + date +
            b"\r\nServer: probe\r\nContent-Type: application/json\r\n"
            b"Transfer-Encoding: chunked\r\nX-Probe-Pid: 901\r\nX-Probe-Shard: 0\r\n\r\n" +
            b"1b\r\n" + run.EXPECTED + b"\r\n0\r\n\r\n")
        status, headers, body = run.read_response(io.BytesIO(response))
        run.validate_preflight_response(status, headers, body, [901], expected=True)
        self.assertEqual(body, run.EXPECTED)
        with self.assertRaisesRegex(ValueError, "Date"):
            run.validate_preflight_response(status, {k: v for k, v in headers.items() if k != "date"},
                                            body, [901], expected=True)
        with self.assertRaisesRegex(ValueError, "chunked"):
            run.validate_preflight_response(status, headers | {"transfer-encoding": "identity"},
                                            body, [901], expected=True)

    def measurement(self):
        return {
            **self.cpu_evidence(),
            "mode": "candidate3", "returncodes": [0, 0],
            "client": {"requests": 42, "scheduled": 42, "errors": 0, "integrity_errors": 0,
                       "status_errors": 0, "dropped": 0, "response_bytes": 42 * len(run.EXPECTED),
                       "new_connections": 0, "total_connections": 256,
                       "server_pids": {"901": 42}, "rps": 100, "p99_us": 1200},
            "server": {"workers": [{"delta": {"responses": 42, "python_application_started": 42,
                "python_application_finished": 42, "python_application_errors": 0},
                "after": {"python_application_active": 0}}]},
            "closed": {"closed": True, "workers": [{"active_requests": 0, "reserved_bytes": 0,
                "pending_decisions": 0, "queued_decisions": 0, "python_application_active": 0}]},
        }

    def test_measured_result_refuses_errors_and_missing_counts(self):
        row = self.measurement()
        run.validate_measurement(row, 256)
        row["client"]["status_errors"] = 1
        with self.assertRaisesRegex(ValueError, "status_errors"):
            run.validate_measurement(row, 256)
        row["client"]["status_errors"] = 0
        del row["server"]["workers"][0]["delta"]["python_application_finished"]
        with self.assertRaisesRegex(ValueError, "python_application_finished"):
            run.validate_measurement(row, 256)

    def test_measurement_rejects_unknown_cpu_budget(self):
        row = self.measurement()
        del row["cpu_budget"]
        with self.assertRaisesRegex(ValueError, "budget"):
            run.validate_measurement(row, 256)

    def test_measurement_rejects_parent_only_throttling(self):
        row = self.measurement()
        row["cpu_window"]["after"]["counters"]["/cpu"]["nr_throttled"] = 1
        with self.assertRaisesRegex(ValueError, "throttl"):
            run.validate_measurement(row, 256)

    def test_measurement_rejects_changed_budget_since_preflight(self):
        row = self.measurement()
        row["cpu_budget"] = copy.deepcopy(row["cpu_budget"])
        row["cpu_budget"]["cpu"]["membership"] = "/different"
        with self.assertRaisesRegex(ValueError, "changed"):
            run.validate_measurement(row, 256)

    def test_summary_requires_complete_paired_rounds(self):
        rows = [{"round": round_no, "mode": mode, "validated": True,
                 "client": {"rps": rps, "p99_us": 1000}, "server_cpu_per_request_us": 5}
                for round_no in range(3)
                for mode, rps in (("candidate1", 90), ("candidate3", 100),
                                  ("multiprocess", 80), ("native3", 130))]
        summary = summarize.summarize(rows)
        self.assertEqual(summary["paired_candidate3_over_multiprocess"], [1.25]*3)
        self.assertEqual(summary["modes"]["candidate3"]["median_rps"], 100)
        with self.assertRaisesRegex(ValueError, "missing"):
            summarize.summarize(rows[:-1])

    def test_summary_does_not_pass_if_one_paired_round_is_slower(self):
        rows = [{"round": round_no, "mode": mode, "validated": True,
                 "client": {"rps": (70 if round_no == 1 and mode == "candidate3" else rps),
                            "p99_us": 1000}, "server_cpu_per_request_us": 5}
                for round_no in range(3)
                for mode, rps in (("candidate1", 90), ("candidate3", 100),
                                  ("multiprocess", 80), ("native3", 130))]
        summary = summarize.summarize(rows)
        self.assertGreater(summary["median_paired_candidate3_over_multiprocess"], 1)
        self.assertFalse(summary["all_rounds_ge1"])
        self.assertFalse(summary["candidate3_meets_multiprocess"])


if __name__ == "__main__":
    unittest.main()
