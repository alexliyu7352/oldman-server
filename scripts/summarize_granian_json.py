"""Summarize the single-workload Granian reference comparison, keeping all rounds."""
import argparse
import json
from pathlib import Path
from statistics import median
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from probes.framework_json.run import atomic_json, validate_measurement

MODES = ("candidate3", "multiprocess", "granian-asgi", "granian-rsgi")


def summarize(directory):
    rows = [json.loads(p.read_text()) for p in sorted(directory.glob("json-*-r*.json"))]
    paired = {}
    for row in rows:
        if not row["validated"]:
            raise ValueError("unvalidated result")
        validate_measurement(row, 256)
        if row["warmup"] != 3 or row["seconds"] != 10 or row["concurrency"] != 256:
            raise ValueError("unexpected benchmark parameters")
        if row["server_cpus"] != [0, 1, 2] or row["client_cpus"] != [3, 4, 5, 6]:
            raise ValueError("unexpected CPU layout")
        group = paired.setdefault(row["round"], {})
        if row["mode"] not in MODES or row["mode"] in group:
            raise ValueError("unexpected or duplicate mode")
        group[row["mode"]] = row
    if sorted(paired) != [0, 1, 2] or any(set(group) != set(MODES) for group in paired.values()):
        raise ValueError("need three complete paired rounds")
    output = {"rounds": [0, 1, 2], "modes": {}, "paired_ratios": {}}
    for mode in MODES:
        group = [paired[i][mode] for i in output["rounds"]]
        rps = [row["client"]["rps"] for row in group]
        py_us = []
        if mode != "multiprocess":
            for row in group:
                worker = row["server"]["workers"][0]
                main = next(t for t in worker["threads"] if t["tid"] == worker["pid"])
                py_us.append(main["cpu_seconds"]*1e6/row["client"]["requests"])
        output["modes"][mode] = {
            "rps_per_round": rps, "median_rps": median(rps),
            "p99_ms_per_round": [r["client"]["p99_us"]/1000 for r in group],
            "median_p99_ms": median(r["client"]["p99_us"]/1000 for r in group),
            "server_cpu_cores_per_round": [r["server_cpu_cores"] for r in group],
            "median_server_cpu_cores": median(r["server_cpu_cores"] for r in group),
            "median_server_cpu_per_request_us": median(r["server_cpu_per_request_us"] for r in group),
            "python_main_cpu_us_per_request": py_us,
            "median_python_main_cpu_us_per_request": median(py_us) if py_us else None,
            "client_cpu_cores_per_round": [r["client_cpu_cores"] for r in group],
            "requests": sum(r["client"]["requests"] for r in group),
            "nr_throttled": sum(r["cgroup_delta"].get("nr_throttled", 0) for r in group),
        }
    for left, right in (("granian-rsgi", "multiprocess"), ("candidate3", "multiprocess"),
                        ("granian-rsgi", "candidate3"), ("granian-rsgi", "granian-asgi")):
        ratios = [paired[i][left]["client"]["rps"]/paired[i][right]["client"]["rps"] for i in output["rounds"]]
        output["paired_ratios"][left+"/"+right] = ratios
    output["total_requests"] = sum(r["client"]["requests"] for r in rows)
    output["total_response_bytes"] = sum(r["client"]["response_bytes"] for r in rows)
    output["all_rows_validated"] = True
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="granian-json-formal")
    args = parser.parse_args()
    directory = ROOT / "results" / args.out
    result = summarize(directory)
    atomic_json(directory / "summary.json", result)
    print(json.dumps(result, indent=2))
