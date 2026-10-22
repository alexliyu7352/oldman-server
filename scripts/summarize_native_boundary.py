"""Revalidate and summarize the one-workload native-boundary comparison."""
import argparse
import json
from pathlib import Path
from statistics import median
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from probes.framework_json.run import atomic_json, validate_measurement

MODES = ("candidate-before", "candidate3", "multiprocess", "granian-rsgi", "native3")


def summarize(directory):
    rows = [json.loads(p.read_text()) for p in sorted(directory.glob("json-*-r*.json"))]
    paired = {}
    for row in rows:
        if not row["validated"]:
            raise ValueError("unvalidated result")
        validate_measurement(row, 256)
        if (row["warmup"], row["seconds"], row["concurrency"]) != (3, 10, 256):
            raise ValueError("unexpected benchmark parameters")
        if row["server_cpus"] != [0, 1, 2] or row["client_cpus"] != [3, 4, 5, 6]:
            raise ValueError("unexpected CPU layout")
        group = paired.setdefault(row["round"], {})
        if row["mode"] not in MODES or row["mode"] in group:
            raise ValueError("unexpected/duplicate mode")
        group[row["mode"]] = row
    if sorted(paired) != [0, 1, 2] or any(set(g) != set(MODES) for g in paired.values()):
        raise ValueError("need three complete paired rounds")
    result = {"rounds": [0, 1, 2], "modes": {}, "ratios": {}}
    for mode in MODES:
        group = [paired[i][mode] for i in result["rounds"]]
        py_us = []
        if mode not in ("multiprocess", "native3"):
            for row in group:
                w = row["server"]["workers"][0]
                main = next(t for t in w["threads"] if t["tid"] == w["pid"])
                py_us.append(main["cpu_seconds"] * 1e6 / row["client"]["requests"])
        result["modes"][mode] = {
            "rps": [r["client"]["rps"] for r in group],
            "median_rps": median(r["client"]["rps"] for r in group),
            "p99_ms": [r["client"]["p99_us"] / 1000 for r in group],
            "median_p99_ms": median(r["client"]["p99_us"] / 1000 for r in group),
            "median_server_cpu_cores": median(r["server_cpu_cores"] for r in group),
            "median_server_cpu_us_per_request": median(r["server_cpu_per_request_us"] for r in group),
            "python_main_cpu_us_per_request": py_us,
            "median_python_main_cpu_us_per_request": median(py_us) if py_us else None,
            "requests": sum(r["client"]["requests"] for r in group),
            "throttled": [r["cgroup_delta"].get("nr_throttled", 0) for r in group],
            "client_cpu_cores": [r["client_cpu_cores"] for r in group],
        }
    for other in ("candidate-before", "multiprocess", "granian-rsgi", "native3"):
        ratios = [paired[i]["candidate3"]["client"]["rps"] / paired[i][other]["client"]["rps"]
                  for i in result["rounds"]]
        result["ratios"]["candidate3/" + other] = {
            "paired": ratios, "median_paired": median(ratios),
            "ratio_of_medians": result["modes"]["candidate3"]["median_rps"] / result["modes"][other]["median_rps"]}
    result["meets_multiprocess_every_round"] = all(v >= 1 for v in result["ratios"]["candidate3/multiprocess"]["paired"])
    result["total_requests"] = sum(r["client"]["requests"] for r in rows)
    result["total_response_bytes"] = sum(r["client"]["response_bytes"] for r in rows)
    result["all_validated"] = True
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="native-boundary-formal")
    args = parser.parse_args()
    directory = ROOT / "results" / args.out
    result = summarize(directory)
    atomic_json(directory / "summary.json", result)
    print(json.dumps(result, indent=2))
