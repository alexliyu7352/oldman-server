"""Describe retirement results without treating noisy samples as a speedup."""
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import statistics

ROOT = Path(__file__).resolve().parents[1]
FOLDER = ROOT / "results/io-retirement-formal"


def distribution(values):
    return dict(values=values, median=statistics.median(values),
                minimum=min(values), maximum=max(values))


def summarize():
    manifest = json.loads((FOLDER / "manifest.json").read_text())
    mismatches = [name for name, digest in manifest["fingerprints"].items()
                  if hashlib.sha256((ROOT / name).read_bytes()).hexdigest() != digest]
    assert not mismatches, mismatches
    cases = defaultdict(list)
    for path in sorted(FOLDER.glob("fetch-*.json")):
        row = json.loads(path.read_text())
        assert row["validated"] and row["client"]["errors"] == 0, path
        cases[row["mode"]].append(row)
    expected = {"native", "multiprocess", "seastar-dedicated", "seastar-retire"}
    assert set(cases) == expected
    modes = {}
    for mode, rows in cases.items():
        rows.sort(key=lambda row: row["round"])
        assert [row["round"] for row in rows] == [0, 1, 2]
        metrics = {
            "rps": [row["client"]["rps"] for row in rows],
            "http_p99_ms": [row["client"]["p99_us"] / 1000 for row in rows],
            "server_cpu_cores": [row["server_cpu_cores"] for row in rows],
            "server_cpu_us_per_request": [row["server_cpu_cores"] /
                                          row["client"]["rps"] * 1e6 for row in rows],
        }
        if mode.startswith("seastar"):
            metrics["terminal_events_per_request"] = [
                row["server"]["workers"][0]["delta"]["python_terminal_events"] /
                row["client"]["requests"] for row in rows]
            metrics["eventfd_writes_per_request"] = [
                row["server"]["workers"][0]["delta"]["notification_writes"] /
                row["client"]["requests"] for row in rows]
            metrics["python_thread_cpu_us_per_request"] = [
                next(t["cpu_seconds"] for t in row["server"]["workers"][0]["threads"]
                     if t["tid"] == row["server"]["workers"][0]["pid"]) /
                row["client"]["requests"] * 1e6 for row in rows]
        modes[mode] = {key: distribution(values) for key, values in metrics.items()}
    ratios = {}
    for numerator, denominator in (("seastar-retire", "seastar-dedicated"),
                                    ("seastar-retire", "multiprocess"),
                                    ("seastar-dedicated", "multiprocess"),
                                    ("seastar-retire", "native")):
        a, b = modes[numerator]["rps"], modes[denominator]["rps"]
        values = [x / y for x, y in zip(a["values"], b["values"])]
        ratios[numerator + "_over_" + denominator] = {
            "same_round_ratios": values,
            "ratio_of_medians": a["median"] / b["median"],
            "all_rounds_at_least_one": all(v >= 1 for v in values),
        }
    rows = [row for group in cases.values() for row in group]
    return {
        "cpu_model": manifest["cpu_model"],
        "diagnostic_env": manifest["diagnostic_env"],
        "source_hashes_match": not mismatches,
        "modes": modes, "ratios": ratios,
        "total": {"measurements": len(rows),
                  "requests": sum(row["client"]["requests"] for row in rows),
                  "errors": sum(row["client"]["errors"] for row in rows),
                  "throttled_periods": sum(row["cgroup_delta"]["nr_throttled"] for row in rows)},
        "decision": "Keep retirement opt-in: no reliable throughput improvement; candidate fails one matched multiprocess round.",
        "limitations": ["Sequential same-round controls, not simultaneous pairs",
                        "Large variation within each mode and native reactor stall reports",
                        "Different CPU model from prior io-final-8k; absolute RPS must not be pooled",
                        "Instrumented profile timings are not pure binding overhead"],
    }


if __name__ == "__main__":
    summary = summarize()
    output = ROOT / "results/io-retirement-summary.json"
    output.write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))
