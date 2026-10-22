"""Descriptive comparisons for the bounded HTTP I/O probe."""
from collections import defaultdict
import json
from pathlib import Path
import statistics

ROOT = Path(__file__).resolve().parents[1]


def distribution(values):
    return dict(median=statistics.median(values), minimum=min(values), maximum=max(values), values=values)


def group(folder, candidate):
    rows = [json.loads(p.read_text()) for p in sorted((ROOT/"results"/folder).glob("fetch-*.json"))]
    assert rows and all(r["validated"] for r in rows)
    grouped = defaultdict(list)
    for row in rows: grouped[row["mode"]].append(row)
    modes = {}
    for mode, cases in grouped.items():
        cases.sort(key=lambda r:r["round"])
        assert len({r["round"] for r in cases}) == len(cases)
        item = {"rps": distribution([r["client"]["rps"] for r in cases]),
            "http_p99_ms": distribution([r["client"]["p99_us"]/1000 for r in cases]),
            "server_cpu_cores": distribution([r["server_cpu_cores"] for r in cases]),
            "upstream_cpu_cores": distribution([r["upstream_cpu_cores"] for r in cases]),
            "client_cpu_cores": distribution([r["client"]["cpu_cores"] for r in cases]),
            "max_sampled_pss_mib": max(r["max_sampled_pss_mib"] for r in cases),
            "validated_requests": sum(r["client"]["requests"] for r in cases),
            "errors": sum(r["client"]["errors"] for r in cases),
            "throttled_periods": sum(r["cgroup_delta"]["nr_throttled"] for r in cases)}
        lag = [max(w["loop_lag"]["p99_us"] for w in r["server"]["workers"])/1000
               for r in cases if all("loop_lag" in w for w in r["server"]["workers"])]
        if lag: item["worst_worker_loop_p99_ms"] = distribution(lag)
        modes[mode] = item
    by_round = {r["round"]:r for r in grouped["multiprocess"]}
    assert {r["round"] for r in grouped[candidate]} == set(by_round)
    paired = [r["client"]["rps"]/by_round[r["round"]]["client"]["rps"] for r in grouped[candidate]]
    return dict(modes=modes, candidate=candidate,
        hybrid_over_multiprocess_medians=modes[candidate]["rps"]["median"]/modes["multiprocess"]["rps"]["median"],
        hybrid_over_native_medians=modes[candidate]["rps"]["median"]/modes["native"]["rps"]["median"],
        paired_hybrid_over_multiprocess=paired,
        all_paired_runs_meet_multiprocess=all(v>=1 for v in paired))


if __name__ == "__main__":
    result = {"initial":group("io-formal-8k", "seastar"),
              "final":group("io-final-8k", "seastar-dedicated")}
    rows = [json.loads(p.read_text()) for folder in ("io-screen-8k", "io-formal-8k", "io-topology-screen-8k", "io-final-8k")
            for p in (ROOT/"results"/folder).glob("fetch-*.json")]
    assert all(r["validated"] for r in rows)
    result["total"] = dict(measurements=len(rows), requests=sum(r["client"]["requests"] for r in rows),
                           errors=sum(r["client"]["errors"] for r in rows))
    result["scope"] = "Loopback 8 KiB HTTP gateway; no general CPU/I/O/framework equivalence claim"
    (ROOT/"results/io-business-summary.json").write_text(json.dumps(result,indent=2)+"\n")
    print(json.dumps(result,indent=2))
