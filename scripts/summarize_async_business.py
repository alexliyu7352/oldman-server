"""Descriptive statistics only; three repeats do not establish universal bounds."""
from collections import defaultdict
import json
from pathlib import Path
import statistics

ROOT = Path(__file__).resolve().parents[1]


def distribution(values):
    return dict(median=statistics.median(values), minimum=min(values), maximum=max(values), values=values)


def group(folder):
    rows = [json.loads(p.read_text()) for p in sorted((ROOT/"results"/folder).glob("compress-*.json"))]
    assert rows and all(r["validated"] for r in rows)
    groups = defaultdict(list)
    for row in rows:
        groups[row["mode"]].append(row)
    result = {}
    for mode, cases in groups.items():
        cases.sort(key=lambda r:r["round"])
        item = {"rps": distribution([r["client"]["rps"] for r in cases]),
            "http_p99_ms": distribution([r["client"]["p99_us"]/1000 for r in cases]),
            "server_cpu_cores": distribution([r["server_cpu_cores"] for r in cases]),
            "max_sampled_pss_mib": max(r["max_sampled_pss_mib"] for r in cases),
            "validated_requests": sum(r["client"]["requests"] for r in cases),
            "errors": sum(r["client"]["errors"] for r in cases),
            "throttled_periods": sum(r["cgroup_delta"]["nr_throttled"] for r in cases)}
        lag = [max(w["loop_lag"]["p99_us"] for w in r["server"]["workers"])/1000
               for r in cases if all("loop_lag" in w for w in r["server"]["workers"])]
        if lag:
            item["worst_worker_loop_p99_ms"] = distribution(lag)
        result[mode] = item
    hybrid = "seastar-inline" if "seastar-inline" in groups else "seastar"
    native = "native-inline" if "native-inline" in groups else "native"
    paired = [a["client"]["rps"]/b["client"]["rps"]
              for a,b in zip(sorted(groups[hybrid],key=lambda r:r["round"]),
                             sorted(groups["multiprocess"],key=lambda r:r["round"]))]
    return dict(modes=result,
        hybrid_over_multiprocess_medians=result[hybrid]["rps"]["median"]/result["multiprocess"]["rps"]["median"],
        hybrid_over_native_medians=result[hybrid]["rps"]["median"]/result[native]["rps"]["median"],
        paired_hybrid_over_multiprocess=paired,
        all_paired_runs_meet_multiprocess=all(v>=1 for v in paired))


if __name__ == "__main__":
    result = {folder:group(folder) for folder in ("async-formal-256k", "async-formal-1m")}
    result["acceptance"] = "NOT MET: latest representative candidate is slower than matched-kernel uvloop multiprocess in all three repeats"
    result["scope"] = "256 KiB final inline candidate; 1 MiB earlier pool candidate with archived source snapshot; do not combine as one implementation's scaling curve"
    (ROOT/"results/async-business-summary.json").write_text(json.dumps(result,indent=2)+"\n")
    print(json.dumps(result,indent=2))
