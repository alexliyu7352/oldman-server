"""Validate and summarize the nine recorded rows; never runs a benchmark."""
import hashlib
import json
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "results/batched-decisions"
MODES = ("seastar", "seastar-batch", "native")
manifest = json.loads((OUT / "manifest.json").read_text())
for name, expected in manifest["fingerprints"].items():
    assert hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == expected, name

summary, verified, total = {}, [], 0
for mode in MODES:
    values = []
    for round_id in range(3):
        path = OUT / f"{mode}-r{round_id}-client5.json"
        row = json.loads(path.read_text())
        c = row["client"]
        d = row["server"]["workers"][0]["delta"]
        n = c["requests"]
        assert n > 0 and c["scheduled"] == d["responses"] == sum(c["server_pids"].values()) == n
        assert c["response_bytes"] == d["response_bytes"] == n * 1024
        assert all(c[k] == 0 for k in ("errors", "status_errors", "integrity_errors", "dropped", "new_connections"))
        assert all(d.get(k, 0) == 0 for k in ("errors", "rejected", "decision_timeouts", "python_decision_errors"))
        assert c["total_connections"] == 128 and row["returncodes"] == [0, 0]
        assert row["reactor_cpus"] == [1, 2] and row["client_cpus"] == [3, 4, 5, 6, 7]
        assert len(row["ready"]["pids"]) == len(row["ready"]["service_pids"]) == 1
        assert d.get("python_decisions", 0) == (0 if mode == "native" else n)
        assert d["decisions"] == (0 if mode == "native" else n)
        assert row["closed"]["closed"]
        assert all(w.get(k, 0) == 0 for w in row["closed"]["workers"]
                   for k in ("active_requests", "reserved_bytes", "pending_decisions", "queued_decisions"))
        posts, batches = d["decision_return_posts"], d["decision_return_batches"]
        if mode == "seastar-batch":
            assert d["decision_return_items"] == n
            assert 0 < batches <= posts <= 2*batches and posts < n
        else:
            assert posts == batches == d["decision_return_items"] == 0
        shard_ids = [str(s["tid"]) for s in row["ready"]["workers"][0]["shards"]]
        reactor_cpu = sum(row["threads"][tid]["cpu_cores"] for tid in shard_ids)
        python_cpu = 0 if mode == "native" else row["threads"][str(row["ready"]["pids"][0])]["cpu_cores"]
        values.append(dict(round=round_id, rps=c["rps"], p99_ms=c["p99_us"]/1000,
            cpu_cores=row["server_cpu_cores"], client_cpu_cores=c["cpu_cores"],
            total_cpu_us_per_request=row["server_cpu_cores"]*1e6/c["rps"],
            reactor_cpu_us_per_request=reactor_cpu*1e6/c["rps"],
            python_cpu_us_per_request=python_cpu*1e6/c["rps"],
            callback_cpu_us=d.get("python_decision_cpu_ns", 0)/n/1000,
            decision_roundtrip_us=d["decision_wait_ns"]/n/1000,
            items_per_batch=n/batches if batches else 0,
            return_call_reduction=1-batches/n if batches else 0,
            reactor_post_reduction=1-posts/n if posts else 0,
            throttled_usec=row["cgroup_delta"]["throttled_usec"]))
        total += n
        verified.append(dict(file=path.name, requests=n, sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
    summary[mode] = dict(rows=values, medians={k:median(v[k] for v in values) for k in values[0] if k != "round"})

summary["paired_rps_gains"] = [summary["seastar-batch"]["rows"][i]["rps"] / summary["seastar"]["rows"][i]["rps"] - 1 for i in range(3)]
summary["median_rps_gain"] = summary["seastar-batch"]["medians"]["rps"] / summary["seastar"]["medians"]["rps"] - 1
summary["batch_fraction_of_native"] = summary["seastar-batch"]["medians"]["rps"] / summary["native"]["medians"]["rps"]
summary["verified_responses"] = total
correctness = json.loads((ROOT / "results/batched-correctness/correctness.json").read_text())
summary["correctness"] = dict(cases=len(correctness)+2,
    failed=[r["test"] for r in correctness if r["returncode"]],
    udp_repeat=json.loads((ROOT / "results/batched-correctness/udp-repeat.json").read_text()))
(OUT / "summary.json").write_text(json.dumps(summary, indent=2)+"\n")
(OUT / "verification.json").write_text(json.dumps(dict(rows=verified, verified_responses=total,
    benchmark_rows_passed=True, source_fingerprints_match=True,
    full_correctness_suite_passed=not summary["correctness"]["failed"]), indent=2)+"\n")
print(json.dumps(summary, indent=2))
