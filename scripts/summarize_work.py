"""Derive report tables from all 84 measured rows, retaining every repetition."""
import json
from pathlib import Path
import statistics

root = Path(__file__).resolve().parents[1] / "results/phase2"
runs = json.loads((root / "runs.json").read_text())
assert len(runs) == 14 and all(r["returncode"] == 0 for r in runs)
records = []
inputs = {}
total_requests = 0
for run in runs:
    data = json.loads((root / f"{run['mode']}-{run['lanes']}.json").read_text())
    assert len(data["rows"]) == 6
    if "final_stats" in data:
        assert all(data["final_stats"][key] == 0 for key in
                   ("pending_requests", "pending_bytes", "queued_completions", "pool_queued", "pool_active", "pool_live", "live_workers", "rejected"))
    for size in (1024, 1048576):
        rows = [r for r in data["rows"] if r["payload_bytes"] == size]
        assert {r["repeat"] for r in rows} == {0, 1, 2}
        for row in rows:
            signature = row["input_sha256"], row["compressed_sha256"], row["compressed_bytes"]
            assert signature == inputs.setdefault(size, signature)
            assert row["elapsed_seconds"] >= 5 and row["requests"] > 0
            total_requests += row["requests"]
        def stats(values):
            values = list(values)
            return {"median": statistics.median(values), "min": min(values), "max": max(values)}
        records.append({"mode": data["mode"], "lanes": data["lanes"], "bytes": size,
                        "rps": stats(r["rps"] for r in rows),
                        "cpu": stats(r["process_average_cpus"] for r in rows),
                        "algorithm_cpus": stats(r["algorithm_average_cpus"] for r in rows),
                        "latency_p99_us": stats(r["latency"]["p99_us"] for r in rows),
                        "lag_p99_us": stats(r["loop_lag"]["p99_us"] for r in rows),
                        "lag_max_us": max(r["loop_lag"]["max_us"] for r in rows),
                        "sequential_p50_us": rows[0]["sequential_latency"]["p50_us"],
                        "algorithm_cpu_ms_per_request": stats(r["algorithm_thread_cpu_seconds"] / r["requests"] * 1000 for r in rows),
                        "cgroup_throttled_periods": sum(r["cgroup_delta"].get("nr_throttled", 0) for r in rows)})
summary = {"configurations": len(runs), "measured_rows": len(runs) * 6,
           "measured_requests": total_requests, "input_signatures": inputs, "records": records}
(root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
text = []
for size in (1024, 1048576):
    text += [f"## {size} B", "", "| 路径 | 计算线程/shard | RPS 中位数（范围） | 平均 CPU 用量中位数 | 心跳 P99 ms 范围 | 请求 P99 ms 范围 |", "|---|---:|---:|---:|---:|---:|"]
    for r in records:
        if r["bytes"] != size:
            continue
        rate, lag, latency = r["rps"], r["lag_p99_us"], r["latency_p99_us"]
        text.append(f"| {r['mode']} | {r['lanes']} | {rate['median']:.1f} ({rate['min']:.1f}–{rate['max']:.1f}) | {r['cpu']['median']:.2f} | {lag['min']/1000:.3f}–{lag['max']/1000:.3f} | {latency['min']/1000:.3f}–{latency['max']/1000:.3f} |")
    text.append("")
(root / "tables.md").write_text("\n".join(text))
print(json.dumps({"configurations": len(runs), "measured_rows": len(runs) * 6, "measured_requests": total_requests}))
