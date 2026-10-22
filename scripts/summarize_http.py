"""Summarize every formal repeat; refuse incomplete or inconsistent data."""
import json
from collections import defaultdict
from pathlib import Path
from statistics import median

root = Path(__file__).resolve().parents[1]
folder = root / "results/http"
keys = json.loads((folder / "index.json").read_text())
manifest = json.loads((folder / "manifest.json").read_text())
assert len(keys) == len(set(keys)) == manifest["cases"]*manifest["repeats"] == 81
groups = defaultdict(list)
requests = 0
for key in keys:
    r = json.loads((folder / (key + ".json")).read_text())
    c, s = r["client"], r["server"]
    d = s["delta"]
    assert r["returncodes"] == [0, 0] and c["errors"] == 0, key
    assert c["requests"] == d["responses"], (key, c["requests"], d["responses"])
    assert c["new_connections"] == 0 and c["total_connections"] == 32, key
    assert d["python_decisions"] == (0 if r["mode"] == "native" else c["requests"]), key
    assert d["body_bytes_to_python"] == d["body_bytes_from_python"] == (c["requests"]*r["size"] if r["mode"] == "copy" else 0), key
    assert len(c["server_shards"]) == 2, key
    assert all(t["affinity"] and set(t["affinity"]) <= set(r["ready"]["server_cpu_set"]) for t in s["threads"]), key
    assert all(r["closed"]["stats"][k] == 0 for k in ("active_requests", "reserved_bytes", "pending_decisions", "queued_decisions")), key
    worker_ids = {w["tid"] for w in r["ready"]["workers"]}
    python_thread = next(t for t in s["threads"] if t["tid"] == r["ready"]["pid"])
    requests += c["requests"]
    metrics = {"rps": c["rps"], "request_p50_us": c["p50_us"], "request_p99_us": c["p99_us"],
        "server_cpu": s["cpu_cores"], "python_cpu": python_thread["cpu_cores"], "client_cpu": c["cpu_cores"],
        "worker_cpu": sum(t["cpu_cores"] for t in s["threads"] if t["tid"] in worker_ids),
        "loop_p99_us": s["loop_lag"]["p99_us"], "loop_max_us": s["loop_lag"]["max_us"],
        "decision_cpu_us": d["python_decision_cpu_ns"]/max(1,d["python_decisions"])/1000,
        "decision_wait_us": d["decision_wait_ns"]/max(1,d["decisions"])/1000,
        "algorithm_us": d["compute_ns"]/max(1,c["requests"])/1000,
        "worker_queue_us": d["worker_queue_ns"]/max(1,c["requests"])/1000,
        "python_body_MBps": d["body_bytes_to_python"]/s["seconds"]/1e6,
        "server_window_excess_ms": (s["seconds"]-c["seconds"])*1000,
        "throttled_us": s["cgroup_delta"]["throttled_usec"]}
    groups[(r["mode"],r["route"],r["size"],r["burn"],r["workers"])].append((r["repeat"],key,metrics))
summary=[]
for (mode,route,size,burn,workers), rows in sorted(groups.items()):
    assert sorted(r[0] for r in rows) == [0,1,2]
    stats = {k: {"median": median(r[2][k] for r in rows), "min": min(r[2][k] for r in rows),
                 "max": max(r[2][k] for r in rows), "all": [r[2][k] for r in sorted(rows)]}
             for k in rows[0][2]}
    summary.append(dict(mode=mode,route=route,size=size,burn=burn,workers=workers,metrics=stats,
                        keys=[r[1] for r in sorted(rows)]))
output={"rows":len(keys),"requests":requests,"errors":0,"groups":summary}
(folder/"summary.json").write_text(json.dumps(output,indent=2))
print(json.dumps({k:v for k,v in output.items() if k!="groups"}))
for r in summary:
    m=r["metrics"]
    print(f"{r['mode']:6} {r['route']:8} {r['size']:7} b{r['burn']:4} w{r['workers']} "
          f"RPS {m['rps']['median']:9.1f} [{m['rps']['min']:.1f},{m['rps']['max']:.1f}] "
          f"CPU {m['server_cpu']['median']:.2f} Py {m['python_cpu']['median']:.2f} "
          f"lag99 {m['loop_p99_us']['median']:.0f}us client {m['client_cpu']['median']:.2f}")
