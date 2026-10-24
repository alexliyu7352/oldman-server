"""Ad-hoc served windows through the fixed comparison runner (diagnosis, not a gate).

Each case is mode:workload, optionally :ENVVAR=value passed to the service.
Prints RPS, service and main-thread CPU per request, per-thread cores, the cores
other processes used on the window's CPUs and their SMT siblings (foreign_cores),
and the share of the window in which some task stalled on CPU, memory or IO (pressure).

usage: calibrate.py [--server-cores 4 --workers 4] OUT_DIR mode:workload[:ENVVAR=value] ...
(--server-cores: server CPUs, stock running a worker on each; --workers: candidate
worker processes splitting them, one CPU each being the same-thread mode.)
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
from probes.sanic_comparison import run
from probes.sanic_boundary import run as boundary
from probes.application_lifecycle.inputs import runtime_fingerprint
from scripts.cpu_budget import describe_cpu


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server-cores", type=int, default=3, choices=(3, 4))
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("out", type=Path)
    parser.add_argument("cases", nargs="+")
    args = parser.parse_args()
    out = args.out
    out.mkdir(parents=True, exist_ok=False)
    budget = describe_cpu()
    layout, _ = boundary.layout_for(budget, args.server_cores)
    runtime = runtime_fingerprint()
    os.sched_setaffinity(0, set(layout["controller"]))
    tick = os.sysconf("SC_CLK_TCK")
    base_env = dict(os.environ)
    for case in args.cases:
        mode, workload, *extra = case.split(":", 2)
        os.environ.clear()
        os.environ.update(base_env)
        if extra:
            name, _, value = extra[0].partition("=")
            os.environ[name] = value
        row = run.execute(out / f"{mode}-{workload}-{time.time_ns()}", mode, workload, layout=layout,
                          budget=budget, runtime=runtime, warmup=2, seconds=5,
                          workers=args.workers if mode == "candidate" else 1,
                          scale=run.connection_scale(args.server_cores))
        seconds, pids = row["client"]["seconds"], row["ready"]["pids"]
        threads, main_ticks = [], 0
        for pid in pids:
            before, after = row["threads_before"][str(pid)], row["threads_after"][str(pid)]
            main_ticks += after[str(pid)]["ticks"] - before[str(pid)]["ticks"]
            for tid, sample in after.items():
                ticks = sample["ticks"] - before.get(tid, {}).get("ticks", 0)
                if ticks > 5:
                    threads.append(round(ticks / tick / seconds, 2))
        print(json.dumps({"case": case, "rps": round(row["client"]["rps"]),
                          "service_cpu_us": round(row["service_cpu_us_per_request"], 2),
                          "main_cpu_us": round(main_ticks / tick * 1e6 / row["client"]["requests"], 2),
                          "thread_cores": threads, "p99_us": round(row["client"]["p99_us"]),
                          "foreign_cores": row["foreign_cores"], "pressure": row["pressure"]}), flush=True)
    os.environ.clear()
    os.environ.update(base_env)


if __name__ == "__main__":
    main()
