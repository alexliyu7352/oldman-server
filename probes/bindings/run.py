"""One six-request fixture; isolate receive/return and the Python round trip."""
import argparse
import gc
import hashlib
import inspect
import json
import os
from pathlib import Path
import sys
import time
from importlib.metadata import version

import _binding_probe
from oldman_server.native.engine import Request

ROOT = Path(__file__).resolve().parents[2]
KINDS = ("PybindFixture", "CapiFixture")
PHASES = ("receive", "return", "roundtrip")
BATCH = 6


def policy(req):
    return req.method == "GET" and req.route == "fixed"


def run_phase(kind, phase, iterations):
    engine = getattr(_binding_probe, kind)()
    decisions = [(r[0], r[1], 1, b"") for r in engine.drain_requests(64)]
    counts = dict(python_decisions=0, python_decision_cpu_ns=0, python_decision_wall_ns=0)

    def operation():
        if phase == "receive":
            return engine.drain_requests(64)
        if phase == "return":
            return engine.resolve_requests(decisions)
        returned = []
        for values in engine.drain_requests(64):
            request = Request(*values)
            start, cpu = time.monotonic_ns(), time.thread_time_ns()
            answer = policy(request)
            if inspect.isawaitable(answer) or not isinstance(answer, bool):
                raise TypeError("synchronous boolean policy required")
            code = int(answer)
            counts["python_decisions"] += 1
            counts["python_decision_cpu_ns"] += time.thread_time_ns() - cpu
            counts["python_decision_wall_ns"] += time.monotonic_ns() - start
            returned.append((request.id, request.shard, code, request.body or b""))
        engine.resolve_requests(returned)

    gc.collect()
    warmup = 5000
    for _ in range(warmup): operation()
    wall0, cpu0 = time.perf_counter_ns(), time.thread_time_ns()
    for _ in range(iterations): operation()
    cpu, wall = time.thread_time_ns()-cpu0, time.perf_counter_ns()-wall0
    calls, received, last = engine.snapshot()
    if phase != "receive":
        assert calls == iterations+warmup and received == BATCH*calls
        assert last == decisions
    if phase == "roundtrip": assert counts["python_decisions"] == (iterations+warmup)*BATCH
    return dict(kind=kind, phase=phase, batches=iterations, items=iterations*BATCH,
                wall_ns=wall, cpu_ns=cpu, cpu_ns_per_item=cpu/(iterations*BATCH),
                wall_ns_per_item=wall/(iterations*BATCH), accepted_calls=calls,
                accepted_items=received)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--round", type=int, default=0)
    parser.add_argument("--iterations", type=int, default=300000)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    available = sorted(os.sched_getaffinity(0))
    os.sched_setaffinity(0, {available[0]})
    out = args.out
    out.mkdir(parents=True, exist_ok=True)
    destination = out / f"round-{args.round}.json"
    assert not destination.exists(), "refuse to overwrite measurement"
    paths = [Path(__file__), ROOT/"probes/bindings/probe.cc", ROOT/"probes/bindings/test_bindings.py",
             ROOT/"native/server.hh", ROOT/"src/oldman_server/native/engine.py", ROOT/"CMakeLists.txt",
             Path(_binding_probe.__file__)]
    hashes = {str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    manifest = out / "manifest.json"
    if not manifest.exists():
        manifest.write_text(json.dumps(dict(python=sys.version, pybind11=version("pybind11"),
            cpu=available[0], batch=BATCH, drain_limit=64, iterations=args.iterations,
            gc_enabled=gc.isenabled(), cpu_max=Path("/sys/fs/cgroup/cpu.max").read_text().strip(),
            fingerprints=hashes, note="same native fixture; no Seastar runtime, queues, notifications, sockets or thread hops"), indent=2)+"\n")
    saved = json.loads(manifest.read_text())
    assert saved["fingerprints"] == hashes and saved["iterations"] == args.iterations
    kinds = KINDS if args.round % 2 == 0 else tuple(reversed(KINDS))
    rows = []
    for phase in PHASES:
        for kind in kinds:
            row = run_phase(kind, phase, args.iterations)
            rows.append(row)
            print(json.dumps({k:row[k] for k in ("kind", "phase", "cpu_ns_per_item", "wall_ns_per_item")}), flush=True)
    destination.write_text(json.dumps(dict(round=args.round, rows=rows), indent=2)+"\n")
    by_key = {(r["phase"], r["kind"]):r for r in rows}
    ratios = {phase:by_key[(phase, "PybindFixture")]["cpu_ns_per_item"] / by_key[(phase, "CapiFixture")]["cpu_ns_per_item"] for phase in PHASES}
    print(json.dumps(dict(round=args.round, pybind_over_capi=ratios)), flush=True)


if __name__ == "__main__": main()
