"""Bridge costs only. Not a CPU speedup or server throughput benchmark."""
import asyncio
import json
import os
from pathlib import Path
import statistics
import threading
import time

from oldman_server.native.engine import Config, Engine
from oldman_server.native import _native as _seastar_probe


def distribution(samples):
    values = sorted(samples)
    def pct(p):
        return values[min(len(values) - 1, int((len(values) - 1) * p))]
    return {"samples": len(values), "p50_us": pct(.50) * 1e6,
            "p99_us": pct(.99) * 1e6, "max_us": values[-1] * 1e6,
            "mean_us": statistics.fmean(values) * 1e6}


async def heartbeat(done, samples):
    loop = asyncio.get_running_loop()
    while not done.is_set():
        target = loop.time() + .001
        await asyncio.sleep(.001)
        samples.append(max(0, loop.time() - target))


def threads():
    result = []
    for task in Path("/proc/self/task").iterdir():
        try:
            fields = (task / "stat").read_text().rsplit(")", 1)[1].split()
            result.append({"tid": int(task.name), "name": (task / "comm").read_text().strip(),
                           "affinity": sorted(os.sched_getaffinity(int(task.name))),
                           "cpu_ticks": int(fields[11]) + int(fields[12])})
        except FileNotFoundError:
            pass
    return result


async def main():
    available = sorted(os.sched_getaffinity(0))
    engine = await Engine.open(Config(shard_cpus=tuple(available[-2:])))
    os.sched_setaffinity(0, {available[0]})
    threads_before = threads()
    rows = []
    try:
        for payload_size in (0, 1024, 65536, 1048576):
            payload = b"a" * payload_size
            for _ in range(100):
                await engine.echo(payload)
            for repeat in range(3):
                copy_times = []
                for _ in range(1000 if payload_size < 1048576 else 200):
                    start = time.perf_counter()
                    copied = _seastar_probe.copy_roundtrip(payload)
                    copy_times.append(time.perf_counter() - start)
                    assert copied == payload
                latencies = []
                for _ in range(1000 if payload_size < 1048576 else 200):
                    start = time.perf_counter()
                    r = await engine.echo(payload)
                    latencies.append(time.perf_counter() - start)
                    assert r.payload == payload
                lag = []
                done = asyncio.Event()
                beat = asyncio.create_task(heartbeat(done, lag))
                count = 0
                batch = 32 if payload_size < 1048576 else 16
                begin = time.perf_counter()
                cpu_begin = time.process_time()
                while time.perf_counter() - begin < 2.0:
                    results = await asyncio.gather(*(engine.echo(payload) for _ in range(batch)))
                    assert all(r.payload == payload for r in results)
                    count += batch
                elapsed = time.perf_counter() - begin
                cpu_seconds = time.process_time() - cpu_begin
                done.set()
                await beat
                rows.append({"payload_bytes": payload_size, "repeat": repeat,
                             "sequential_roundtrip": distribution(latencies),
                             "synchronous_two_copy_baseline": distribution(copy_times),
                             "concurrency": batch, "requests": count,
                             "elapsed_seconds": elapsed, "requests_per_second": count / elapsed,
                             "process_cpu_seconds": cpu_seconds,
                             "loop_lag": distribution(lag)})
                print(json.dumps(rows[-1]), flush=True)
        result = {"pid": os.getpid(), "python_tid": threading.get_native_id(),
                  "python_affinity": sorted(os.sched_getaffinity(0)),
                  "shards": engine.shards(), "rows": rows,
                  "threads_before": threads_before, "threads_after_work": threads(),
                  "clock_ticks_per_second": os.sysconf("SC_CLK_TCK"),
                  "smaps_rollup": Path("/proc/self/smaps_rollup").read_text(),
                  "scope": "echo bridge with two copies; timers not CPU work; no server tested"}
    finally:
        await engine.aclose()
    result["final_stats"] = engine.stats()
    result["threads_after_close"] = threads()
    Path("results/bridge.json").write_text(json.dumps(result, indent=2) + "\n")


asyncio.run(main())
