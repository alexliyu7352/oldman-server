"""Stage 2 operation comparison; closed-loop, full bytes returned to Python."""
import argparse
import asyncio
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import random
import statistics
import threading
import time
import zlib

from oldman_server.native import _native as native
from oldman_server.native.engine import Config, Engine


def distribution_ns(samples):
    if not samples:
        return {"samples": 0}
    values = sorted(samples)
    def percentile(p):
        return values[int((len(values) - 1) * p)] / 1000
    return {"samples": len(values), "p50_us": percentile(.5), "p99_us": percentile(.99),
            "max_us": values[-1] / 1000, "mean_us": statistics.fmean(values) / 1000}


def payload_for(size):
    rng = random.Random(20260924)
    chunks = []
    remaining = size
    while remaining:
        n = min(16384, remaining // 2)
        if not n:
            chunks.append(b"x")
            break
        block = rng.randbytes(n)
        chunks.append(block + block)
        remaining -= 2 * n
    return b"".join(chunks)


def threads():
    rows = []
    for task in Path("/proc/self/task").iterdir():
        try:
            fields = (task / "stat").read_text().rsplit(")", 1)[1].split()
            rows.append({"tid": int(task.name), "name": (task / "comm").read_text().strip(),
                         "affinity": sorted(os.sched_getaffinity(int(task.name))),
                         "cpu_ticks": int(fields[11]) + int(fields[12])})
        except (FileNotFoundError, ProcessLookupError):
            pass
    return rows


def cgroup_stats():
    return dict((k, int(v)) for k, v in (line.split() for line in Path("/sys/fs/cgroup/cpu.stat").read_text().splitlines()))


class Runner:
    def __init__(self, args):
        self.args = args
        self.engine = None
        self.pool = None
        self.worker_info = []

    async def start(self):
        available = sorted(os.sched_getaffinity(0))
        if len(available) < 8:
            raise RuntimeError("benchmark requires eight available CPUs")
        self.available = available
        a = self.args
        if a.mode == "seastar_pool":
            self.engine = await Engine.open(Config(shard_cpus=(available[1],),
                                                  worker_cpus=tuple(available[2:2+a.lanes])))
        elif a.mode == "cooperative":
            self.engine = await Engine.open(Config(shard_cpus=tuple(available[1:1+a.lanes])))
        os.sched_setaffinity(0, {available[0]})
        if a.mode == "threadpool":
            cpus = iter(available[1:8] if a.lanes == 7 else available[2:2+a.lanes])
            lock = threading.Lock()
            def initialize():
                with lock:
                    cpu = next(cpus)
                os.sched_setaffinity(0, {cpu})
                with lock:
                    self.worker_info.append({"tid": threading.get_native_id(), "affinity": sorted(os.sched_getaffinity(0))})
            self.pool = ThreadPoolExecutor(a.lanes, initializer=initialize, thread_name_prefix="standard-pool")
            barrier = threading.Barrier(a.lanes)
            loop = asyncio.get_running_loop()
            await asyncio.gather(*(loop.run_in_executor(self.pool, barrier.wait) for _ in range(a.lanes)))

    async def call(self, payload):
        a = self.args
        submitted = time.monotonic_ns()
        if a.mode == "direct":
            output, m = native.native_compress(payload, 6)
        elif a.mode == "threadpool":
            output, m = await asyncio.get_running_loop().run_in_executor(self.pool, native.native_compress, payload, 6)
            # Includes executor scheduling, GIL acquisition, input copy and binding entry.
            m["executor_submit_to_work_ns"] = m["work_start_ns"] - submitted
            # Includes output copy, GIL reacquisition, future notification and Python resume.
            m["executor_work_end_to_resume_ns"] = time.monotonic_ns() - m["work_end_ns"]
        else:
            r = await self.engine.compress(payload, mode="pool" if a.mode == "seastar_pool" else "cooperative")
            output, m = r.payload, r.metrics
        return output, m

    async def close(self):
        if self.engine:
            await self.engine.aclose()
        if self.pool:
            self.pool.shutdown()


async def phase(runner, payload, seconds, concurrency, collect):
    loop = asyncio.get_running_loop()
    done = asyncio.Event()
    lag = []
    missed = 0
    high_water = defaultdict(int)
    async def heartbeat():
        nonlocal missed
        ticks = 0
        while not done.is_set():
            due = time.monotonic_ns() + 1_000_000
            await asyncio.sleep(.001)
            delay = max(0, time.monotonic_ns() - due)
            lag.append(delay)
            missed += delay // 1_000_000
            ticks += 1
            if runner.engine and ticks % 10 == 0:
                status = runner.engine.stats()
                for key in ("pending_requests", "pending_bytes", "pool_queued", "pool_active", "queued_completions"):
                    high_water[key] = max(high_water[key], status[key])
    beat = asyncio.create_task(heartbeat()) if collect else None
    if beat:
        await asyncio.sleep(0)
    latency = []
    parts = defaultdict(list)
    count = 0
    active_cpu_ns = 0
    sample_stride = 32 if len(payload) < 65536 else 1
    last_output = None
    before_threads = threads() if collect else []
    before_cgroup = cgroup_stats() if collect else {}
    cpu_begin = time.process_time()
    begin = time.monotonic()
    deadline = begin + seconds
    async def lane():
        nonlocal count, active_cpu_ns, last_output
        while time.monotonic() < deadline:
            start = time.monotonic_ns()
            output, m = await runner.call(payload)
            end = time.monotonic_ns()
            count += 1
            if collect:
                latency.append(end - start)
                active_cpu_ns += m["compute_cpu_ns"]
                if count % sample_stride == 0:
                    for key in ("input_copy_ns", "output_copy_ns", "dispatch_ns", "queue_ns", "algorithm_ns",
                                "compute_cpu_ns", "return_to_shard_ns", "notification_wait_ns",
                                "python_callback_ns", "python_resume_ns", "executor_submit_to_work_ns",
                                "executor_work_end_to_resume_ns"):
                        if key in m:
                            parts[key].append(m[key])
            last_output = output
            # Give the loop a chance between direct synchronous calls; no yield inside the call.
            if runner.args.mode == "direct":
                await asyncio.sleep(0)
    await asyncio.gather(*(lane() for _ in range(concurrency)))
    elapsed = time.monotonic() - begin
    cpu_seconds = time.process_time() - cpu_begin
    after_threads = threads() if collect else []
    after_cgroup = cgroup_stats() if collect else {}
    done.set()
    if beat:
        await beat
    assert zlib.decompress(last_output) == payload
    return {"requests": count, "elapsed_seconds": elapsed, "rps": count / elapsed,
            "concurrency": concurrency, "latency": distribution_ns(latency),
            "loop_lag": distribution_ns(lag), "missed_1ms_intervals": missed,
            "process_cpu_seconds": cpu_seconds, "process_average_cpus": cpu_seconds / elapsed,
            "algorithm_thread_cpu_seconds": active_cpu_ns / 1e9,
            "algorithm_average_cpus": active_cpu_ns / 1e9 / elapsed,
            "parts_sample_every": sample_stride, "parts": {k: distribution_ns(v) for k, v in parts.items()},
            "threads_before": before_threads, "threads_after": after_threads,
            "cgroup_delta": {k: after_cgroup[k] - v for k, v in before_cgroup.items()},
            "sampled_queue_high_water": dict(high_water)}


async def main(args):
    runner = Runner(args)
    await runner.start()
    rows = []
    result = {"mode": args.mode, "lanes": args.lanes, "pid": os.getpid(),
              "python_tid": threading.get_native_id(), "python_affinity": sorted(os.sched_getaffinity(0)),
              "zlib_runtime": native.zlib_runtime_version, "zlib_header": native.zlib_header_version,
              "cpu_max": Path("/sys/fs/cgroup/cpu.max").read_text().strip(),
              "clock_ticks_per_second": os.sysconf("SC_CLK_TCK"), "arguments": vars(args),
              "shards": runner.engine.shards() if runner.engine else [],
              "workers": runner.engine.workers() if runner.engine else runner.worker_info,
              "threads_ready": threads(), "rows": rows}
    try:
        for size in args.sizes:
            payload = payload_for(size)
            reference = zlib.compress(payload, 6)
            out, _ = await runner.call(payload)
            assert out == reference and zlib.decompress(out) == payload
            sequential = []
            for _ in range(100 if size == 1024 else 20):
                start = time.monotonic_ns()
                await runner.call(payload)
                sequential.append(time.monotonic_ns() - start)
            for repeat in range(args.repeats):
                concurrency = 1 if args.mode == "direct" else 2 * args.lanes
                await phase(runner, payload, args.warmup, concurrency, False)
                row = await phase(runner, payload, args.seconds, concurrency, True)
                row.update({"payload_bytes": size, "compressed_bytes": len(reference), "repeat": repeat,
                            "input_sha256": hashlib.sha256(payload).hexdigest(),
                            "compressed_sha256": hashlib.sha256(reference).hexdigest(),
                            "sequential_latency": distribution_ns(sequential)})
                rows.append(row)
                print(json.dumps({"mode": args.mode, "lanes": args.lanes, "size": size, "repeat": repeat,
                                  "rps": row["rps"], "cpu": row["process_average_cpus"], "lag": row["loop_lag"]}), flush=True)
        result["smaps_rollup"] = Path("/proc/self/smaps_rollup").read_text()
    finally:
        await runner.close()
    result["threads_after_close"] = threads()
    if runner.engine:
        result["final_stats"] = runner.engine.stats()
    Path(args.output).write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("direct", "threadpool", "seastar_pool", "cooperative"), required=True)
    parser.add_argument("--lanes", type=int, default=1)
    parser.add_argument("--sizes", type=int, nargs="+", default=[1024, 1048576])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--warmup", type=float, default=2)
    parser.add_argument("--seconds", type=float, default=5)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if not 1 <= args.lanes <= (6 if args.mode == "seastar_pool" else 7):
        parser.error("lane count exceeds the eight-CPU budget")
    asyncio.run(main(args))
