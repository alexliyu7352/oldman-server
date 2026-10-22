"""20 ms synchronous native waits: isolation evidence, not a throughput claim."""
import asyncio
import json
import os
from pathlib import Path
import time

from benchmark_work import distribution_ns, threads
from oldman_server.native.engine import Config, Engine


async def main():
    cpus = sorted(os.sched_getaffinity(0))
    engine = await Engine.open(Config(shard_cpus=(cpus[1],), worker_cpus=(cpus[2],)))
    os.sched_setaffinity(0, {cpus[0]})
    done = asyncio.Event()
    heartbeat_lag, echoes, waits = [], [], []
    echoes_during_active_work = 0
    async def pulse():
        nonlocal echoes_during_active_work
        while not done.is_set():
            due = time.monotonic_ns() + 1_000_000
            await asyncio.sleep(.001)
            heartbeat_lag.append(max(0, time.monotonic_ns() - due))
            was_active = engine.stats()["pool_active"] == 1
            start = time.monotonic_ns()
            assert (await engine.echo(b"alive")).payload == b"alive"
            echoes.append(time.monotonic_ns() - start)
            if was_active and engine.stats()["pool_active"] == 1:
                echoes_during_active_work += 1
    pulse_task = asyncio.create_task(pulse())
    try:
        for _ in range(100):
            result = await engine.blocking_wait(20)
            m = result.metrics
            assert m["work_tid"] == engine.workers()[0]["tid"]
            assert m["shard_tid"] == engine.shards()[0]["tid"]
            assert m["work_end_ns"] - m["work_start_ns"] >= 19_000_000
            waits.append(m["work_end_ns"] - m["work_start_ns"])
    finally:
        done.set()
        await pulse_task
        await engine.aclose()
    assert echoes_during_active_work > 0
    report = {"pid": os.getpid(), "wait_count": len(waits), "native_wait": distribution_ns(waits),
              "python_loop_lag": distribution_ns(heartbeat_lag), "echo_latency": distribution_ns(echoes),
              "echoes_completed_while_worker_active": echoes_during_active_work,
              "shards": engine.shards(), "workers": engine.workers(), "final_stats": engine.stats(),
              "threads_after_close": threads()}
    Path("results/phase2/isolation.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report), flush=True)


asyncio.run(main())
