"""One GIL Python interpreter and loop; native HTTP bodies stay in C++."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import sys
import time

from benchmark_work import cgroup_stats, distribution_ns, threads
from oldman_server.native.engine import Config, Engine, ServerConfig


def arithmetic(iterations):
    value = 7
    for _ in range(iterations):
        value = (value * 1664525 + 1013904223) & 0xffffffff
    return value


async def main(args):
    cpus = sorted(os.sched_getaffinity(0))
    assert len(cpus) >= 6
    def decision(req):
        valid = (req.route in ("fixed", "file") and req.method == "GET" or
                 req.route == "compress" and req.method == "POST")
        if args.burn:
            arithmetic(args.burn)
        return valid
    engine = await Engine.open(Config(shard_cpus=tuple(cpus[1:3]),
        worker_cpus=tuple(cpus[3:3+args.workers]), server=ServerConfig(
            http_port=args.port, file_path=args.file, max_requests=256,
            max_bytes=256*1048576, decision_timeout_ms=5000)), decision_handler=decision)
    os.sched_setaffinity(0, {cpus[0]})
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    transport, _ = await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), sys.stdin)
    window = None
    lag = []
    async def heartbeat():
        while True:
            active = window is not None
            due = time.monotonic_ns() + 1_000_000
            await asyncio.sleep(.001)
            if active and window is not None:
                lag.append(max(0, time.monotonic_ns() - due))
    beat = asyncio.create_task(heartbeat())
    def emit(data):
        print(json.dumps(data), flush=True)
    emit({"ready": True, "pid": os.getpid(), "shards": engine.shards(), "workers": engine.workers(),
          "main_cpu": cpus[0], "server_cpu_set": cpus, "burn_iterations": args.burn})
    try:
        while line := await reader.readline():
            command = line.decode().strip()
            if command == "begin":
                lag.clear()
                window = {"stats": engine.server_stats(), "threads": threads(), "cgroup": cgroup_stats(),
                          "cpu": time.process_time(), "time": time.monotonic()}
                emit({"begun": True})
            elif command == "end":
                finish, cpu = time.monotonic(), time.process_time()
                after = engine.server_stats()
                before = window
                window = None
                elapsed = finish - before["time"]
                delta = {k: v-before["stats"][k] for k, v in after.items() if isinstance(v, int)}
                old = {r["tid"]: r for r in before["threads"]}
                thread_rows = []
                for r in threads():
                    r["cpu_seconds"] = (r["cpu_ticks"]-old.get(r["tid"], {}).get("cpu_ticks", 0))/os.sysconf("SC_CLK_TCK")
                    r["cpu_cores"] = r["cpu_seconds"]/elapsed
                    thread_rows.append(r)
                cg = cgroup_stats()
                emit({"seconds": elapsed, "cpu_seconds": cpu-before["cpu"],
                      "cpu_cores": (cpu-before["cpu"])/elapsed, "delta": delta, "after": after,
                      "threads": thread_rows, "loop_lag": distribution_ns(lag),
                      "cgroup_delta": {k: v-before["cgroup"][k] for k, v in cg.items()},
                      "status": Path("/proc/self/status").read_text()})
            elif command == "quit":
                break
            else:
                raise ValueError(command)
    finally:
        beat.cancel()
        await asyncio.gather(beat, return_exceptions=True)
        transport.close()
        await engine.aclose()
        emit({"closed": True, "stats": engine.server_stats()})


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--file", required=True)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--burn", type=int, default=0)
    asyncio.run(main(parser.parse_args()))
