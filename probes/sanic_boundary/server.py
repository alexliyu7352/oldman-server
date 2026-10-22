"""Fresh-process Sanic service for the cost probe."""
import argparse
import asyncio
from contextlib import asynccontextmanager
import cProfile
import hashlib
import importlib
import json
import os
from pathlib import Path
import pstats
import sys
import time
import tracemalloc

import uvloop

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from oldman_server.native.engine import Config, Engine, ServerConfig
from oldman_server.native.host import serve
from probes.application_lifecycle.inputs import runtime_state
from probes.sanic_spike.run import lifespan, quiescent
from probes.sanic_boundary.application import buffered_asgi, make_app


MODULES = ("oldman_server", "oldman_server.native.engine", "oldman_server.native.host",
           "oldman_server.native._native")


def module_evidence():
    modules = {}
    for name in MODULES:
        path = Path(importlib.import_module(name).__file__).resolve()
        modules[name] = {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    return {"modules": modules, "version": importlib.import_module("oldman_server").__version__}


@asynccontextmanager
async def host(app, config, mode):
    if mode == "native":
        async with serve(app, config) as engine:
            yield engine
    else:
        async with lifespan(app):
            engine = await Engine.open(config, application_handler=buffered_asgi(
                app, config.server.max_response_bytes))
            try:
                yield engine
            finally:
                await engine.aclose()


def emit(value):
    print(json.dumps(value), flush=True)


async def commands():
    reader = asyncio.StreamReader()
    transport, _ = await asyncio.get_running_loop().connect_read_pipe(
        lambda: asyncio.StreamReaderProtocol(reader), sys.stdin)
    try:
        while line := await reader.readline():
            yield line.decode().strip()
    finally:
        transport.close()


async def main(args):
    cpus = sorted(os.sched_getaffinity(0))
    if len(cpus) != 3:
        raise ValueError("service requires three CPUs")
    config = Config(shard_cpus=tuple(cpus[1:]), server=ServerConfig(
        http_port=args.port, application_dispatch_all=True, max_requests=1024,
        max_bytes=64*1024*1024, max_body_bytes=65536, max_response_bytes=65536,
        decision_timeout_ms=5000, generate_date_header=True, coalesce_notifications=True))
    app = make_app()
    async with host(app, config, args.mode) as engine:
        os.sched_setaffinity(0, {cpus[0]})
        emit({"ready": True, "pid": os.getpid(), "runtime": runtime_state(),
              "loaded": module_evidence(),
              "loop": str(type(asyncio.get_running_loop())), "shards": engine.shards()})
        before = profiler = None
        async for command in commands():
            if command == "begin":
                if before is not None:
                    raise ValueError("begin twice")
                before = await quiescent(engine)
                if args.diagnostic == "cprofile":
                    # CPU on the Python event-loop thread; excludes time asleep.
                    profiler = cProfile.Profile(time.thread_time)
                    profiler.enable()
                elif args.diagnostic == "tracemalloc":
                    tracemalloc.start()
                cpu0, thread0 = time.process_time_ns(), time.thread_time_ns()
                emit({"begun": True})
            elif command == "end":
                after = await quiescent(engine)
                cpu1, thread1 = time.process_time_ns(), time.thread_time_ns()
                evidence = {}
                if profiler:
                    profiler.disable()
                    profiler.dump_stats(str(args.out / "profile.pstats"))
                    stats = pstats.Stats(profiler)
                    evidence["calls"] = [{"file": key[0], "line": key[1], "function": key[2],
                        "primitive_calls": value[0], "calls": value[1],
                        "self_thread_cpu_seconds": value[2], "cumulative_thread_cpu_seconds": value[3]}
                        for key, value in sorted(stats.stats.items())]
                elif args.diagnostic == "tracemalloc":
                    current, peak = tracemalloc.get_traced_memory()
                    snapshot = tracemalloc.take_snapshot()
                    evidence = {"traced_current_bytes": current, "traced_peak_bytes": peak,
                        "snapshot_live_blocks": sum(s.count for s in snapshot.statistics("filename")),
                        "top_live": [{"traceback": str(s.traceback), "bytes": s.size, "blocks": s.count}
                                     for s in snapshot.statistics("lineno")[:30]],
                        "scope": "Python-traced live memory, NOT cumulative allocations or C++ heap"}
                    tracemalloc.stop()
                emit({"after": after, "delta": {k: v-before[k] for k, v in after.items()
                                               if isinstance(v, int)},
                      "process_cpu_ns": cpu1-cpu0, "python_thread_cpu_ns": thread1-thread0,
                      "diagnostics": evidence})
            elif command == "quit":
                break
            else:
                raise ValueError(command)
    emit({"closed": True, "stats": engine.server_stats()})


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("native", "asgi"), required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--diagnostic", choices=("none", "cprofile", "tracemalloc"), default="none")
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    uvloop.run(main(args))
