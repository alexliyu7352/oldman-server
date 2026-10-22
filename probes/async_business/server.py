"""Actual Python async business before and after a native operation; no policy route."""
import argparse
import asyncio
import os
from pathlib import Path
import sys
import threading
import time
import zlib

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import comparison_server as comparison
from comparison_server import web, native, compress_bound
import uvloop
from oldman_server.native.engine import Config, Engine, ServerConfig, HttpResponse


def level_from(parameter):
    try:
        level = int(parameter or "6")
    except ValueError:
        return None
    return level if 0 <= level <= 9 else None


async def business(req):
    level = level_from(req.parameter)
    if level is None:
        return HttpResponse(status=400)
    result = await req.compress(level=level)
    if result.size > compress_bound(req.body_size):
        return HttpResponse(status=422)
    return HttpResponse(body=result, tag=str(result.size))


class NativeService(comparison.SeastarService):
    async def start(self, port, listener=None):
        cooperative = self.mode == "seastar-cooperative"
        inline = self.mode == "seastar-inline"
        workers = [] if cooperative or inline else self.cpus[1:] if self.mode == "seastar-dedicated" else self.cpus
        self.engine = await Engine.open(Config(shard_cpus=tuple(self.cpus) if cooperative or inline else (self.cpus[0],),
            worker_cpus=tuple(workers), share_worker_cpus=True, batch_decisions=True,
            server=ServerConfig(http_port=port, max_requests=self.requests,
                max_bytes=self.byte_limit, decision_timeout_ms=5000,
                coalesce_notifications=True, cooperative_compression=cooperative,
                inline_compression=inline)), http_handler=business)
        os.sched_setaffinity(0, {self.cpus[0]})

    def info(self):
        return super().info() | {"event_loop": str(type(asyncio.get_running_loop())),
                                "uvloop": uvloop.__version__, "business": "Python await native; post-result decision"}


class PythonService(comparison.PythonService):
    async def start(self, port, listener=None):
        mode = self.mode
        if mode.startswith("multiprocess"):
            self.mode = "multiprocess"
        await super().start(port, listener)
        self.mode = mode

    async def handle(self, req):
        n = req.content_length
        if n is None or req.headers.get("Transfer-Encoding"):
            return web.Response(status=411)
        if n > 1048576:
            return web.Response(status=413)
        amount = n + compress_bound(n)
        if not self.budget.acquire(amount):
            self.counts["rejected"] += 1
            return web.Response(status=503)
        try:
            payload = await req.content.readexactly(n)
            level = level_from(req.query.get("level", ""))
            if level is None:
                return web.Response(status=400)
            self.counts["python_decisions"] += 1
            # Fastest simple multiprocess baseline: native algorithm directly on
            # each worker loop. It releases the GIL but occupies this OS thread.
            if self.mode.endswith("zlib"):
                started = time.thread_time_ns()
                result = zlib.compress(payload, level)
                metrics = {"compute_cpu_ns": time.thread_time_ns() - started, "work_tid": threading.get_native_id()}
            else:
                result, metrics = native.native_compress(payload, level)
            self.counts["native_calls"] += 1
            self.counts["compute_cpu_ns"] += metrics["compute_cpu_ns"]
            if len(result) > compress_bound(n):
                return web.Response(status=422)
            response = web.Response(body=result, headers={
                "Content-Type": "application/octet-stream", "X-Probe-Pid": str(os.getpid()),
                "X-Probe-Shard": str(self.cpus[0]), "X-Probe-Worker": str(metrics["work_tid"]),
                "X-App-Result": str(len(result))})
            response.enable_chunked_encoding()
            await response.prepare(req)
            await response.write_eof()
            self.counts["responses"] += 1
            return response
        finally:
            self.budget.release(amount)

    def info(self):
        return super().info() | {"event_loop": str(type(asyncio.get_running_loop())), "uvloop": uvloop.__version__,
            "compression": "stdlib zlib" if self.mode.endswith("zlib") else "shared C++ algorithm",
            "zlib": zlib.ZLIB_RUNTIME_VERSION if self.mode.endswith("zlib") else native.zlib_runtime_version}


def child(args, cpu, requests, byte_limit, connection, inherited):
    for pipe in inherited:
        pipe.close()
    uvloop.run(comparison.worker(args, [cpu], requests, byte_limit, connection))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True)
    parser.add_argument("--port", required=True, type=int)
    args = parser.parse_args()
    args.requests, args.bytes = 128, 256*1048576
    cpus = sorted(os.sched_getaffinity(0))
    comparison.PythonService, comparison.SeastarService = PythonService, NativeService
    comparison.child = child
    if args.mode.startswith("multiprocess"):
        comparison.prefork(args, cpus)
    else:
        uvloop.run(comparison.worker(args, cpus, args.requests, args.bytes))
