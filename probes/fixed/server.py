"""Disposable 1 KiB fixed-response adapter. Reuse prior control and metrics code."""
import argparse
import asyncio
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import uvloop
from aiohttp import web
import comparison_server as comparison
from oldman_server.native.engine import Config, Engine, ServerConfig


class FixedService:
    def __init__(self, mode, cpus, requests, byte_limit):
        self.mode, self.cpus = mode, cpus
        self.budget = comparison.Budget(requests, byte_limit)
        self.counts = dict(responses=0, rejected=0, python_decisions=0)
        self.pid = str(os.getpid())

    async def start(self, port, listener=None):
        os.sched_setaffinity(0, {self.cpus[0]})
        app = web.Application()
        app.router.add_get("/hybrid/fixed", self.handle)
        self.runner = web.AppRunner(app, access_log=None, shutdown_timeout=.2,
                                   handler_cancellation=False, keepalive_timeout=120)
        await self.runner.setup()
        await web.TCPSite(self.runner, "127.0.0.1", port,
                         reuse_port=self.mode == "multiprocess").start()

    async def handle(self, request):
        size = int(request.query.get("size", "1024"))
        if size != 1024: return web.Response(status=400)
        if not self.budget.acquire(size):
            self.counts["rejected"] += 1
            return web.Response(status=503)
        try:
            if request.method != "GET" or request.path != "/hybrid/fixed":
                return web.Response(status=403)
            self.counts["python_decisions"] += 1
            response = web.Response(body=b"x" * size, headers={"Content-Type": "application/octet-stream",
                "X-Probe-Pid": self.pid, "X-Probe-Shard": str(self.cpus[0])})
            response.enable_chunked_encoding()
            await response.prepare(request)
            await response.write_eof()
            self.counts["responses"] += 1
            return response
        finally:
            self.budget.release(size)

    def stats(self):
        return {**self.counts, "active_requests": self.budget.active,
                "reserved_bytes": self.budget.reserved}

    def info(self):
        return {"pid": os.getpid(), "main_cpu": self.cpus[0],
                "event_loop": str(type(asyncio.get_running_loop())),
                "uvloop": uvloop.__version__, "aiohttp": comparison.aiohttp.__version__,
                "parser": str(comparison.aiohttp.http_parser.HttpRequestParser)}

    async def close(self):
        await self.runner.cleanup()


class HybridService:
    def __init__(self, mode, cpus, requests, byte_limit):
        self.cpus, self.requests, self.byte_limit = cpus, requests, byte_limit

    async def start(self, port, listener=None):
        def decide(req): return req.method == "GET" and req.route == "fixed"
        self.engine = await Engine.open(Config(shard_cpus=tuple(self.cpus[1:]),
            server=ServerConfig(http_port=port, max_requests=self.requests,
                max_bytes=self.byte_limit, decision_timeout_ms=5000)), decision_handler=decide)
        os.sched_setaffinity(0, {self.cpus[0]})

    def stats(self): return self.engine.server_stats()
    def info(self):
        return {"pid": os.getpid(), "main_cpu": self.cpus[0], "shards": self.engine.shards(),
                "event_loop": str(type(asyncio.get_running_loop())), "uvloop": uvloop.__version__}
    async def close(self): await self.engine.aclose()


def child(args, cpu, requests, byte_limit, connection, inherited):
    for pipe in inherited: pipe.close()
    uvloop.run(comparison.worker(args, [cpu], requests, byte_limit, connection))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["single", "multiprocess", "seastar"], required=True)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    args.requests, args.bytes = 1024, 64*1024*1024
    cpus = sorted(os.sched_getaffinity(0))
    comparison.PythonService, comparison.SeastarService = FixedService, HybridService
    comparison.child = child
    if args.mode == "multiprocess": comparison.prefork(args, cpus)
    else: uvloop.run(comparison.worker(args, cpus, args.requests, args.bytes))
