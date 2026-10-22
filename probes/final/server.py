"""Final comparison: dynamic Python decisions and explicitly native policy routes."""
import argparse
import asyncio
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from probes.fixed.server import HybridService, FixedService, comparison, uvloop, web
from oldman_server.native.engine import Config, Engine, ServerConfig, NativePolicy

FILE = ""


class MeasuredFileResponse(web.FileResponse):
    def __init__(self, *args, budget, counts, **kwargs):
        super().__init__(*args, **kwargs)
        self.budget, self.counts = budget, counts

    async def prepare(self, request):
        # FileResponse sends the file during prepare; let aiohttp call it once.
        try:
            writer = await super().prepare(request)
            if self.status == 200: self.counts["responses"] += 1
            return writer
        finally:
            self.budget.release(2*65536)


class PythonService(FixedService):
    async def start(self, port, listener=None):
        os.sched_setaffinity(0, {self.cpus[0]})
        app = web.Application()
        app.router.add_get("/hybrid/fixed", self.handle)
        app.router.add_get("/hybrid/file", self.handle_file)
        self.runner = web.AppRunner(app, access_log=None, shutdown_timeout=.2,
                                   handler_cancellation=False, keepalive_timeout=120)
        await self.runner.setup()
        await web.TCPSite(self.runner, "127.0.0.1", port,
                         reuse_port=self.mode.startswith("multiprocess")).start()

    def info(self):
        loop = asyncio.get_running_loop()
        return super().info() | {"sendfile_implementation": loop.sendfile.__qualname__}

    async def handle_file(self, request):
        if not self.budget.acquire(2*65536):
            self.counts["rejected"] += 1
            return web.Response(status=503)
        if request.method != "GET" or request.path != "/hybrid/file":
            self.budget.release(2*65536)
            return web.Response(status=403)
        self.counts["python_decisions"] += 1
        # aiohttp's standard file response, including its available sendfile path.
        return MeasuredFileResponse(FILE, budget=self.budget, counts=self.counts,
            chunk_size=65536, headers={"Content-Type": "application/octet-stream",
                "X-Probe-Pid": self.pid, "X-Probe-Shard": str(self.cpus[0]), "X-Probe-Policy": "0"})


class NativeService(HybridService):
    def __init__(self, mode, cpus, requests, byte_limit):
        super().__init__(mode, cpus, requests, byte_limit)
        self.mode = mode
        self.policy_version = 0

    async def start(self, port, listener=None):
        def decide(req): return req.method == "GET" and req.route in ("fixed", "file")
        cpus = (self.cpus[:1] if self.mode.endswith("1") else
                self.cpus[1:] if self.mode in ("seastar-batch", "seastar-coalesced") else self.cpus)
        self.engine = await Engine.open(Config(shard_cpus=tuple(cpus), batch_decisions=True,
            server=ServerConfig(http_port=port, max_requests=self.requests,
                max_bytes=self.byte_limit, decision_timeout_ms=5000, file_path=FILE,
                coalesce_notifications=self.mode != "seastar-batch")), decision_handler=decide)
        os.sched_setaffinity(0, {self.cpus[0]})
        if self.mode.startswith("seastar-policy"):
            self.policy_version = await self.engine.install_policy(NativePolicy(allow_fixed=True, allow_file=True))

    def info(self):
        return super().info() | {"batch_decisions": True, "mode": self.mode, "policy_version": self.policy_version}


def child(args, cpu, requests, byte_limit, connection, inherited):
    for pipe in inherited: pipe.close()
    run_loop = asyncio.run if args.mode.endswith("asyncio") else uvloop.run
    run_loop(comparison.worker(args, [cpu], requests, byte_limit, connection))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--file", required=True)
    args = parser.parse_args()
    args.requests, args.bytes = 1024, 64*1024*1024
    FILE = args.file
    cpus = sorted(os.sched_getaffinity(0))
    comparison.PythonService, comparison.SeastarService = PythonService, NativeService
    comparison.child = child
    if args.mode.startswith("multiprocess"): comparison.prefork(args, cpus)
    else: uvloop.run(comparison.worker(args, cpus, args.requests, args.bytes))
