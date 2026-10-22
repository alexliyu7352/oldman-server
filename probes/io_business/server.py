"""Same GET business using native I/O or aiohttp + uvloop workers."""
import argparse
import asyncio
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import comparison_server as comparison
from comparison_server import web
import aiohttp
import uvloop
from oldman_server.native.engine import Config, Engine, ServerConfig, HttpResponse

UPSTREAM_PORTS = ()


def origin_from(parameter):
    parameter = parameter or "0"
    if not parameter.isascii() or not parameter.isdecimal():
        return None
    origin = int(parameter)
    return origin if origin < len(UPSTREAM_PORTS) else None


async def business(req):
    origin = origin_from(req.parameter)
    if origin is None:
        return HttpResponse(status=400)
    result = await req.fetch(upstream=origin)
    if result.status != 200 or result.size == 0:
        return HttpResponse(status=502)
    return HttpResponse(body=result, tag=f"{result.status}:{result.size}")


class NativeService(comparison.SeastarService):
    async def start(self, port, listener=None):
        reactor_cpus = self.cpus[1:] if self.mode in ("seastar-dedicated", "seastar-retire") else self.cpus
        self.engine = await Engine.open(Config(shard_cpus=tuple(reactor_cpus), batch_decisions=True,
            retire_http_contexts=self.mode == "seastar-retire",
            server=ServerConfig(http_port=port, max_requests=self.requests, max_bytes=self.byte_limit,
                max_body_bytes=65536, decision_timeout_ms=5000, coalesce_notifications=True,
                upstream_ports=UPSTREAM_PORTS)), http_handler=business)
        if os.environ.get("PROBE_BRIDGE_TIMING_DIR"):
            from diagnostic_profile import TimedNative
            self.engine._native = TimedNative(self.engine._native)
        os.sched_setaffinity(0, {self.cpus[0]})

    def info(self):
        return super().info() | {"event_loop": str(type(asyncio.get_running_loop())),
                                "uvloop": uvloop.__version__, "origins": UPSTREAM_PORTS,
                                "retire_http_contexts": self.engine.config.retire_http_contexts}


class PythonService(comparison.PythonService):
    async def start(self, port, listener=None):
        os.sched_setaffinity(0, {self.cpus[0]})
        self.counts.update(python_http_started=0, python_http_resumed=0, python_http_finished=0,
            python_http_errors=0, upstream_requests=0, upstream_responses=0, upstream_bytes=0)
        self.client = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=self.budget.requests),
            timeout=aiohttp.ClientTimeout(total=5), auto_decompress=False,
            skip_auto_headers={"User-Agent", "Accept", "Accept-Encoding"})
        app = web.Application(client_max_size=65536)
        app.router.add_get("/python/fetch", self.handle)
        self.runner = web.AppRunner(app, access_log=None, shutdown_timeout=.2,
                                   handler_cancellation=False, keepalive_timeout=120)
        await self.runner.setup()
        await web.TCPSite(self.runner, "127.0.0.1", port, reuse_port=self.mode == "multiprocess").start()

    async def handle(self, req):
        if req.content_length or req.headers.get("Transfer-Encoding"):
            return web.Response(status=400)
        if not self.budget.acquire(65536):
            self.counts["rejected"] += 1
            return web.Response(status=503)
        task = asyncio.current_task()
        self.tasks.add(task)
        self.counts["python_http_started"] += 1
        try:
            origin = origin_from(req.query.get("upstream", ""))
            if origin is None:
                return web.Response(status=400)
            self.counts["upstream_requests"] += 1
            async with self.client.get(f"http://127.0.0.1:{UPSTREAM_PORTS[origin]}/native/fixed?size=8192") as response:
                if response.content_length is not None and response.content_length > 65536:
                    return web.Response(status=502)
                body = await response.content.read(65537)
                # read(n) may return only one network chunk; consume to EOF.
                parts, size = [body], len(body)
                while not response.content.at_eof():
                    chunk = await response.content.read(65537 - size)
                    if not chunk:
                        break
                    parts.append(chunk); size += len(chunk)
                    if size > 65536:
                        return web.Response(status=502)
                body = b"".join(parts)
                status = response.status
            self.counts["upstream_responses"] += 1
            self.counts["upstream_bytes"] += len(body)
            self.counts["python_http_resumed"] += 1
            if status != 200 or not body or len(body) > 65536:
                return web.Response(status=502)
            response = web.Response(body=body, headers={"Content-Type": "application/octet-stream",
                "X-Probe-Pid": str(os.getpid()), "X-Probe-Shard": str(self.cpus[0]),
                "X-App-Result": f"{status}:{len(body)}"})
            response.enable_chunked_encoding()
            await response.prepare(req)
            await response.write_eof()
            self.counts["responses"] += 1
            return response
        except asyncio.TimeoutError:
            self.counts["python_http_errors"] += 1
            return web.Response(status=504)
        except aiohttp.ClientError:
            self.counts["python_http_errors"] += 1
            return web.Response(status=502)
        finally:
            self.counts["python_http_finished"] += 1
            self.tasks.discard(task)
            self.budget.release(65536)

    def info(self):
        return super().info() | {"event_loop": str(type(asyncio.get_running_loop())),
                                "uvloop": uvloop.__version__, "origins": UPSTREAM_PORTS}

    async def close(self):
        try:
            await super().close()
        finally:
            await self.client.close()


def child(args, cpu, requests, byte_limit, connection, inherited):
    for pipe in inherited:
        pipe.close()
    uvloop.run(comparison.worker(args, [cpu], requests, byte_limit, connection))


if __name__ == "__main__":
    if os.environ.get("PROBE_PROFILE_DIR"):
        from diagnostic_profile import ProfilingSnapshot
        comparison.Snapshot = ProfilingSnapshot
    if os.environ.get("PROBE_BRIDGE_TIMING_DIR"):
        from diagnostic_profile import BridgeSnapshot, install_bridge_timers
        install_bridge_timers(Engine)
        comparison.Snapshot = BridgeSnapshot
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["seastar", "seastar-dedicated", "seastar-retire", "multiprocess"], required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--upstream", type=int, required=True)
    args = parser.parse_args()
    args.requests, args.bytes = 768, 64 * 1048576
    UPSTREAM_PORTS = (args.upstream,)
    cpus = sorted(os.sched_getaffinity(0))
    comparison.PythonService, comparison.SeastarService = PythonService, NativeService
    comparison.child = child
    if args.mode == "multiprocess":
        comparison.prefork(args, cpus)
    else:
        uvloop.run(comparison.worker(args, cpus, args.requests, args.bytes))
