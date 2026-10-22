"""Same native compression behind aiohttp direct/pool/prefork or Seastar.

This is a benchmark adapter, not a general application framework. Only the
controller pipe carries IPC in prefork mode; HTTP bodies never cross it.
"""
import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import multiprocessing as mp
import os
import socket
import sys
import threading

import aiohttp
from aiohttp import web
import aiohttp.http_parser
from oldman_server.native import _native as native
from oldman_server.native.engine import Config, Engine, ServerConfig
from comparison_metrics import Snapshot


def compress_bound(n):
    # zlib compressBound() formula for the linked default zlib build.
    return n + (n >> 12) + (n >> 14) + (n >> 25) + 13


class Budget:
    def __init__(self, requests, byte_limit):
        self.requests, self.byte_limit = requests, byte_limit
        self.active = self.reserved = 0
        self.high_active = self.high_bytes = 0

    def acquire(self, amount):
        if self.active >= self.requests or amount > self.byte_limit-self.reserved:
            return False
        self.active += 1
        self.reserved += amount
        self.high_active = max(self.high_active, self.active)
        self.high_bytes = max(self.high_bytes, self.reserved)
        return True

    def release(self, amount):
        self.active -= 1
        self.reserved -= amount
        assert self.active >= 0 and self.reserved >= 0


class PythonService:
    def __init__(self, mode, cpus, requests, byte_limit):
        self.mode, self.cpus = mode, cpus
        self.budget = Budget(requests, byte_limit)
        self.pool = None
        self.runner = None
        self.compute_workers = []
        self.tasks = set()
        self.counts = dict(responses=0, rejected=0, failed=0, python_decisions=0,
            native_calls=0, input_copy_bytes=0, output_copy_bytes=0, compute_cpu_ns=0,
            body_bytes_to_python=0, body_bytes_from_python=0, queued_work=0,
            queued_work_peak=0)

    async def start(self, port, listener=None):
        os.sched_setaffinity(0, {self.cpus[0]})
        if self.mode == "pool":
            cpus = iter(self.cpus[1:])
            lock = threading.Lock()
            def initialize():
                with lock:
                    cpu = next(cpus)
                os.sched_setaffinity(0, {cpu})
                self.compute_workers.append({"tid": threading.get_native_id(), "affinity": [cpu]})
            self.pool = ThreadPoolExecutor(len(self.cpus)-1, initializer=initialize, thread_name_prefix="native-pool")
            barrier = threading.Barrier(len(self.cpus)-1)
            loop = asyncio.get_running_loop()
            await asyncio.gather(*(loop.run_in_executor(self.pool, barrier.wait) for _ in self.cpus[1:]))
        app = web.Application(client_max_size=1048576)
        app.router.add_post("/hybrid/compress", self.handle)
        self.runner = web.AppRunner(app, access_log=None, shutdown_timeout=.2,
                                    handler_cancellation=False, keepalive_timeout=120)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", port, reuse_port=self.mode == "multiprocess")
        await site.start()

    async def work(self, body):
        if self.pool is None:
            return native.native_compress(body, 6)
        self.counts["queued_work"] += 1
        self.counts["queued_work_peak"] = max(self.counts["queued_work_peak"], self.counts["queued_work"])
        future = asyncio.get_running_loop().run_in_executor(self.pool, native.native_compress, body, 6)
        try:
            # A handler may be cancelled on shutdown. Its reservation stays
            # live until the OS worker actually stops touching its body.
            try:
                return await asyncio.shield(future)
            except asyncio.CancelledError:
                await future
                raise
        finally:
            self.counts["queued_work"] -= 1

    async def handle(self, request):
        n = request.content_length
        if n is None or request.headers.get("Transfer-Encoding"):
            return web.Response(status=411)
        if n > 1048576:
            response = web.Response(status=413); response.force_close(); return response
        amount = n + compress_bound(n)
        if not self.budget.acquire(amount):
            self.counts["rejected"] += 1
            response = web.Response(status=503); response.force_close(); return response
        task = asyncio.current_task()
        self.tasks.add(task)
        try:
            body = await request.content.readexactly(n)
            self.counts["body_bytes_to_python"] += len(body)
            # Same short allow decision as the Seastar metadata callback.
            if request.method != "POST" or request.path != "/hybrid/compress":
                return web.Response(status=403)
            self.counts["python_decisions"] += 1
            result, metrics = await self.work(body)
            self.counts["native_calls"] += 1
            self.counts["compute_cpu_ns"] += metrics["compute_cpu_ns"]
            self.counts["input_copy_bytes"] += len(body)
            self.counts["output_copy_bytes"] += len(result)
            self.counts["body_bytes_from_python"] += len(result)
            response = web.StreamResponse(status=200, headers={"Content-Type": "application/octet-stream",
                "X-Probe-Pid": str(os.getpid()), "X-Probe-Shard": str(self.cpus[0]),
                "X-Probe-Worker": str(metrics["work_tid"])})
            response.enable_chunked_encoding()
            await response.prepare(request)
            await response.write(result)
            await response.write_eof()
            self.counts["responses"] += 1
            return response
        except asyncio.IncompleteReadError:
            self.counts["failed"] += 1
            return web.Response(status=400)
        except BaseException:
            self.counts["failed"] += 1
            raise
        finally:
            self.tasks.discard(task)
            self.budget.release(amount)

    def stats(self):
        return {**self.counts, "active_requests": self.budget.active, "reserved_bytes": self.budget.reserved,
                "active_peak": self.budget.high_active, "bytes_peak": self.budget.high_bytes}

    def info(self):
        return {"pid": os.getpid(), "main_cpu": self.cpus[0], "request_limit": self.budget.requests,
                "byte_limit": self.budget.byte_limit, "compute_workers": self.compute_workers,
                "aiohttp": aiohttp.__version__, "http_parser": str(aiohttp.http_parser.HttpRequestParser)}

    async def close(self):
        if self.runner:
            await self.runner.cleanup()
        if self.tasks:
            await asyncio.gather(*list(self.tasks), return_exceptions=True)
        if self.pool:
            self.pool.shutdown(wait=True)


class SeastarService:
    def __init__(self, mode, cpus, requests, byte_limit):
        self.mode, self.cpus, self.requests, self.byte_limit = mode, cpus, requests, byte_limit

    async def start(self, port, listener=None):
        shared = self.mode == "seastar-shared"
        def decision(req):
            return req.method == "POST" and req.route == "compress"
        self.engine = await Engine.open(Config(shard_cpus=(self.cpus[0 if shared else 1],),
            worker_cpus=tuple(self.cpus[1 if shared else 2:]),
            server=ServerConfig(http_port=port, max_requests=self.requests, max_bytes=self.byte_limit,
                                decision_timeout_ms=5000)), decision_handler=decision)
        os.sched_setaffinity(0, {self.cpus[0]})

    def stats(self):
        return self.engine.server_stats()

    def info(self):
        return {"pid": os.getpid(), "main_cpu": self.cpus[0], "request_limit": self.requests,
                "byte_limit": self.byte_limit, "shards": self.engine.shards(), "compute_workers": self.engine.workers()}

    async def close(self):
        await self.engine.aclose()


async def worker(args, cpus, requests, byte_limit, control=None, listener=None):
    service_cls = SeastarService if args.mode.startswith("seastar") else PythonService
    service = service_cls(args.mode, cpus, requests, byte_limit)
    await service.start(args.port, listener)
    snapshot = Snapshot(service.stats)
    loop = asyncio.get_running_loop()
    transport = None
    if control is None:
        reader = asyncio.StreamReader()
        transport, _ = await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), sys.stdin)
        def emit(value): print(json.dumps(value), flush=True)
        async def command(): return (await reader.readline()).decode().strip()
        emit({"ready": True, "pids": [os.getpid()], "service_pids": [os.getpid()],
              "workers": [service.info()], "server_cpus": cpus, "mode": args.mode})
    else:
        queue = asyncio.Queue()
        def receive():
            try: queue.put_nowait(control.recv())
            except EOFError: queue.put_nowait("quit"); loop.remove_reader(control.fileno())
        loop.add_reader(control.fileno(), receive)
        emit = control.send
        command = queue.get
        emit(service.info())
    try:
        while cmd := await command():
            if cmd == "begin":
                snapshot.begin(); emit({"begun": True})
            elif cmd == "end":
                emit({"workers": [snapshot.end()]})
            elif cmd == "quit":
                break
            else:
                raise ValueError(cmd)
    finally:
        await snapshot.close()
        await service.close()
        emit({"closed": True, "workers": [service.stats()]})
        if transport: transport.close()
        if control:
            loop.remove_reader(control.fileno()); control.close()


def child(args, cpu, requests, byte_limit, connection, inherited):
    # Each child closes all parent pipe endpoints inherited from earlier forks.
    for pipe in inherited: pipe.close()
    asyncio.run(worker(args, [cpu], requests, byte_limit, connection))


def finish_workers(children, timeout=5):
    """Reap every owned worker; a close snapshot is not proof of a clean exit."""
    outcomes = []
    for process in children:
        process.join(timeout)
        forced = process.is_alive()
        if forced:
            process.terminate(); process.join(3)
        if process.is_alive():
            process.kill(); process.join()
        outcomes.append({"pid": process.pid, "exitcode": process.exitcode, "forced": forced})
    return outcomes


def prefork(args, cpus):
    """One child per entry of cpus: a CPU, or a list of CPUs for a child that
    serves through the native host (its Python CPU first)."""
    ctx = mp.get_context("fork")
    children, pipes = [], []
    try:
        for i, cpu in enumerate(cpus):
            parent, connection = ctx.Pipe()
            process = ctx.Process(target=child, args=(args, cpu,
                args.requests//len(cpus)+(i < args.requests % len(cpus)),
                args.bytes//len(cpus)+(i < args.bytes % len(cpus)), connection, [*pipes, parent]))
            process.start(); connection.close(); children.append(process); pipes.append(parent)
        os.sched_setaffinity(0, {cpus[0] if isinstance(cpus[0], int) else cpus[0][0]})
        infos = [p.recv() for p in pipes]
        print(json.dumps({"ready": True, "pids": [p.pid for p in children],
            "service_pids": [os.getpid(), *(p.pid for p in children)], "workers": infos,
            "server_cpus": cpus, "mode": args.mode}), flush=True)
        for line in sys.stdin:
            cmd = line.strip()
            for p in pipes: p.send(cmd)
            replies = [p.recv() for p in pipes]
            result = {"begun": True} if cmd == "begin" else {"workers": [w for r in replies for w in r["workers"]]}
            if cmd == "quit":
                result["worker_exits"] = finish_workers(children)
                result["closed"] = all(row["exitcode"] == 0 and not row["forced"]
                                       for row in result["worker_exits"])
            print(json.dumps(result), flush=True)
            if cmd == "quit":
                if not result["closed"]:
                    raise RuntimeError("benchmark worker did not exit cleanly")
                break
    finally:
        finish_workers(children, timeout=1)
        for p in pipes: p.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["direct", "pool", "multiprocess", "seastar", "seastar-shared"], required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--requests", type=int, default=96)
    parser.add_argument("--bytes", type=int, default=256*1048576)
    args = parser.parse_args()
    cpus = sorted(os.sched_getaffinity(0))
    assert len(cpus) == 6 and args.requests >= 6 and args.bytes >= 6
    if args.mode == "multiprocess": prefork(args, cpus)
    else: asyncio.run(worker(args, cpus, args.requests, args.bytes))
