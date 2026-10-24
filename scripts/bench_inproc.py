"""In-process Python main-thread cost per request: real Engine and framework,
stub native runtime.

No sockets, native shards or native drain/resolve work run here, so absolute
values are lower than a served window. Use it to compare framework changes
before and after; the phase gate uses probes/sanic_comparison/run.py.
"""
import argparse
import asyncio
from collections import deque
import json
import os
import statistics
import sys
import time
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import uvloop

from oldman_server.native import _native as _seastar_probe
from oldman_server.native import engine as probe_engine
from oldman_server.native.engine import ApplicationRequest, Config, ServerConfig
from probes.sanic_boundary.application import EXPECTED, INPUT

WIRE = {
    "small": ("GET", b"/small", b""),
    "body": ("POST", b"/body", INPUT),
}


def wire_request(workload):
    method, target, body = WIRE[workload]
    headers = ((b"Host", b"127.0.0.1"), (b"Content-Type", b"application/octet-stream"),
               (b"Content-Length", str(len(body)).encode()), (b"Connection", b"keep-alive"))
    return lambda: ApplicationRequest(method, target, headers, body, http_version="1.1",
                                      client=("127.0.0.1", 40000), server=("127.0.0.1", 8000))


class StubRuntime:
    """Closed loop of `concurrency` requests; every checked response is verified."""

    def __init__(self, workload):
        self._fd = os.eventfd(0, os.EFD_NONBLOCK | os.EFD_CLOEXEC)
        self._make = wire_request(workload)
        self._expected = EXPECTED[workload]
        self._pending = deque()
        self._state = 1
        self._total = self._issued = self._completed = 0
        self._done = None
        self._signal()  # Engine startup waits for the first drain to see state 1.

    def _signal(self):
        os.eventfd_write(self._fd, 1)

    def _issue(self):
        self._pending.append((self._issued, 0, self._make()))
        self._issued += 1

    def begin(self, total, concurrency, done):
        self._total, self._issued, self._completed, self._done = total, 0, 0, done
        for _ in range(min(concurrency, total)):
            self._issue()
        self._signal()

    def complete(self, count):
        self._completed += count
        while self._issued < self._total and self._issued - self._completed < 128:
            self._issue()
        if self._pending:
            self._signal()
        if self._completed >= self._total and self._done is not None and not self._done.done():
            self._done.set_result(None)

    # Methods used by Engine.
    def fd(self):
        return self._fd

    def fallback(self):
        return ""

    def clear_notification(self):
        try:
            os.eventfd_read(self._fd)
        except BlockingIOError:
            pass

    def drain_requests_owned(self, count, connections=None):
        batch = []
        while self._pending and len(batch) < count:
            batch.append(self._pending.popleft())
        if self._pending:
            self._signal()
        return batch

    def resolve_http_responses_checked(self, batch, max_bytes, bytes_headers=False,
                                       no_body_statuses=False):
        for _, _, status, body, _ in batch:
            if status != 200 or body != self._expected:
                raise AssertionError(f"unexpected response {status} {body[:60]!r}")
        self.complete(len(batch))
        return 0

    def stats(self):
        return {"state": self._state}

    def server_stats(self):
        return {}

    def shards(self):
        return []

    def stop(self):
        self._state = 3
        self._signal()

    # The persistent Engine starts and stops only the network server.
    def start_server(self, server):
        return 1

    def stop_server(self, generation=0):
        pass

    def join(self):
        pass

    def close_fd(self):
        os.close(self._fd)


def server_config():
    # Same bounds as probes/sanic_comparison/server.py:native_config.
    return ServerConfig(http_port=1, application_dispatch_all=True, max_requests=1024,
                        max_bytes=64 * 1024 * 1024, max_body_bytes=65536, max_response_bytes=65536,
                        decision_timeout_ms=5000, generate_date_header=True,
                        coalesce_notifications=True)


def count_middleware(app, counts):
    # Identical to the candidate accounting middleware in the comparison server.
    @app.on_request
    async def entered(request):
        counts["started"] += 1
        counts["active"] += 1

    @app.on_response
    async def finished(request, response):
        counts["finished"] += 1
        counts["active"] -= 1
        counts["responses"] += 1
        counts["errors"] += int(response.status != 200)


async def measure(args, sampler=None):
    stub = StubRuntime(args.workload)
    # Only the runtime is replaced; the config type stays the real binding.
    binding = {name: getattr(_seastar_probe, name) for name in dir(_seastar_probe)}
    binding["Runtime"] = lambda *unused: stub
    probe_engine._native = SimpleNamespace(**binding)
    counts = dict(started=0, finished=0, responses=0, errors=0, active=0)
    config = Config(shard_cpus=(sorted(os.sched_getaffinity(0))[0],), server=server_config())
    from probes.sanic_boundary.application import make_app
    from oldman_server.native.host import serve
    app = make_app()
    count_middleware(app, counts)
    host = serve(app, config)
    await host.__aenter__()
    loop = asyncio.get_running_loop()
    rows = []
    try:
        for index in range(args.repeats + 1):
            done = loop.create_future()
            started = counts["started"]
            cpu, wall = time.thread_time_ns(), time.perf_counter_ns()
            if sampler is not None and index:
                sampler.start()
            stub.begin(args.requests, args.concurrency, done)
            await done
            if sampler is not None:
                sampler.stop()
            cpu, wall = time.thread_time_ns() - cpu, time.perf_counter_ns() - wall
            if counts["started"] - started != args.requests or counts["errors"]:
                raise AssertionError(f"request accounting mismatch: {counts}")
            if index:  # The first pass warms caches, the router LRU and allocators.
                rows.append({"cpu_us": cpu / args.requests / 1000,
                             "wall_us": wall / args.requests / 1000})
    finally:
        await host.__aexit__(None, None, None)
    return rows


class Sampler:
    """SIGPROF stack sampling of the main thread (perf is unavailable to users here).

    Time spent in C code is charged to the Python function that called it.
    """

    def __init__(self, interval):
        self.interval = interval
        self.own, self.total, self.samples = {}, {}, 0
        self.keys = {}  # code object -> label; keeps the handler itself cheap

    def _record(self, signum, frame):
        self.samples += 1
        seen = set()
        top = True
        keys = self.keys
        while frame is not None:
            code = frame.f_code
            key = keys.get(code)
            if key is None:
                key = keys[code] = (f"{code.co_qualname} "
                                    f"({code.co_filename.rsplit('/', 1)[-1]}:{code.co_firstlineno})")
            if top:
                self.own[key] = self.own.get(key, 0) + 1
                top = False
            if key not in seen:
                seen.add(key)
                self.total[key] = self.total.get(key, 0) + 1
            frame = frame.f_back

    def start(self):
        import signal
        signal.signal(signal.SIGPROF, self._record)
        signal.setitimer(signal.ITIMER_PROF, self.interval, self.interval)

    def stop(self):
        import signal
        signal.setitimer(signal.ITIMER_PROF, 0, 0)

    def report(self, us_per_request, limit):
        scale = us_per_request / max(self.samples, 1)
        for title, table in (("self", self.own), ("inclusive", self.total)):
            print(f"--- {title} µs/request (samples={self.samples})")
            for key, count in sorted(table.items(), key=lambda item: -item[1])[:limit]:
                print(f"{count * scale:7.2f}  {key}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("sanic",), default="sanic")
    parser.add_argument("--workload", choices=tuple(WIRE), default="small")
    parser.add_argument("--requests", type=int, default=100_000)
    parser.add_argument("--concurrency", type=int, default=128)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--sample", type=float, metavar="SECONDS",
                        help="SIGPROF sampling interval; adds overhead, diagnosis only")
    parser.add_argument("--top", type=int, default=40)
    args = parser.parse_args()
    sampler = Sampler(args.sample) if args.sample else None
    rows = uvloop.run(measure(args, sampler))
    cpu = [row["cpu_us"] for row in rows]
    print(json.dumps({"mode": args.mode, "workload": args.workload, "requests": args.requests,
                      "cpu_us_median": round(statistics.median(cpu), 3),
                      "cpu_us_min": round(min(cpu), 3), "cpu_us_max": round(max(cpu), 3),
                      "wall_us_median": round(statistics.median(r["wall_us"] for r in rows), 3),
                      "sampled": bool(sampler)}))
    if sampler is not None:
        sampler.report(statistics.mean(cpu), args.top)


if __name__ == "__main__":
    main()
