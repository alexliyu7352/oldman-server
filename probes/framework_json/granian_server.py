"""Single-process Granian reference using the existing measurement protocol.

Real asyncio Tasks, a fresh dict and stdlib JSON encoding on every request.
Streaming APIs preserve the chunked framing used by the Seastar comparison.
"""
import argparse
import asyncio
import importlib.metadata
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import comparison_server as comparison
import uvloop
from granian.constants import HTTPModes, Interfaces, TaskImpl
from granian.server.embed import Server


class GranianService(comparison.PythonService):
    async def start(self, port, listener=None):
        self.counts.update(python_application_started=0, python_application_finished=0,
                           python_application_errors=0, responses=0)
        self.interface = self.mode.removeprefix("granian-")
        self.server = Server(
            self.asgi if self.interface == "asgi" else self.rsgi,
            address="127.0.0.1", port=port,
            interface=Interfaces.ASGINL if self.interface == "asgi" else Interfaces.RSGI,
            runtime_threads=2, runtime_blocking_threads=1, task_impl=TaskImpl.asyncio,
            http=HTTPModes.http1, websockets=False, backlog=1024, backpressure=1024,
            log_enabled=False, log_access=False,
        )
        # Newly created native threads inherit CPUs 1,2; the Python loop then
        # moves to CPU 0, matching the candidate's one-Python/two-native budget.
        os.sched_setaffinity(0, set(self.cpus[1:]))
        self.serving = asyncio.create_task(self.server.serve())
        deadline = asyncio.get_running_loop().time() + 10
        while True:
            if self.serving.done():
                await self.serving
                raise RuntimeError("Granian stopped before readiness")
            try:
                _reader, writer = await asyncio.open_connection("127.0.0.1", port)
                writer.close()
                await writer.wait_closed()
                break
            except OSError:
                if asyncio.get_running_loop().time() >= deadline:
                    raise TimeoutError("Granian did not listen")
                await asyncio.sleep(.01)
        os.sched_setaffinity(0, {self.cpus[0]})

    def begin_request(self):
        if not self.budget.acquire(256):
            self.counts["rejected"] += 1
            return None
        task = asyncio.current_task()
        self.tasks.add(task)
        self.counts["python_application_started"] += 1
        return task

    def finish_request(self, task):
        self.counts["python_application_finished"] += 1
        self.tasks.discard(task)
        self.budget.release(256)

    def headers(self):
        return [("content-type", "application/json"),
                ("x-probe-pid", str(os.getpid())),
                ("x-probe-shard", str(self.cpus[0]))]

    async def asgi(self, scope, receive, send):
        if scope["path"] != "/json" or scope["method"] != "GET":
            await send({"type": "http.response.start", "status": 404, "headers": []})
            await send({"type": "http.response.body", "body": b"Not Found"})
            return
        task = self.begin_request()
        if task is None:
            await send({"type": "http.response.start", "status": 503, "headers": []})
            await send({"type": "http.response.body", "body": b""})
            return
        try:
            body = json.dumps({"message": "Hello, World!"}, separators=(",", ":")).encode("utf-8")
            await send({"type": "http.response.start", "status": 200,
                        "headers": [(k.encode("ascii"), v.encode("ascii")) for k, v in self.headers()]})
            await send({"type": "http.response.body", "body": body, "more_body": True})
            await send({"type": "http.response.body", "body": b"", "more_body": False})
            self.counts["responses"] += 1
        except BaseException:
            self.counts["python_application_errors"] += 1
            raise
        finally:
            self.finish_request(task)

    async def rsgi(self, scope, proto):
        if scope.path != "/json" or scope.method != "GET":
            proto.response_bytes(404, [], b"Not Found")
            return
        task = self.begin_request()
        if task is None:
            proto.response_empty(503, [])
            return
        try:
            body = json.dumps({"message": "Hello, World!"}, separators=(",", ":")).encode("utf-8")
            stream = proto.response_stream(200, self.headers())
            await stream.send_bytes(body)
            del stream
            self.counts["responses"] += 1
        except BaseException:
            self.counts["python_application_errors"] += 1
            raise
        finally:
            self.finish_request(task)

    def stats(self):
        return super().stats() | {"python_application_active": len(self.tasks)}

    def info(self):
        return {"pid": os.getpid(), "main_cpu": self.cpus[0],
                "request_limit": self.budget.requests, "byte_limit": self.budget.byte_limit,
                "event_loop": str(type(asyncio.get_running_loop())),
                "granian": importlib.metadata.version("granian"), "uvloop": uvloop.__version__,
                "interface": self.interface, "task_impl": "asyncio", "runtime_threads": 2,
                "runtime_cpus": self.cpus[1:], "embedded": True, "workers": 1}

    async def close(self):
        self.server.interrupt_signal = True
        self.server.main_loop_interrupt.set()
        await asyncio.wait_for(self.serving, 10)
        if self.tasks:
            await asyncio.wait_for(asyncio.gather(*self.tasks), 10)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("granian-asgi", "granian-rsgi"), required=True)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    cpus = sorted(os.sched_getaffinity(0))
    if len(cpus) != 3:
        raise ValueError("Granian server requires the same three-CPU budget")
    comparison.PythonService = GranianService
    uvloop.run(comparison.worker(args, cpus, 1024, 64*1024*1024))


if __name__ == "__main__":
    main()
