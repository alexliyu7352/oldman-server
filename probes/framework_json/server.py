"""One fresh JSON object and stdlib serialization per request in each Python mode."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "python"))
import comparison_server as comparison
from comparison_server import web
import uvloop
import seastar_http
from seastar_http import App

# The Engine module seastar_http runs on: this tree's, or a frozen snapshot's
# that probes/application_lifecycle preloaded under its old name.
_engine_module = sys.modules[seastar_http.Engine.__module__]
Config, ServerConfig = _engine_module.Config, _engine_module.ServerConfig


class PythonService(comparison.PythonService):
    json_codec = "stdlib"

    async def start(self, port, listener=None):
        os.sched_setaffinity(0, {self.cpus[0]})
        self.counts.update(python_application_started=0, python_application_finished=0,
                           python_application_errors=0, responses=0)
        app = web.Application()
        app.router.add_get("/json", self.handle)
        self.runner = web.AppRunner(app, access_log=None, shutdown_timeout=.2,
                                    handler_cancellation=False, keepalive_timeout=120)
        await self.runner.setup()
        await web.TCPSite(self.runner, "127.0.0.1", port, reuse_port=True).start()

    async def handle(self, request):
        if not self.budget.acquire(256):
            self.counts["rejected"] += 1
            return web.Response(status=503)
        task = asyncio.current_task()
        self.tasks.add(task)
        self.counts["python_application_started"] += 1
        try:
            if self.json_codec == "native":
                body = comparison.native.encode_json({"message": "Hello, World!"})
            else:
                body = json.dumps({"message": "Hello, World!"}, separators=(",", ":")).encode("utf-8")
            response = web.Response(body=body, headers={
                "Content-Type": "application/json",
                "X-Probe-Pid": str(os.getpid()),
                "X-Probe-Shard": str(self.cpus[0])})
            response.enable_chunked_encoding()
            await response.prepare(request)
            await response.write_eof()
            self.counts["responses"] += 1
            return response
        except BaseException:
            self.counts["python_application_errors"] += 1
            raise
        finally:
            self.counts["python_application_finished"] += 1
            self.tasks.discard(task)
            self.budget.release(256)

    def stats(self):
        return super().stats() | {"python_application_active": len(self.tasks)}

    def info(self):
        return super().info() | {"event_loop": str(type(asyncio.get_running_loop())),
                                 "uvloop": uvloop.__version__, "json_codec": self.json_codec}


class CandidateService(comparison.SeastarService):
    native_responses = False

    async def start(self, port, listener=None):
        reactors = self.cpus if self.mode.endswith("candidate1") else self.cpus[1:]
        # Do not pass the new keyword to a frozen pre-experiment App.
        app = App(native_responses=True) if self.native_responses else App()

        @app.get("/json")
        async def json_handler(_request):
            return {"message": "Hello, World!"}

        self.engine = await app.start(Config(shard_cpus=tuple(reactors),
            server=ServerConfig(http_port=port, max_requests=self.requests, max_bytes=self.byte_limit,
                max_body_bytes=65536, max_response_bytes=65536, decision_timeout_ms=5000,
                generate_date_header=True, coalesce_notifications=True)))
        os.sched_setaffinity(0, {self.cpus[0]})

    def info(self):
        return super().info() | {"event_loop": str(type(asyncio.get_running_loop())),
                                 "uvloop": uvloop.__version__,
                                 "native_responses": self.native_responses,
                                 "reactor_cpus": self.cpus if self.mode.endswith("candidate1") else self.cpus[1:]}


def child(args, cpu, requests, byte_limit, connection, inherited):
    for pipe in inherited:
        pipe.close()
    uvloop.run(comparison.worker(args, [cpu], requests, byte_limit, connection))


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("candidate1", "candidate3", "multiprocess"), required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--native-responses", action="store_true",
                        help="opt in to the native-response experiment")
    parser.add_argument("--json-codec", choices=("stdlib", "native"), default="stdlib",
                        help="per-request serializer for the multiprocess reference")
    args = parser.parse_args(argv)
    if args.native_responses and args.mode == "multiprocess":
        parser.error("--native-responses requires a candidate mode")
    if args.json_codec != "stdlib" and args.mode != "multiprocess":
        parser.error("--json-codec only applies to multiprocess")
    CandidateService.native_responses = args.native_responses
    PythonService.json_codec = args.json_codec
    args.requests, args.bytes = 1024, 64*1024*1024
    cpus = sorted(os.sched_getaffinity(0))
    if len(cpus) != (1 if args.mode == "candidate1" else 3):
        raise ValueError("server process must be pinned to the mode's CPU budget")
    comparison.PythonService = PythonService
    comparison.SeastarService = CandidateService
    comparison.child = child
    # comparison_server.worker treats only seastar-prefixed modes as Engine services.
    # Expose its existing branch without changing the shared helper.
    if args.mode == "multiprocess":
        comparison.prefork(args, cpus)
    else:
        args.mode = "seastar-" + args.mode
        uvloop.run(comparison.worker(args, cpus, args.requests, args.bytes))


if __name__ == "__main__":
    main()
