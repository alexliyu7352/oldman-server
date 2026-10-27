"""The reverse-proxy probe's application, served as a deployment serves it:
`app.run(workers=N)` on the native host, behind OpenResty (see run.py).

Workers import this module afresh (spawn) and find the app by name, so the app
is built at import and configured from the environment: OLDMAN_SERVER_NATIVE_*
by the framework itself, PROBE_OUT here."""
import argparse
import asyncio
import json
import multiprocessing
import os
from pathlib import Path
import time

from oldman_server import OldmanServer
from oldman_server.response import json as json_response

EXPECTED = b'{"message":"Hello, World!"}'
# /work spends this much CPU in the handler, so the workers (not OpenResty or
# the client) limit the throughput, as heavier real handlers do.
WORK_SECONDS = 25e-6
SLEEP_SECONDS = 0.001
# Each worker imports this module itself, so this is that worker's pid.
PID_HEADER = {"X-Probe-Pid": str(os.getpid())}

app = OldmanServer("nginx_proxy", configure_logging=False)
app.config.ACCESS_LOG = False


@app.get("/small")
async def small(request):
    return json_response({"message": "Hello, World!"}, headers=PID_HEADER)


@app.get("/work")
async def work(request):
    end = time.perf_counter() + WORK_SECONDS
    while time.perf_counter() < end:
        pass
    return json_response({"message": "Hello, World!"}, headers=PID_HEADER)


@app.get("/sleep")
async def sleep(request):
    await asyncio.sleep(SLEEP_SECONDS)
    return json_response({"message": "Hello, World!"}, headers=PID_HEADER)


@app.after_server_start
async def report(app):
    # run.py waits for one file per worker and checks responses against them.
    out = Path(os.environ["PROBE_OUT"])
    record = {"pid": os.getpid(), "name": multiprocessing.current_process().name}
    (out / f"worker-{os.getpid()}.json").write_text(json.dumps(record))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int)
    parser.add_argument("--unix")
    parser.add_argument("--workers", type=int, required=True)
    args = parser.parse_args()
    if not app.config.NATIVE_SERVER:
        raise SystemExit("set OLDMAN_SERVER_NATIVE_SERVER=true")
    address = {"unix": args.unix} if args.unix else {"host": "127.0.0.1", "port": args.port}
    app.run(**address, workers=args.workers, access_log=False)
