"""Same-machine wrk runs of web-frameworks-benchmark's Python entries (the
apps and environments setup.sh makes) beside oldman-server, in two groups: bare
callables on a server, and frameworks with routing; and stock Sanic with
sanic-ext against oldman-server with its ext, both with oldman's five
extensions (ext_app.py). Server: 4 CPUs (taskset), 4 workers or processes
each (PYTHON_CPU_COUNT=4 where the app sizes itself by cpu_count); wrk on 3
other CPUs, as probes/wrk_comparison.

  drive.py OUT [--services a,b] [--routes root,user,post] [--connections 64,256] [--rounds 2]

Each window: the three routes' responses checked once, warm-up, wrk; RPS,
non-2xx, socket errors, the service tree's own CPU per request, other processes'
load on the server CPUs, client cores. Rounds alternate the service order;
before each (connections, route) level the machine must be quiet."""
import argparse
import json
import os
import re
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
import urllib.request

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / "scripts"))
from probes.sanic_boundary.run import layout_for
from probes.wrk_comparison.run import busy, cores, tree_ticks, wait_quiet, wrk, WRK
from scripts.cpu_budget import describe_cpu

SETUP = ROOT / "build/framework-comparison"  # setup.sh's apps/ and venvs/
APPS = Path(os.environ.get("APPS", SETUP / "apps"))
VENVS = Path(os.environ.get("VENVS", SETUP / "venvs"))
PORT = 3000  # the site's port; several apps hard-code it
OURS = str(ROOT / ".venv/bin/python")
UVICORN = ["-m", "uvicorn", "--log-level", "critical", "--host", "0.0.0.0", "--port", "3000",
           "--workers", "4", "server:app"]
WEFT = lambda protocol: ["-m", "weft", "--log-level", "error", "--protocol", protocol, "--host", "0.0.0.0",
                         "--port", "3000", "--workers", "4", "server:app"]
# name: (group, venv or None for ours, cwd, arguments after the interpreter, engine, extra environment)
SERVICES = {
    "oldman-server-bare": ("bare", None, HERE, [str(HERE / "bare.py"), "3000"], "oldman-server native host", {}),
    "fastpysgi-wsgi": ("bare", "fastpysgi-wsgi", "fastpysgi-wsgi",
                       ["server.py", "--host", "0.0.0.0", "--port", "3000", "--workers", "4"], "fastpysgi", {}),
    "weft-wsgi": ("bare", "weft", "weft-wsgi", WEFT("wsgi"), "weft", {}),
    "weft-asgi": ("bare", "weft", "weft-asgi", WEFT("asgi"), "weft", {}),
    "oldman-server": ("framework", None, ROOT, ["probes/wrk_comparison/serve.py", "candidate", "3000", "4"],
                      "oldman-server native host, 4 same-thread workers", {}),
    "wred": ("framework", "wred", "wred", WEFT("asgi"), "weft", {}),
    "oxapy": ("framework", "oxapy", "oxapy", ["server.py"], "oxapy", {}),
    "robyn": ("framework", "robyn", "robyn",
              ["server.py", "--workers=4", "--processes=4", "--log-level=CRITICAL"], "robyn", {}),
    "emmett": ("framework", "emmett", "emmett",
               ["-m", "emmett", "serve", "--host", "0.0.0.0", "--port", "3000", "--workers", "4"], "emmett (granian)", {}),
    "sanic": ("framework", "sanic", "sanic", ["server.py"], "sanic", {}),
    "blacksheep": ("framework", "blacksheep", "blacksheep", UVICORN, "uvicorn", {}),
    "starlette": ("framework", "starlette", "starlette", UVICORN, "uvicorn", {}),
    "litestar": ("framework", "litestar", "litestar", UVICORN, "uvicorn", {}),
    "fastapi": ("framework", "fastapi", "fastapi", UVICORN, "uvicorn", {}),
    "lihil": ("framework", "lihil", "lihil", UVICORN, "uvicorn", {}),
    "asgineer": ("framework", "asgineer", "asgineer", UVICORN, "uvicorn", {}),
    # oldman's five extensions on each side (ext_app.py), beside the plain sanic / oldman-server rows.
    "sanic-ext5": ("ext", "sanic-ext", HERE, [str(HERE / "ext_app.py"), "stock", "3000", "4"],
                   "sanic + sanic-ext, oldman's five extensions", {}),
    "oldman-server-ext5": ("ext", None, HERE, [str(HERE / "ext_app.py"), "candidate", "3000", "4"],
                           "oldman-server native host + ext, oldman's five extensions", {}),
}
# Services with a /loop route, checked to run on uvloop: ours, and both sides of the ext pair.
LOOP_ROUTE = {"oldman-server-bare", "oldman-server", "sanic-ext5", "oldman-server-ext5"}
ROUTES = {"root": ("GET", "/", b""), "user": ("GET", "/user/0", b"0"), "post": ("POST", "/user", b"")}
POST_LUA = HERE / "post.lua"


def request(method, path):
    data = b"" if method == "POST" else None
    with urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{PORT}{path}", data=data,
                                                       method=method), timeout=2) as response:
        return response.status, response.read()


def port_free():
    with socket.socket() as s:
        return s.connect_ex(("127.0.0.1", PORT)) != 0


def start(name, layout):
    group, venv, cwd, arguments, engine, extra = SERVICES[name]
    python = OURS if venv is None else str(VENVS / venv / "bin/python")
    cwd = cwd if isinstance(cwd, Path) else APPS / cwd
    env = {**os.environ, "PYTHON_CPU_COUNT": "4", **extra}
    if venv is not None:
        env = {k: v for k, v in env.items() if not k.startswith(("OLDMAN_SERVER_", "PYTHONPATH"))}
        env["PATH"] = f"{VENVS / venv / 'bin'}:{env['PATH']}"
    for _ in range(100):
        if port_free():
            break
        time.sleep(0.2)
    else:
        raise RuntimeError(f"port {PORT} still in use before {name}")
    process = subprocess.Popen(["taskset", "-c", ",".join(map(str, layout["server"])), python, *arguments],
                               env=env, cwd=cwd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                               start_new_session=True)
    try:
        for _ in range(300):
            try:
                for route, (method, path, body) in ROUTES.items():
                    status, got = request(method, path)
                    if not 200 <= status < 300 or got != body:  # litestar answers POST with 201
                        raise RuntimeError(f"{name} {method} {path}: {status} {got[:80]!r}")
                # Ours and the ext pair must run on uvloop (the project's rule for its
                # own comparisons); the others run as the site configures them.
                if name in LOOP_ROUTE and request("GET", "/loop")[1] != b"uvloop":
                    raise RuntimeError(f"{name} does not run on uvloop")
                return process
            except OSError:
                if process.poll() is not None:
                    raise RuntimeError(f"{name} exited: {process.stderr.read().decode()[-3000:]}")
                time.sleep(0.2)
        raise RuntimeError(f"{name} did not start")
    except BaseException:
        stop(process)
        raise


def stop(process):
    for sig, wait in ((signal.SIGINT, 15), (signal.SIGTERM, 10), (signal.SIGKILL, 10)):
        try:
            os.killpg(process.pid, sig)
            process.wait(wait)
            break
        except ProcessLookupError:
            break
        except subprocess.TimeoutExpired:
            continue
    try:  # children that left the group (multiprocessing daemons) end with it
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def window(name, layout, route, connections, warmup, seconds):
    method, path, _ = ROUTES[route]
    process = start(name, layout)
    try:
        script = ["-s", str(POST_LUA)] if method == "POST" else []
        run = lambda s: wrk_with(script, path, connections, s, layout["client"])
        run(warmup)
        server_cpus, client_cpus = set(layout["server"]), set(layout["client"])
        before, client_before = busy(server_cpus), busy(client_cpus)
        own_before, started = tree_ticks(process.pid), time.monotonic()
        result = run(seconds)
        elapsed = time.monotonic() - started
        own = (tree_ticks(process.pid) - own_before) / os.sysconf("SC_CLK_TCK") / elapsed
        server_busy = cores(before, busy(server_cpus))
        group, _, _, _, engine, _ = SERVICES[name]
        return {"service": name, "group": group, "engine": engine, "route": route, "connections": connections,
                **result, "own_cores": round(own, 3),
                "foreign_server_cores": round(max(0.0, server_busy - own), 3),
                "client_cores": round(cores(client_before, busy(client_cpus)), 3),
                "cpu_us": round(own / result["rps"] * 1e6, 2)}
    finally:
        stop(process)


def wrk_with(script, path, connections, seconds, client):
    if not script:
        return wrk(PORT, path, connections, seconds, client)
    # probes/wrk_comparison.run.wrk has no script option: the same command plus -s.
    command = ["taskset", "-c", ",".join(map(str, client)), str(WRK), f"-t{len(client)}", f"-c{connections}",
               f"-d{seconds}s", "--latency", *script, f"http://127.0.0.1:{PORT}{path}"]
    out = subprocess.run(command, capture_output=True, text=True, check=True).stdout
    found = lambda pattern: re.search(pattern, out)
    non2xx = found(r"Non-2xx or 3xx responses: (\d+)")
    errors = found(r"Socket errors: connect (\d+), read (\d+), write (\d+), timeout (\d+)")
    return {"rps": float(found(r"Requests/sec:\s+([\d.]+)").group(1)),
            "non2xx": int(non2xx.group(1)) if non2xx else 0,
            "socket_errors": sum(map(int, errors.groups())) if errors else 0,
            "p50": found(r"(?m)^\s+50%\s+(\S+)").group(1), "p99": found(r"(?m)^\s+99%\s+(\S+)").group(1)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("out", type=Path)
    parser.add_argument("--services", default=",".join(SERVICES))
    parser.add_argument("--routes", default=",".join(ROUTES))
    parser.add_argument("--connections", default="64,256")
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument("--seconds", type=int, default=10)
    parser.add_argument("--no-quiet", action="store_true")
    parser.add_argument("--quiet-limit", type=int, default=1800, help="longest wait for a quiet machine (s)")
    parser.add_argument("--check", action="store_true", help="start each service and check its routes only")
    args = parser.parse_args()
    services = args.services.split(",")
    if not set(services) <= SERVICES.keys():
        raise SystemExit(f"services: {', '.join(SERVICES)}")
    layout, _ = layout_for(describe_cpu(), 4)
    os.sched_setaffinity(0, set(layout["controller"]))
    if args.check:
        for name in services:
            stop(start(name, layout))
            print(name, "ok", flush=True)
        return
    args.out.mkdir(parents=True, exist_ok=False)
    with open(args.out / "rows.jsonl", "w") as log:
        for connections in map(int, args.connections.split(",")):
            for route in args.routes.split(","):
                if not args.no_quiet:
                    wait_quiet(args.quiet_limit)
                for round_ in range(args.rounds):
                    for name in services if round_ % 2 == 0 else services[::-1]:
                        try:
                            row = {"round": round_, **window(name, layout, route, connections,
                                                             args.warmup, args.seconds)}
                        except Exception as error:  # one service failing does not end the run
                            row = {"round": round_, "service": name, "route": route,
                                   "connections": connections, "error": str(error)[-2000:]}
                        log.write(json.dumps(row) + "\n"); log.flush()
                        print(json.dumps(row), flush=True)


if __name__ == "__main__":
    main()
