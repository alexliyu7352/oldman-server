"""Same-machine throughput with wrk on the web-frameworks-benchmark routes:
oldman-server's layouts, a bare function on the native host (the server's own
ceiling, as the site's bare WSGI/ASGI entries), and stock Sanic, on the same
server cores. A reference beside the paired gate (probes/sanic_comparison,
whose client checks every response but tops out above 200k RPS on 3 cores).

  run.py --out DIR [--connections 64,256,512] [--rounds 2] [--path /] [--quiet]
         [--services candidate,candidate-shards,bare,bare-shards,stock] [--close]
         (each candidate and bare service also as NAME-io_uring; the bare
         function serves / only, the framework apps also /static/small.bin
         and /static/large.bin; --close: a new connection for every request)

Each window: warm-up, then wrk; it records RPS, non-2xx responses, the
service's own CPU (its process tree) per request, other processes' load on the
server CPUs, and the client's cores. Rounds alternate the order of services.
wrk: $WRK, or the development environment's build."""
import argparse
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / "scripts"))
from probes.sanic_boundary.run import layout_for
from probes.sanic_comparison.server import verify_stock
from probes.sanic_spike.run import free_port
from scripts.cpu_budget import describe_cpu

WRK = Path(os.environ.get("WRK", "/opt/tools/wrk/wrk"))
SERVE = Path(__file__).with_name("serve.py")
# name: serve.py arguments after the port, environment. Four server CPUs: the
# candidate as planned for 4 workers (same-thread, one CPU each), and 2 workers
# of 1 Python thread + 1 shard; the bare function in both layouts; stock, 4 workers.
# The -io_uring variants (not run by default) take the io_uring backend.
SERVICES = {
    "candidate": (["candidate", "4"], {}),
    "candidate-shards": (["candidate", "2"], {"OLDMAN_SERVER_NATIVE_SHARDS": "1"}),
    "bare": (["bare", "4", "1"], {}),
    "bare-shards": (["bare", "2", "0"], {}),
    "stock": (["stock", "4"], {}),
}
DEFAULT_SERVICES = list(SERVICES)
for _name in ("candidate", "candidate-shards", "bare", "bare-shards"):
    _arguments, _extra = SERVICES[_name]
    SERVICES[f"{_name}-io_uring"] = (_arguments, {**_extra, "OLDMAN_SERVER_NATIVE_REACTOR_BACKEND": "io_uring"})


def busy(cpus):
    """Busy and total ticks of each of `cpus`."""
    rows = {}
    for line in Path("/proc/stat").read_text().splitlines():
        name, *values = line.split()
        if name[:3] == "cpu" and name[3:].isdigit() and int(name[3:]) in cpus:
            values = list(map(int, values))
            rows[int(name[3:])] = (sum(values) - values[3] - values[4], sum(values))
    return rows


def cores(before, after):
    return sum((after[c][0] - before[c][0]) / max(1, after[c][1] - before[c][1]) for c in before)


def tree_ticks(root):
    """CPU ticks of a process, its children and theirs (every thread)."""
    total, pending = 0, [root]
    while pending:
        pid = pending.pop()
        try:
            fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
            total += int(fields[11]) + int(fields[12])
            for task in Path(f"/proc/{pid}/task").iterdir():
                pending += [int(c) for c in (task / "children").read_text().split()]
        except OSError:
            pass
    return total


def wait_quiet(limit=1800):
    """Until the whole machine has been under two busy cores for 30 s (at most
    `limit` s): a desktop's browser alone often keeps one busy."""
    deadline, quiet = time.monotonic() + limit, 0
    everything = set(range(os.cpu_count()))
    while quiet < 3 and time.monotonic() < deadline:
        before = busy(everything); time.sleep(10)
        quiet = quiet + 1 if cores(before, busy(everything)) < 2 else 0


def wrk(port, path, connections, seconds, client, close=False):
    command = ["taskset", "-c", ",".join(map(str, client)), str(WRK), f"-t{len(client)}",
               f"-c{connections}", f"-d{seconds}s", "--latency", *(["-H", "Connection: close"] if close else []),
               f"http://127.0.0.1:{port}{path}"]
    out = subprocess.run(command, capture_output=True, text=True, check=True).stdout
    found = lambda pattern: re.search(pattern, out)
    non2xx = found(r"Non-2xx or 3xx responses: (\d+)")
    errors = found(r"Socket errors: connect (\d+), read (\d+), write (\d+), timeout (\d+)")
    return {"rps": float(found(r"Requests/sec:\s+([\d.]+)").group(1)),
            "non2xx": int(non2xx.group(1)) if non2xx else 0,
            "socket_errors": sum(map(int, errors.groups())) if errors else 0,
            # The latency distribution's lines (a standard deviation column
            # elsewhere can end in 50% too).
            "p50": found(r"(?m)^\s+50%\s+(\S+)").group(1), "p99": found(r"(?m)^\s+99%\s+(\S+)").group(1)}


def window(name, layout, connections, path, warmup, seconds, close=False):
    arguments, extra = SERVICES[name]
    port = free_port()
    env = {**os.environ, **extra}
    if name == "stock":
        env["PYTHONPATH"] = str(verify_stock())
    server = ",".join(map(str, layout["server"]))
    process = subprocess.Popen(["taskset", "-c", server, sys.executable, str(SERVE), arguments[0], str(port),
                                *arguments[1:]], env=env, cwd=ROOT, stdout=subprocess.DEVNULL,
                               stderr=subprocess.PIPE, start_new_session=True)
    try:
        for _ in range(200):
            try:
                loop = urllib.request.urlopen(f"http://127.0.0.1:{port}/loop", timeout=1).read().decode()
                break
            except OSError:
                if process.poll() is not None:
                    raise RuntimeError(f"{name} exited: {process.stderr.read().decode()[-3000:]}")
                time.sleep(0.2)
        else:
            raise RuntimeError(f"{name} did not start")
        if loop != "uvloop":
            raise RuntimeError(f"{name} runs on {loop}, not uvloop")
        wrk(port, path, connections, warmup, layout["client"], close)
        server_cpus, client_cpus = set(layout["server"]), set(layout["client"])
        before, client_before = busy(server_cpus), busy(client_cpus)
        own_before, started = tree_ticks(process.pid), time.monotonic()
        result = wrk(port, path, connections, seconds, layout["client"], close)
        elapsed = time.monotonic() - started
        own = (tree_ticks(process.pid) - own_before) / os.sysconf("SC_CLK_TCK") / elapsed
        server_busy = cores(before, busy(server_cpus))
        return {"service": name, "connections": connections, "path": path, "close": close, "loop": loop, **result,
                "own_cores": round(own, 3), "foreign_server_cores": round(max(0.0, server_busy - own), 3),
                "client_cores": round(cores(client_before, busy(client_cpus)), 3),
                "cpu_us": round(own / result["rps"] * 1e6, 2)}
    finally:
        try:
            os.killpg(process.pid, signal.SIGINT)
            process.wait(20)
        except ProcessLookupError:
            pass  # it ended already
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--connections", default="64,256,512")
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--path", default="/")
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument("--seconds", type=int, default=10)
    parser.add_argument("--services", default=",".join(DEFAULT_SERVICES))
    parser.add_argument("--quiet", action="store_true", help="wait for a quiet machine before each level")
    parser.add_argument("--close", action="store_true", help="a new connection for every request")
    args = parser.parse_args()
    services = args.services.split(",")
    if not set(services) <= SERVICES.keys():
        raise SystemExit(f"services: {', '.join(SERVICES)}")
    if not WRK.exists():
        raise SystemExit(f"wrk not found: {WRK} (set WRK)")
    args.out.mkdir(parents=True, exist_ok=False)
    layout, _ = layout_for(describe_cpu(), 4)
    os.sched_setaffinity(0, set(layout["controller"]))
    rows = []
    with open(args.out / "rows.jsonl", "w") as log:
        for connections in map(int, args.connections.split(",")):
            if args.quiet:
                wait_quiet()
            for round_ in range(args.rounds):
                for name in services if round_ % 2 == 0 else services[::-1]:
                    row = {"round": round_, **window(name, layout, connections, args.path,
                                                     args.warmup, args.seconds, args.close)}
                    rows.append(row)
                    log.write(json.dumps(row) + "\n"); log.flush()
                    print(json.dumps(row), flush=True)
    summary = {}
    for row in rows:
        entry = summary.setdefault(f"{row['connections']}:{row['service']}", {"rps": [], "cpu_us": []})
        entry["rps"].append(row["rps"]); entry["cpu_us"].append(row["cpu_us"])
    summary = {key: {"rps": round(sum(v["rps"]) / len(v["rps"])), "cpu_us": round(sum(v["cpu_us"]) / len(v["cpu_us"]), 2)}
               for key, v in summary.items()}
    (args.out / "summary.json").write_text(json.dumps({"layout": layout, "services": summary}, indent=1) + "\n")
    for key, value in summary.items():
        print(key, value)


if __name__ == "__main__":
    main()
