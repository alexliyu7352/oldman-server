"""Bounded Sanic entry cost study. Every service lifecycle gets a new process."""
import argparse
import hashlib
import json
import math
import os
import re
from pathlib import Path
import socket
import subprocess
import sys
import sysconfig
import time
import traceback

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
from probes.framework_json.run import atomic_json, cpu_layout, read_response
from probes.application_lifecycle.inputs import runtime_fingerprint
from probes.sanic_boundary.application import EXPECTED, INPUT, concurrency
from probes.sanic_spike.run import free_port
from probes.final.run import threads
from scripts.cpu_budget import (describe_cpu, cpu_topology, require_budget,
                                cpu_snapshot, require_unthrottled)
from run_comparison import native_libraries, stop_process
from run_http_benchmarks import read_line, read_json, send

ZERO = ("active_requests", "reserved_bytes", "pending_application_nodes",
        "python_application_active", "pending_decisions", "queued_decisions")


def fingerprints():
    paths = [ROOT / "pyproject.toml", ROOT / "uv.lock", ROOT / ".python-version",
             ROOT / "dependency-lock.json", ROOT / "scripts/env.sh"]
    for directory in ("python", "native", "scripts", "probes/sanic_boundary",
                      "probes/sanic_spike", "probes/framework_json", "probes/final",
                      "probes/application_lifecycle", "src/oldman_server"):
        paths.extend(p for p in (ROOT / directory).rglob("*")
                     if p.suffix in (".py", ".cc", ".hh", ".json", ".toml"))
    return {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(set(paths))} | native_libraries()


def layout_for(budget, server_cores=3):
    """Server, client (three) and controller CPUs, one per physical core:
    the server takes the first server_cores (3, or 4 for the multi-worker
    comparison), the client the next three, the controller the eighth."""
    require_budget(budget, 8)
    if server_cores not in (3, 4):
        raise ValueError("eight physical cores fit three or four server cores")
    topology = cpu_topology(budget["effective_cpus"])
    layout = cpu_layout(budget["effective_cpus"], topology)
    active = [*layout["candidate3"], *layout["client"], layout["controller"]]
    return {"server": active[:server_cores], "client": active[server_cores:server_cores + 3],
            "controller": [active[7]]}, topology


def check_threads(pid, cpus):
    rows = threads([pid])
    if not rows:
        raise ValueError("missing process threads")
    for tid in rows:
        if not os.sched_getaffinity(int(tid)) <= set(cpus):
            raise ValueError(f"thread {tid} escaped CPU budget")
    return rows


PACKAGE = ROOT / "src/oldman_server"


def package_version():
    return re.search(r'__version__ = "([^"]+)"', (PACKAGE / "__version__.py").read_text()).group(1)


def verify_modules(evidence):
    """The service loaded this tree's package, native host and extension."""
    paths = {"oldman_server": PACKAGE / "__init__.py",
             "oldman_server.native.engine": PACKAGE / "native/engine.py",
             "oldman_server.native.host": PACKAGE / "native/host.py",
             "oldman_server.native._native": PACKAGE / ("native/_native" + sysconfig.get_config_var("EXT_SUFFIX"))}
    expected = {name: {"path": str(path.resolve()), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
                for name, path in paths.items()}
    if evidence != {"modules": expected, "version": package_version()}:
        raise ValueError("loaded modules do not match local source/binary")


def preflight(port, pid, workload):
    body = INPUT if workload == "body" else b""
    method = "POST" if body else "GET"
    with socket.create_connection(("127.0.0.1", port), timeout=5) as conn:
        conn.sendall((f"{method} /{workload} HTTP/1.1\r\nHost: 127.0.0.1\r\n"
                      f"Content-Type: application/octet-stream\r\nContent-Length: {len(body)}\r\n"
                      "Connection: keep-alive\r\n\r\n").encode() + body)
        with conn.makefile("rb") as stream:
            status, headers, actual = read_response(stream)
    if status != 200 or actual != EXPECTED[workload] or headers.get("x-probe-pid") != str(pid):
        raise ValueError(f"preflight body/status/PID mismatch: {status}, {actual!r}")
    content_type = "application/octet-stream" if workload == "body" else "application/json"
    if (headers.get("content-type") != content_type or
            headers.get("transfer-encoding") != "chunked" or "content-length" in headers or
            headers.get("content-encoding") or headers.get("connection", "").lower() == "close"):
        raise ValueError(f"preflight response semantics mismatch: {headers}")
    return {"status": status, "headers": headers, "body_hex": actual.hex()}


def validate_row(row):
    c, s = row["client"], row["server"]
    n = c["requests"]
    for key in ("seconds", "arrival_seconds", "rate", "rps", "p99_us", "cpu_seconds"):
        if not isinstance(c[key], (int, float)) or not math.isfinite(c[key]) or c[key] < 0:
            raise ValueError(f"invalid client {key}")
    if (c["arrival_seconds"] != row["seconds"] or c["rate"] != row["rate"] or
            not row["seconds"] <= c["seconds"] <= row["seconds"] + 10):
        raise ValueError("measurement duration/rate mismatch")
    if row["rate"] and c["scheduled"] != math.ceil(row["rate"] * row["seconds"] - 1e-8):
        raise ValueError("offered load mismatch")
    if row["returncodes"] != [0, 0] or n <= 0:
        raise ValueError("process failed or empty window")
    if any(c[k] != 0 for k in ("errors", "status_errors", "integrity_errors", "dropped", "new_connections")):
        raise ValueError("load error or cold connection in measurement")
    if (c["scheduled"] != n or c["total_connections"] != row["concurrency"] or
            c["server_pids"] != {str(row["ready"]["pid"]): n} or
            c["response_bytes"] != n * len(EXPECTED[row["workload"]])):
        raise ValueError("request/connection/body count mismatch")
    if any(s["delta"][k] != n for k in ("responses", "python_application_started", "python_application_finished")):
        raise ValueError("response/Task count mismatch")
    if s["delta"]["python_application_errors"] or not row["closed"]["closed"]:
        raise ValueError("application errors or failed close")
    if any(stats[k] for stats in (s["after"], row["closed"]["stats"]) for k in ZERO):
        raise ValueError("resources did not drain")
    if not 0 < s["python_thread_cpu_ns"] <= s["process_cpu_ns"]:
        raise ValueError("invalid CPU clocks")
    evidence, diagnostic = s["diagnostics"], row["diagnostic"]
    if diagnostic == "none" and evidence:
        raise ValueError("instrumented data in unprofiled window")
    if diagnostic == "cprofile" and not evidence.get("calls"):
        raise ValueError("missing call profile")
    if diagnostic == "tracemalloc" and not 0 <= evidence["traced_current_bytes"] <= evidence["traced_peak_bytes"]:
        raise ValueError("invalid traced live-memory sample")


def execute(out, mode, workload, diagnostic, *, budget, layout, seconds, warmup, concurrency, rate):
    out.mkdir(parents=True, exist_ok=False)
    (out / "expected.bin").write_bytes(EXPECTED[workload])
    (out / "input.bin").write_bytes(INPUT)
    port = free_port()
    row = {"mode": mode, "workload": workload, "diagnostic": diagnostic,
           "seconds": seconds, "warmup": warmup, "concurrency": concurrency, "rate": rate,
           "layout": layout, "validated": False, "commands": []}
    processes, logs = [], []

    def start(command, role):
        command = ["taskset", "-c", ",".join(map(str, layout[role]))] + command
        row["commands"].append(command)
        log = (out / f"{role}.log").open("wb")
        logs.append(log)
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=log, bufsize=0, start_new_session=True, env=os.environ | {"UV_USE_IO_URING": "0"})
        processes.append(process)
        return process

    try:
        script = str(Path(__file__).with_name("server.py"))
        server = start([sys.executable, script, "--mode", mode, "--port", str(port),
            "--diagnostic", diagnostic, "--out", str(out)], "server")
        row["ready"] = read_json(server)
        ready = row["ready"]
        verify_modules(ready["loaded"])
        if (ready["pid"] != server.pid or ready["runtime"] != runtime_fingerprint()["state"] or
                "uvloop.Loop" not in ready["loop"] or
                [s["affinity"] for s in ready["shards"]] != [[cpu] for cpu in layout["server"][1:]]):
            raise ValueError("service runtime/shard mismatch")
        row["preflight"] = preflight(port, server.pid, workload)
        cpus = ",".join(map(str, layout["client"]))
        client = start([str(ROOT / "build/http_load"), "--smp", "3", "--cpuset", cpus,
            "--thread-affinity", "1", "--reactor-backend", "epoll", "--overprovisioned",
            "--default-log-level", "warn", "--enable-glibc-exception-scaling-workaround", "0",
            "--port", str(port), "--target-pids", str(server.pid), "--path", "/" + workload,
            "--method", "POST" if workload == "body" else "GET", "--input", str(out / "input.bin"),
            "--expected", str(out / "expected.bin"), "--concurrency", str(concurrency),
            "--rate", str(rate), "--warmup", str(warmup), "--seconds", str(seconds)], "client")
        while read_line(client) != "MEASURE_READY":
            pass
        row["threads_before"] = {role: check_threads(proc.pid, layout[role])
            for role, proc in zip(("server", "client"), processes, strict=True)}
        row["cpu_before"] = cpu_snapshot()
        if any(budget[k] != row["cpu_before"][k] for k in ("cpu", "cpuset")):
            raise ValueError("CPU budget changed")
        send(server, "begin")
        if not read_json(server)["begun"]:
            raise ValueError("begin refused")
        send(client, "start")
        while read_line(client, timeout=seconds+20) != "MEASURE_DONE":
            pass
        send(server, "end")
        row["server"] = read_json(server)
        row["client"] = read_json(client)
        row["cpu_after"] = cpu_snapshot()
        row["threads_after"] = check_threads(server.pid, layout["server"])
        send(server, "quit")
        row["closed"] = read_json(server)
        row["returncodes"] = [p.wait(10) for p in (server, client)]
        atomic_json(out / "row.json", row)
        require_unthrottled(row["cpu_before"], row["cpu_after"])
        validate_row(row)
        row["validated"] = True
        atomic_json(out / "row.json", row)
        return row
    except BaseException:
        row["failure"] = traceback.format_exc()
        atomic_json(out / "row.json", row)
        raise
    finally:
        for process in reversed(processes):
            stop_process(process)
        for log in logs:
            log.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    budget = describe_cpu()
    layout, topology = layout_for(budget)
    source = fingerprints()
    runtime = runtime_fingerprint()
    manifest = {"source": source, "runtime": runtime, "cpu_budget": budget,
        "topology": topology, "layout": layout, "smoke": args.smoke,
        "git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "created_unix_ns": time.time_ns(), "os_release": Path("/etc/os-release").read_text(),
        "kernel": os.uname().release, "loop_environment": {"UV_USE_IO_URING": "0"}}
    atomic_json(out / "manifest.json", manifest)
    old_affinity = os.sched_getaffinity(0)
    os.sched_setaffinity(0, set(layout["controller"]))
    try:
        for workload in EXPECTED:
            for repeat in range(1 if args.smoke else 4):
                for mode in (("asgi", "native") if repeat % 2 == 0 else ("native", "asgi")):
                    key = f"{workload}-r{repeat}-{mode}"
                    row = execute(out / key, mode, workload, "none", budget=budget, layout=layout,
                        seconds=.2 if args.smoke else 5, warmup=.1 if args.smoke else 2,
                        concurrency=concurrency(workload), rate=0)
                    print(json.dumps({"key": key, "rps": row["client"]["rps"]}), flush=True)
        for workload in EXPECTED:
            for diagnostic in ("cprofile", "tracemalloc"):
                for mode in ("asgi", "native"):
                    key = f"{workload}-{diagnostic}-{mode}"
                    execute(out / key, mode, workload, diagnostic, budget=budget, layout=layout,
                        seconds=.2 if args.smoke else 2, warmup=.1 if args.smoke else 1,
                        concurrency=32, rate=1000)
                    print(json.dumps({"key": key, "diagnostic": diagnostic}), flush=True)
        if source != fingerprints() or runtime != runtime_fingerprint():
            raise ValueError("source/binary/runtime changed during study")
        atomic_json(out / "complete.json", {"fingerprints_unchanged": True,
            "windows": 18 if args.smoke else 36})
    finally:
        os.sched_setaffinity(0, old_affinity)


if __name__ == "__main__":
    main()
