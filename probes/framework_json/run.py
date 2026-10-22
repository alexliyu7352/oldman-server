"""Matched local TechEmpower-style JSON comparison; runs only when explicitly invoked."""
import argparse
from email.utils import parsedate_to_datetime
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import secrets
import select
import socket
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
# The tree the "before" mode serves: a copy of the project before the native
# boundary change, placed here by hand (not part of the repository).
BEFORE_TREE = ROOT / "build/native-boundary-before"
sys.path.insert(0, str(ROOT))
from scripts.cpu_budget import (describe_cpu, require_budget, cpu_snapshot,
                                cpu_delta, require_unthrottled, cpu_topology)

EXPECTED = b'{"message":"Hello, World!"}'
MODES = ("candidate1", "candidate3", "multiprocess", "native3")
REFERENCE_MODES = ("granian-asgi", "granian-rsgi")
BEFORE_MODE = "candidate-before"
ZERO_COUNTERS = ("active_requests", "reserved_bytes", "pending_decisions",
                 "queued_decisions", "python_application_active")


def json_body():
    """Serialize a fresh object with the same stdlib settings used by App and aiohttp."""
    return json.dumps({"message": "Hello, World!"}, separators=(",", ":")).encode("utf-8")


def cpu_layout(available, topology):
    cpus = sorted(available)
    if len(cpus) < 9:
        raise ValueError("need nine affinity CPUs: three server, four client, one unused, one controller")
    by_cpu = {row["cpu"]: row for row in topology}
    if not set(cpus) <= by_cpu.keys():
        raise ValueError("missing CPU topology")
    by_core = {}
    for cpu in cpus:
        row = by_cpu[cpu]
        by_core.setdefault((row["package_id"], row["core_id"]), cpu)
    primary = list(by_core.values())
    if len(primary) < 8:
        raise ValueError("need eight physical cores: three server, four client, one controller")
    active = primary[:8]
    return {"candidate1": active[:1], "candidate3": active[:3],
            "multiprocess": active[:3], "native3": active[:3],
            **{mode: active[:3] for mode in REFERENCE_MODES},
            BEFORE_MODE: active[:3],
            "client": active[3:7], "unused": next(cpu for cpu in cpus if cpu not in active),
            "controller": active[7]}


def choose_port():
    for _ in range(400):
        number = 1000 + secrets.randbelow(9000)
        with socket.socket() as sock:
            try:
                sock.bind(("127.0.0.1", number))
            except OSError:
                continue
            return number
    raise RuntimeError("no available four-digit loopback port")


def read_response(stream):
    line = stream.readline()
    if not line.startswith(b"HTTP/1.1 ") or not line.endswith(b"\r\n"):
        raise ValueError(f"invalid HTTP status line: {line!r}")
    status = int(line.split(b" ", 2)[1])
    headers = {}
    while (line := stream.readline()) != b"\r\n":
        if not line or b":" not in line or not line.endswith(b"\r\n"):
            raise ValueError("truncated or invalid HTTP headers")
        key, value = line[:-2].split(b":", 1)
        name = key.decode("ascii").lower()
        if name in headers:
            raise ValueError(f"duplicate header {name}")
        headers[name] = value.strip().decode("latin-1")
    if headers.get("transfer-encoding", "").lower() == "chunked":
        parts = []
        while True:
            size_line = stream.readline()
            try:
                length = int(size_line.split(b";", 1)[0], 16)
            except ValueError as exc:
                raise ValueError("invalid chunk size") from exc
            if length == 0:
                while (trailer := stream.readline()) != b"\r\n":
                    if not trailer:
                        raise ValueError("truncated HTTP trailers")
                break
            if length > 65536:
                raise ValueError("oversized response chunk")
            data = stream.read(length)
            if len(data) != length or stream.read(2) != b"\r\n":
                raise ValueError("truncated chunk")
            parts.append(data)
        body = b"".join(parts)
    else:
        length = int(headers.get("content-length", "0"))
        body = stream.read(length)
        if len(body) != length:
            raise ValueError("truncated HTTP response")
    return status, headers, body


def validate_preflight_response(status, headers, body, pids, *, expected):
    if expected:
        if status != 200 or body != EXPECTED:
            raise ValueError(f"JSON response status/body mismatch: {status}, {body!r}")
        if headers.get("transfer-encoding", "").lower() != "chunked" or "content-length" in headers:
            raise ValueError("JSON response must use chunked framing")
        if headers.get("content-type", "").split(";", 1)[0].lower() != "application/json":
            raise ValueError("JSON response lacks application/json type")
        if headers.get("content-encoding"):
            raise ValueError("JSON response must not be compressed")
        if not headers.get("server"):
            raise ValueError("missing Server header")
        try:
            if int(headers["x-probe-pid"]) not in pids:
                raise ValueError("unknown response PID")
            int(headers["x-probe-shard"])
        except (KeyError, TypeError) as exc:
            raise ValueError("missing PID/shard diagnostic header") from exc
    elif status != 404:
        raise ValueError(f"unknown route returned {status}, expected 404")
    try:
        date = parsedate_to_datetime(headers["date"])
        if date.utcoffset() is None or abs(date.timestamp() - time.time()) > 30:
            raise ValueError("stale or timezone-free Date")
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise ValueError("missing or invalid Date header") from exc


def preflight(port, pids):
    with socket.create_connection(("127.0.0.1", port), timeout=5) as conn:
        conn.settimeout(5)
        with conn.makefile("rb") as stream:
            for path, expected in (("/json", True), ("/json", True), ("/missing", False)):
                conn.sendall(f"GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\n"
                             "Connection: keep-alive\r\nAccept-Encoding: identity\r\n\r\n".encode())
                status, headers, body = read_response(stream)
                validate_preflight_response(status, headers, body, pids, expected=expected)
                if headers.get("connection", "").lower() == "close":
                    raise ValueError("keepalive unexpectedly closed")


def total(stats, field, section="delta"):
    values = [w[section] for w in stats["workers"]]
    if any(field not in v for v in values):
        raise ValueError(f"missing {field} in {section}")
    return sum(v[field] for v in values)


def validate_measurement(row, concurrency):
    budget = row.get("cpu_budget", {})
    require_budget(budget, 8)
    window = row.get("cpu_window")
    if not window or "before" not in window or "after" not in window:
        raise ValueError("missing CPU measurement window")
    if any(budget[key] != window["before"][key] for key in ("cpu", "cpuset")):
        raise ValueError("CPU budget changed since preflight")
    require_unthrottled(window["before"], window["after"])
    c = row["client"]
    if row["returncodes"] != [0, 0]:
        raise ValueError("server/client process failed")
    for field in ("errors", "integrity_errors", "status_errors", "dropped"):
        if c[field] != 0:
            raise ValueError(f"nonzero client {field}: {c[field]}")
    requests = c["requests"]
    if (requests <= 0 or requests != c["scheduled"] or
            requests != sum(c["server_pids"].values()) or
            requests != total(row["server"], "responses")):
        raise ValueError("response/PID/request count mismatch")
    if c["response_bytes"] != len(EXPECTED)*requests:
        raise ValueError("response byte count mismatch")
    if c["new_connections"] != 0 or c["total_connections"] != concurrency:
        raise ValueError("warm persistent connection count mismatch")
    if not c["rps"] > 0 or not c["p99_us"] > 0:
        raise ValueError("missing throughput/latency")
    native = row["mode"] == "native3"
    if total(row["server"], "python_application_started") != (0 if native else requests):
        raise ValueError("python_application_started mismatch")
    if total(row["server"], "python_application_finished") != (0 if native else requests):
        raise ValueError("python_application_finished mismatch")
    if total(row["server"], "python_application_errors") != 0:
        raise ValueError("python_application_errors nonzero")
    for worker in row["server"]["workers"]:
        if worker["after"].get("python_application_active", 0):
            raise ValueError("python_application_active after measurement")
    closed = row["closed"]
    if not closed["closed"] or any(worker.get(field, 0) != 0
                                   for worker in closed["workers"] for field in ZERO_COUNTERS):
        raise ValueError("server failed to close cleanly")


def atomic_json(path, value):
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temp.replace(path)


def fingerprints():
    paths = [ROOT / "build/http_load", ROOT / "build/framework_native",
             *sorted((ROOT / "build").glob("_seastar_probe*.so")),
             *sorted((ROOT / "probes/framework_json").glob("*.py")),
             ROOT / "probes/framework_json/native_main.cc",
             *sorted((ROOT / "native").glob("*.cc")), *sorted((ROOT / "native").glob("*.hh")),
             ROOT / "python/seastar_http.py", ROOT / "src/oldman_server/native/engine.py",
             ROOT / "scripts/comparison_server.py", ROOT / "scripts/comparison_metrics.py",
             ROOT / "scripts/cpu_budget.py",
             ROOT / "probes/final/run.py",
             ROOT / "scripts/run_http_benchmarks.py", ROOT / "scripts/run_comparison.py",
             ROOT / "scripts/benchmark_work.py", ROOT / "scripts/http_server.py"]
    before = BEFORE_TREE
    if before.exists():
        paths.extend(p for p in sorted(before.rglob("*"))
                     if p.is_file() and p.suffix in (".py", ".so", ".cc", ".hh"))
    return {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}


def manifest_for(layout, cpu_budget, topology):
    from probes.final.run import native_libraries
    import granian
    granian_root = Path(granian.__file__).parent
    return {"created": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "python": sys.version, "kernel": platform.release(),
        "parent_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "cpu_budget": cpu_budget, "cpu_topology": topology,
        "cpu_model": next((s.split(":", 1)[1].strip() for s in Path("/proc/cpuinfo").read_text().splitlines()
                           if s.startswith("model name")), "unknown"),
        "versions": {p: importlib.metadata.version(p) for p in ("aiohttp", "uvloop", "granian")},
        "granian_fingerprints": {str(p.relative_to(granian_root)): hashlib.sha256(p.read_bytes()).hexdigest()
                                 for p in sorted(granian_root.rglob("*"))
                                 if p.suffix in (".py", ".so")},
        "fingerprints": fingerprints(), "native_libraries": native_libraries(),
        "cpu_layout": layout, "payload": EXPECTED.decode(), "payload_bytes": len(EXPECTED),
        "protocol": "HTTP/1.1 GET /json; fresh stdlib serializer; chunked; persistent; no gzip",
        "claim": "local TFB-style JSON comparison, not an official TFB run"}


def one(args, mode, out, layout, *, cpu_budget, make_server_command=None,
        client_binary=None, client_environment=None, on_ready=None, sample_processes=None,
        observe_processes=None):
    # Existing helpers import the native extension; leave pure contract checks importable before build.
    from probes.final.run import (read_line, read_json, send, process_sample, stop_process, threads)
    sample_processes = sample_processes or process_sample
    require_budget(cpu_budget, 8)
    key = f"json-{mode}-r{args.round}"
    path = out / f"{key}.json"
    if path.exists():
        raise ValueError(f"refuse to overwrite measurement {path}")
    cpus = layout[mode]
    client_cpus = layout["client"]
    reactor_cpus = cpus if mode in ("candidate1", "native3") else cpus[1:]
    port = choose_port()
    aff = ",".join(map(str, cpus))
    server_command = (["taskset", "-c", aff, str(ROOT / "build/framework_native"), str(port), aff]
        if mode == "native3" else
        ["taskset", "-c", aff, sys.executable,
         str(ROOT / "probes/framework_json" / ("granian_server.py" if mode in REFERENCE_MODES else "server.py")),
         "--mode", mode, "--port", str(port)])
    if mode == BEFORE_MODE:
        before = BEFORE_TREE
        server_command = ["taskset", "-c", aff, "env",
            "PYTHONPATH=" + str(before / "python") + ":" + str(before / "build"),
            sys.executable, str(before / "probes/framework_json/server.py"),
            "--mode", "candidate3", "--port", str(port)]
    if make_server_command is not None:
        server_command = make_server_command(port, cpus)
    client_command = ["taskset", "-c", ",".join(map(str, client_cpus)),
        str(client_binary or ROOT / "build/http_load"), "--smp", "4", "--cpuset", ",".join(map(str, client_cpus)),
        "--thread-affinity", "1", "--reactor-backend", "epoll", "--overprovisioned",
        "--default-log-level", "warn", "--enable-glibc-exception-scaling-workaround", "0",
        "--port", str(port), "--target-pids", "", "--path", "/json", "--method", "GET",
        "--expected", str(out / "expected.bin"), "--concurrency", str(args.connections),
        "--rate", "0", "--warmup", str(args.warmup), "--seconds", str(args.seconds)]
    server = client = None
    host_samples, host_window = [], {}
    try:
        with (out / f"{key}.server.log").open("wb") as serr, (out / f"{key}.client.log").open("wb") as cerr:
            server = subprocess.Popen(server_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                      stderr=serr, bufsize=0, start_new_session=True)
            ready = read_json(server)
            if not ready["ready"] or len(ready["pids"]) != (3 if mode == "multiprocess" else 1):
                raise ValueError("invalid server readiness/PID count")
            if mode != "native3" and not all("uvloop.Loop" in w["event_loop"] for w in ready["workers"]):
                raise ValueError("Python event loop is not uvloop")
            if mode.startswith("candidate"):
                observed = [s["affinity"][0] for s in ready["workers"][0]["shards"]]
                if observed != reactor_cpus:
                    raise ValueError(f"incorrect reactor pinning {observed} != {reactor_cpus}")
            if mode == "native3":
                observed = [s["cpu"] for s in ready["workers"][0]["shards"]]
                if observed != reactor_cpus:
                    raise ValueError("incorrect native reactor pinning")
            preflight(port, ready["pids"])
            client_command[client_command.index("--target-pids")+1] = ",".join(map(str, ready["pids"]))
            client = subprocess.Popen(client_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                      stderr=cerr, bufsize=0, start_new_session=True, env=client_environment)
            while read_line(client) != "MEASURE_READY":
                pass
            if on_ready is not None:
                on_ready(server, client, ready)
            if observe_processes is not None:
                host_samples.append(observe_processes(ready["service_pids"]))
                host_window["begin_sent_ns"] = time.monotonic_ns()
            send(server, "begin")
            if not read_json(server)["begun"]:
                raise ValueError("server refused begin")
            before_threads = threads(ready["service_pids"])
            samples = [sample_processes(ready["service_pids"])]
            cg0 = cpu_snapshot()
            start = time.monotonic()
            send(client, "start")
            next_sample = start + 1
            while True:
                if select.select([client.stdout], [], [], max(0, min(.1, next_sample-time.monotonic())))[0]:
                    if read_line(client) == "MEASURE_DONE":
                        break
                now = time.monotonic()
                if now >= next_sample:
                    samples.append(sample_processes(ready["service_pids"]))
                    if observe_processes is not None:
                        host_samples.append(observe_processes(ready["service_pids"]))
                    next_sample = now + 1
                if now - start > args.seconds + 20:
                    raise TimeoutError("measurement exceeded bound")
            finish = time.monotonic()
            samples.append(sample_processes(ready["service_pids"]))
            after_threads = threads(ready["service_pids"])
            if mode in REFERENCE_MODES:
                for tid, value in after_threads.items():
                    expected_affinity = str(cpus[0]) if int(tid) == ready["pids"][0] else ",".join(map(str, cpus[1:]))
                    if value["affinity"] not in (expected_affinity, f"{cpus[1]}-{cpus[2]}" if int(tid) != ready["pids"][0] else expected_affinity):
                        raise ValueError(f"Granian thread {tid} affinity {value['affinity']} != {expected_affinity}")
            send(server, "end")
            server_stats = read_json(server)
            if observe_processes is not None:
                host_window["end_received_ns"] = time.monotonic_ns()
                host_samples.append(observe_processes(ready["service_pids"]))
            cg1 = cpu_snapshot()
            client_stats = read_json(client)
            send(server, "quit")
            closed = read_json(server)
            returncodes = [server.wait(10), client.wait(10)]
            old = {p["pid"]: p["cpu_ticks"] for p in samples[0]["processes"]}
            cpu_seconds = sum(p["cpu_ticks"]-old[p["pid"]] for p in samples[-1]["processes"])/os.sysconf("SC_CLK_TCK")
            for tid, value in after_threads.items():
                value["cpu_cores"] = (value["ticks"]-before_threads.get(tid, value)["ticks"])/os.sysconf("SC_CLK_TCK")/(finish-start)
            row = {"key": key, "mode": mode, "round": args.round,
                "commands": [server_command, client_command], "port": port, "ready": ready,
                "server": server_stats, "client": client_stats, "closed": closed, "returncodes": returncodes,
                "server_cpus": cpus, "reactor_cpus": reactor_cpus, "client_cpus": client_cpus,
                "controller_cpu": layout["controller"], "unused_cpu": layout["unused"],
                "warmup": args.warmup, "seconds": args.seconds, "concurrency": args.connections,
                "server_cpu_seconds": cpu_seconds, "server_cpu_cores": cpu_seconds/(finish-start),
                "server_cpu_per_request_us": cpu_seconds*1e6/client_stats["requests"] if client_stats["requests"] else None,
                "client_cpu_cores": client_stats["cpu_cores"], "threads": after_threads,
                "cpu_budget": cpu_budget, "cpu_window": {"before": cg0, "after": cg1},
                "memory_samples": samples, "validated": False}
            if observe_processes is not None:
                row.update(host_samples=host_samples, host_window=host_window)
            # Save raw evidence before rejecting resets, changed limits or throttling.
            atomic_json(path, row)
            row["cgroup_levels_delta"] = cpu_delta(cg0, cg1)
            leaf = cg0["cpu"]["levels"][0]["path"]
            row["cgroup_delta"] = row["cgroup_levels_delta"][leaf]
            atomic_json(path, row)
            validate_measurement(row, args.connections)
            row["validated"] = True
            atomic_json(path, row)
            print(json.dumps({"mode": mode, "round": args.round, "rps": client_stats["rps"],
                "p99_ms": client_stats["p99_us"]/1000, "server_cpu_cores": row["server_cpu_cores"],
                "client_cpu_cores": client_stats["cpu_cores"], "errors": client_stats["errors"]}), flush=True)
            return row
    finally:
        stop_process(client)
        stop_process(server)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    parser.add_argument("--round", type=int, default=0)
    parser.add_argument("--warmup", type=float, default=3)
    parser.add_argument("--seconds", type=float, default=10)
    parser.add_argument("--connections", type=int, default=256)
    parser.add_argument("--modes", nargs="+", choices=MODES + REFERENCE_MODES + (BEFORE_MODE,), default=MODES)
    args = parser.parse_args(argv)
    if args.round < 0 or args.warmup <= 0 or args.seconds <= 0 or args.connections < 4:
        parser.error("round must be nonnegative and duration/concurrency positive")
    # Capture affinity before restricting this controller to its single CPU.
    budget = describe_cpu()
    require_budget(budget, 8)
    topology = cpu_topology(budget["effective_cpus"])
    layout = cpu_layout(budget["effective_cpus"], topology)
    os.sched_setaffinity(0, {layout["controller"]})
    out = ROOT / args.out
    out.mkdir(parents=True, exist_ok=True)
    expected = out / "expected.bin"
    if expected.exists():
        if expected.read_bytes() != EXPECTED:
            raise ValueError("expected response body changed")
    else:
        expected.write_bytes(EXPECTED)
    manifest = out / "manifest.json"
    if not manifest.exists():
        atomic_json(manifest, manifest_for(layout, budget, topology))
    current = json.loads(manifest.read_text())
    if current["cpu_layout"] != layout or current["payload_bytes"] != len(EXPECTED):
        raise ValueError("host layout or workload changed since manifest")
    if current["fingerprints"] != fingerprints():
        raise ValueError("source/binary changed since manifest")
    if current.get("cpu_budget") != budget:
        raise ValueError("CPU budget changed since manifest")
    if current.get("cpu_topology") != topology:
        raise ValueError("CPU topology changed since manifest")
    modes = args.modes if args.round % 2 == 0 else list(reversed(args.modes))
    for mode in modes:
        one(args, mode, out, layout, cpu_budget=budget)


if __name__ == "__main__":
    main()
