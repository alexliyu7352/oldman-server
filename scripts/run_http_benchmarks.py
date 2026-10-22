"""Serial, isolated server/client process pairs. No builds during timing."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import select
import socket
import subprocess
import sys
import time
import zlib

from benchmark_work import payload_for
from http_server import arithmetic

ROOT = Path(__file__).resolve().parents[1]


def validate_row(row, key):
    c, d = row["client"], row["server"]["delta"]
    if (row["key"] != key or row["returncodes"] != [0,0] or c["errors"] != 0 or c["requests"] <= 0
            or c["new_connections"] != 0 or c["total_connections"] != 32
            or c["requests"] != d["responses"]
            or d["python_decisions"] != (0 if row["mode"] == "native" else c["requests"])
            or d["body_bytes_to_python"] != (c["requests"]*row["size"] if row["mode"] == "copy" else 0)
            or d["body_bytes_from_python"] != d["body_bytes_to_python"]
            or not row["closed"]["closed"]
            or any(row["closed"]["stats"][k] for k in ("active_requests","reserved_bytes","pending_decisions","queued_decisions"))):
        raise ValueError(f"invalid measurement row: {key}")


def validate_manifest(old, new):
    for key in ("protocol", "warmup_seconds", "measure_seconds", "repeats", "concurrency",
                "server_cpus", "client_cpus", "payloads", "fingerprints", "cases"):
        if old.get(key) != new[key]:
            raise ValueError(f"resume provenance mismatch: {key}")


def read_line(proc, timeout=30):
    # Binary unbuffered pipes: select must observe every line, not Python's
    # text buffer. readline is safe after readiness; children flush records.
    if not select.select([proc.stdout], [], [], timeout)[0]:
        raise TimeoutError(f"child {proc.pid} did not report")
    line = proc.stdout.readline()
    if not line:
        raise RuntimeError(f"child {proc.pid} exited early: {proc.poll()}")
    return line.decode().strip()


def read_json(proc):
    while True:
        line = read_line(proc)
        if line.startswith("{"):
            return json.loads(line)


def send(proc, command):
    proc.stdin.write((command+"\n").encode())
    proc.stdin.flush()


def main(args):
    available = sorted(os.sched_getaffinity(0))
    assert len(available) >= 8
    server_cpus, client_cpus = available[:6], available[6:8]
    # Controller does negligible work during a run; share the Python CPU.
    os.sched_setaffinity(0, {available[0]})
    out = ROOT / "build" / ("http-smoke" if args.smoke else "http")
    out.mkdir(exist_ok=True)
    data = ROOT / "build/http-data"
    data.mkdir(exist_ok=True)
    hashes = {}
    for size in (1024, 65536, 1048576):
        payload = payload_for(size)
        for kind, body in (("input", payload), ("compress", zlib.compress(payload, 6)), ("fixed", b"x"*size)):
            path = data / f"{kind}-{size}.bin"
            path.write_bytes(body)
            hashes[path.name] = {"bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
    calibration = {}
    for n in (0, 60, 600, 3000, 30000, 120000):
        start = time.thread_time_ns()
        repetitions = max(20, min(2000, 600000//max(1,n)))
        for _ in range(repetitions):
            arithmetic(n)
        calibration[n] = (time.thread_time_ns()-start)/repetitions/1000
    provenance = [ROOT/"build/http_load", *list((ROOT/"src/oldman_server/native").glob("_native*.so")),
                  *list((ROOT/"native").glob("*.*")), ROOT/"src/oldman_server/native/engine.py",
                  ROOT/"scripts/http_server.py", ROOT/"scripts/run_http_benchmarks.py"]
    fingerprints = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in provenance}
    manifest = {"protocol":2, "cases":27, "fingerprints":fingerprints,
        "warmup_seconds": .1 if args.smoke else 2, "measure_seconds": .2 if args.smoke else 5,
        "repeats": 1 if args.smoke else 3, "concurrency": 32,
        "server_cpus": server_cpus, "client_cpus": client_cpus, "controller_cpu": available[0],
        "cpu_max": Path("/sys/fs/cgroup/cpu.max").read_text().strip(),
        "arithmetic_calibration_us": calibration, "payloads": hashes,
        "clock": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "storage": "warm kernel page cache; no physical-storage-limited claim"}
    manifest_path = out / "manifest.json"
    if manifest_path.exists():
        if not args.resume:
            raise ValueError("result directory already has a manifest; use --resume with identical source/build")
        original = json.loads(manifest_path.read_text())
        validate_manifest(original,manifest)
        manifest = original
    else:
        manifest_path.write_text(json.dumps(manifest, indent=2))
    cases = []
    for route, modes in (("fixed", ("native", "hybrid")), ("compress", ("native", "hybrid", "copy"))):
        for size in (1024, 65536, 1048576):
            for mode in modes:
                cases.append((mode, route, size, 0, 3))
    cases += [(mode, "file", 1048576, 0, 3) for mode in ("native", "hybrid")]
    cases += [("hybrid", route, size, burn, 3) for route, size in (("fixed",1024),("compress",1048576)) for burn in (60,600,3000)]
    cases += [("hybrid", "compress", 1048576, burn, 3) for burn in (30000,120000)]
    cases += [(mode, "compress", 1048576, 0, 1) for mode in ("native", "hybrid")]
    index=[]
    for repeat in range(manifest["repeats"]):
        # Alternate order across repeats to reduce systematic thermal/order bias.
        ordered = cases if repeat % 2 == 0 else list(reversed(cases))
        for mode, route, size, burn, workers in ordered:
            key = f"{mode}-{route}-{size}-b{burn}-w{workers}-r{repeat}"
            if args.resume and (out / (key+".json")).exists():
                validate_row(json.loads((out/(key+".json")).read_text()),key)
                index.append(key)
                (out / "index.json").write_text(json.dumps(index, indent=2))
                continue
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0)); port = sock.getsockname()[1]
            server = client = None
            try:
                with (out / (key+".server.log")).open("wb") as serr, (out / (key+".client.log")).open("wb") as cerr:
                    server_cmd = ["taskset", "-c", ",".join(map(str,server_cpus)), sys.executable,
                        str(ROOT/"scripts/http_server.py"), "--port", str(port), "--file", str(data/"input-1048576.bin"),
                        "--workers", str(workers), "--burn", str(burn)]
                    server = subprocess.Popen(server_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=serr, bufsize=0)
                    ready = read_json(server)
                    assert ready["ready"]
                    path = f"/{mode}/{route}" + (f"?size={size}" if route == "fixed" else "")
                    expected = data / (f"input-{size}.bin" if route == "file" else f"{route}-{size}.bin")
                    client_cmd = ["taskset", "-c", ",".join(map(str,client_cpus)), str(ROOT/"build/http_load"),
                        "--smp", "2", "--cpuset", ",".join(map(str,client_cpus)), "--thread-affinity", "1",
                        "--reactor-backend", "epoll", "--overprovisioned", "--default-log-level", "warn",
                        "--enable-glibc-exception-scaling-workaround", "0", "--port", str(port),
                        "--target-pid", str(ready["pid"]), "--path", path,
                        "--method", "POST" if route == "compress" else "GET", "--input", str(data/f"input-{size}.bin"),
                        "--expected", str(expected), "--concurrency", "32", "--warmup", str(manifest["warmup_seconds"]),
                        "--seconds", str(manifest["measure_seconds"])]
                    client = subprocess.Popen(client_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=cerr, bufsize=0)
                    while read_line(client) != "MEASURE_READY":
                        pass
                    send(server, "begin"); assert read_json(server)["begun"]
                    send(client, "start")
                    assert read_line(client) == "MEASURE_DONE"
                    send(server, "end"); server_result = read_json(server)
                    result = read_json(client)
                    send(server, "quit"); closed = read_json(server)
                    client_rc, server_rc = client.wait(10), server.wait(10)
                    row = {"key": key, "mode": mode, "route": route, "size": size, "burn": burn,
                        "workers": workers, "repeat": repeat, "ready": ready, "client": result,
                        "server": server_result, "closed": closed, "returncodes": [server_rc,client_rc],
                        "commands": [server_cmd,client_cmd]}
                    try:
                        validate_row(row,key)
                    except Exception:
                        (out/(key+".failed.json")).write_text(json.dumps(row,indent=2))
                        raise
                    (out / (key+".json")).write_text(json.dumps(row, indent=2))
                    index.append(key)
                    print(json.dumps({"key": key, "rps": result["rps"], "server_cpu": server_result["cpu_cores"],
                        "client_cpu": result["cpu_cores"], "lag_p99_us": server_result["loop_lag"].get("p99_us")}), flush=True)
                    (out / "index.json").write_text(json.dumps(index, indent=2))
            finally:
                for proc in (client,server):
                    if proc and proc.poll() is None:
                        proc.terminate()
                        try: proc.wait(5)
                        except subprocess.TimeoutExpired: proc.kill(); proc.wait()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--resume", action="store_true")
    main(parser.parse_args())
