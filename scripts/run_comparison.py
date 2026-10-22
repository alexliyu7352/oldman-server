"""Serial equal-budget comparisons. No builds/reviews during formal timings."""
import argparse
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import re
import select
import signal
import socket
import subprocess
import sys
import time
import zlib
from oldman_server.native import _native as native

from benchmark_work import cgroup_stats, distribution_ns, payload_for
from run_http_benchmarks import read_json, read_line, send

ROOT = Path(__file__).resolve().parents[1]
MODES = ["direct", "pool", "multiprocess", "seastar", "seastar-shared"]


def atomic_json(path, value):
    temp = path.with_suffix(path.suffix+".tmp")
    temp.write_text(json.dumps(value, indent=2)+"\n")
    temp.replace(path)


def process_sample(pids):
    rows = []
    for pid in pids:
        directory = Path(f"/proc/{pid}")
        stat = (directory/"stat").read_text().rsplit(")", 1)[1].split()
        mem = {}
        for line in (directory/"smaps_rollup").read_text().splitlines()[1:]:
            parts = line.split()
            if len(parts) == 3 and parts[2] == "kB": mem[parts[0].rstrip(":")] = int(parts[1])*1024
        rows.append({"pid": pid, "cpu_ticks": int(stat[11])+int(stat[12]), "memory": mem})
    return {"time": time.monotonic(), "processes": rows,
            "pss_bytes": sum(r["memory"]["Pss"] for r in rows),
            "rss_bytes": sum(r["memory"]["Rss"] for r in rows)}


def slo_pass(client):
    return (client["rate"] > 0 and client["requests"] > 0 and client["errors"] == 0
            and client["dropped"] == 0 and client["p99_us"] <= 50000)


def validate_manifest(old, new):
    for key in ("protocol", "warmup", "seconds", "repeats", "concurrency", "server_cpus", "client_cpus",
                "payload", "expected", "fingerprints", "dependencies", "cases", "cpu_max",
                "python", "kernel", "zlib", "native_zlib", "native_libraries"):
        if old.get(key) != new.get(key): raise ValueError(f"resume provenance mismatch: {key}")


def validate_row(row, key, manifest=None):
    try:
        c = row["client"]
        assert row["key"] == key and row["returncodes"] == [0, 0]
        assert c["rate"] == row["rate"] and row["mode"] == row["ready"]["mode"]
        assert c["integrity_errors"] == 0 and c["requests"] > 0
        assert c["scheduled"] == c["requests"]+c["errors"]+c["dropped"]
        assert c["new_connections"] == 0 and c["total_connections"] == 32
        assert set(map(int, c["server_pids"])) <= set(row["ready"]["pids"])
        assert sum(c["server_pids"].values()) == c["requests"]
        assert sum(w["delta"]["responses"] for w in row["server"]["workers"]) == c["requests"]
        assert row["closed"]["closed"]
        assert all(w["active_requests"] == 0 and w["reserved_bytes"] == 0 for w in row["closed"]["workers"])
        assert all(w.get("queued_work", 0) == 0 and w.get("pending_decisions", 0) == 0
                   and w.get("queued_decisions", 0) == 0 for w in row["closed"]["workers"])
        assert row["memory"]["sampled_max_pss_bytes"] > 0
        assert row["slo_pass"] == slo_pass(c)
        if row["rate"] == 0: assert c["errors"] == 0
        if manifest is not None:
            assert c["arrival_seconds"] == manifest["seconds"]
            assert manifest["seconds"]-.001 <= c["seconds"] < manifest["seconds"]+20
            if row["rate"] > 0:
                assert c["scheduled"] == math.ceil(row["rate"]*manifest["seconds"]-1e-8)
    except (KeyError, TypeError, AssertionError) as e:
        raise ValueError(f"invalid measurement row: {key}") from e


def stop_process(p):
    if p is None:
        return
    try:
        # Every caller starts a new session. The owned process group can
        # outlive its leader, including workers that ignore SIGTERM.
        try:
            os.killpg(p.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            p.wait(3)
        except subprocess.TimeoutExpired:
            pass
        finally:
            try:
                os.killpg(p.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        p.wait()
    finally:
        for pipe in (p.stdout, p.stderr, p.stdin):
            if pipe is not None:
                pipe.close()


def one(out, mode, rate, repeat, manifest, resume=False, label=""):
    key = f"{mode}-rate{rate:g}-r{repeat}" + (f"-{label}" if label else "")
    path = out/(key+".json")
    if resume and path.exists():
        row = json.loads(path.read_text()); validate_row(row, key, manifest); return row
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0)); port = sock.getsockname()[1]
    server = client = None
    try:
        with (out/(key+".server.log")).open("wb") as serr, (out/(key+".client.log")).open("wb") as cerr:
            server_cmd = ["taskset", "-c", ",".join(map(str, manifest["server_cpus"])), sys.executable,
                str(ROOT/"scripts/comparison_server.py"), "--mode", mode, "--port", str(port)]
            server = subprocess.Popen(server_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=serr, bufsize=0, start_new_session=True)
            ready = read_json(server)
            assert ready["ready"]
            idle = process_sample(ready["service_pids"])
            client_cmd = ["taskset", "-c", ",".join(map(str, manifest["client_cpus"])), str(ROOT/"build/http_load"),
                "--smp", "2", "--cpuset", ",".join(map(str, manifest["client_cpus"])), "--thread-affinity", "1",
                "--reactor-backend", "epoll", "--overprovisioned", "--default-log-level", "warn",
                "--enable-glibc-exception-scaling-workaround", "0", "--port", str(port),
                "--target-pids", ",".join(map(str, ready["pids"])), "--path", "/hybrid/compress",
                "--method", "POST", "--input", str(ROOT/"build/comparison-data/input.bin"),
                "--expected", str(ROOT/"build/comparison-data/expected.bin"), "--concurrency", "32",
                "--rate", str(rate), "--warmup", str(manifest["warmup"]), "--seconds", str(manifest["seconds"])]
            client = subprocess.Popen(client_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=cerr, bufsize=0, start_new_session=True)
            while read_line(client) != "MEASURE_READY": pass
            cg0 = cgroup_stats()
            send(server, "begin"); assert read_json(server)["begun"]
            samples = [process_sample(ready["service_pids"])]
            controller_cpu = time.process_time()
            start = time.monotonic()
            send(client, "start")
            next_sample = start+1
            while True:
                if select.select([client.stdout], [], [], max(0, min(.1, next_sample-time.monotonic())))[0]:
                    if read_line(client) == "MEASURE_DONE": break
                now = time.monotonic()
                if now >= next_sample:
                    samples.append(process_sample(ready["service_pids"])); next_sample = now+1
                if now-start > manifest["seconds"]+20: raise TimeoutError("measurement/drain exceeded limit")
            finish = time.monotonic()
            controller_cpu = time.process_time()-controller_cpu
            samples.append(process_sample(ready["service_pids"]))
            send(server, "end"); result = read_json(server)
            cg1 = cgroup_stats()
            client_result = read_json(client)
            lag = [n for w in result["workers"] for n in w.pop("lag_ns")]
            result["loop_lag"] = distribution_ns(lag)
            result["worst_worker_loop_lag_p99_us"] = max(w["loop_lag"].get("p99_us", 0) for w in result["workers"])
            old = {p["pid"]: p["cpu_ticks"] for p in samples[0]["processes"]}
            cpu = sum(p["cpu_ticks"]-old[p["pid"]] for p in samples[-1]["processes"])/os.sysconf("SC_CLK_TCK")
            result.update(seconds=finish-start, cpu_seconds=cpu, cpu_cores=cpu/(finish-start),
                          controller_cpu_seconds=controller_cpu, cgroup_delta={k: v-cg0[k] for k, v in cg1.items()})
            send(server, "quit"); closed = read_json(server)
            client_rc, server_rc = client.wait(10), server.wait(10)
            row = {"key": key, "mode": mode, "rate": rate, "repeat": repeat,
                "ready": ready, "server": result, "client": client_result, "closed": closed,
                "returncodes": [server_rc, client_rc], "commands": [server_cmd, client_cmd],
                "memory": {"idle": idle, "samples": samples,
                    "sampled_max_pss_bytes": max(s["pss_bytes"] for s in samples)},
                "slo_pass": slo_pass(client_result)}
            try: validate_row(row, key, manifest)
            except ValueError:
                atomic_json(out/(key+".failed.json"), row); raise
            atomic_json(path, row)
            return row
    finally:
        stop_process(client); stop_process(server)


def fingerprint():
    paths = [ROOT/"build/http_load", *list((ROOT/"src/oldman_server/native").glob("_native*.so")),
             *list((ROOT/"native").glob("*.*")), *list((ROOT/"python").glob("*.py")),
             *list((ROOT/"scripts").glob("*.py")), ROOT/"requirements-phase4.txt"]
    return {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}


def native_libraries():
    """Resolve ldd in the actual LD_LIBRARY_PATH, and hash loaded library bytes."""
    import aiohttp._http_parser
    binaries = [ROOT/"build/http_load", *list((ROOT/"src/oldman_server/native").glob("_native*.so")),
                Path(sys.executable).resolve(), Path(aiohttp._http_parser.__file__)]
    paths = set(binaries)
    for binary in binaries:
        output = subprocess.check_output(["ldd", str(binary)], text=True)
        for filename in re.findall(r"(/[^\s]+)\s+\(", output): paths.add(Path(filename).resolve())
    return {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)}


def summary(row, count, total):
    c = row["client"]
    print(json.dumps({"done": count, "total": total, "key": row["key"], "rps": c["rps"],
        "p99_ms": c["p99_us"]/1000, "drops": c["dropped"], "errors": c["errors"],
        "server_cpu": row["server"]["cpu_cores"], "client_cpu": c["cpu_cores"],
        "pss_mib": row["memory"]["sampled_max_pss_bytes"]/1048576,
        "lag_p99_ms": row["server"]["loop_lag"].get("p99_us", 0)/1000,
        "slo_pass": row["slo_pass"]}), flush=True)


def main(args):
    available = sorted(os.sched_getaffinity(0))
    assert len(available) >= 8
    server_cpus, client_cpus = available[:6], available[6:8]
    os.sched_setaffinity(0, {server_cpus[0]})
    out = ROOT/"results"/("comparison-"+args.phase+args.suffix)
    out.mkdir(exist_ok=True)
    data = ROOT/"build/comparison-data"
    data.mkdir(exist_ok=True)
    body = payload_for(1048576); expected = zlib.compress(body, 6)
    (data/"input.bin").write_bytes(body); (data/"expected.bin").write_bytes(expected)
    deps = {line.split("==")[0]: importlib.metadata.version(line.split("==")[0])
            for line in (ROOT/"requirements-phase4.txt").read_text().splitlines()}
    cases = [(mode, rate, label) for rate, label in ((0, "closed"), (50, "fixed")) for mode in MODES]
    if args.phase == "formal":
        brackets = json.loads((ROOT/"results/comparison-calibration/brackets.json").read_text())
        cases += [(mode, brackets[mode][bound], "slo-"+bound) for bound in ("low", "high") for mode in MODES]
    manifest = {"protocol": 2, "warmup": 10 if args.phase == "formal" else (.05 if args.phase == "smoke" else 1),
        "seconds": 30 if args.phase == "formal" else (.2 if args.phase == "smoke" else 3),
        "repeats": 3 if args.phase == "formal" else 1, "concurrency": 32,
        "server_cpus": server_cpus, "client_cpus": client_cpus, "cases": cases,
        "payload": hashlib.sha256(body).hexdigest(), "expected": hashlib.sha256(expected).hexdigest(),
        "fingerprints": fingerprint(), "dependencies": deps, "cpu_max": Path("/sys/fs/cgroup/cpu.max").read_text().strip(),
        "python": sys.version, "kernel": platform.release(), "zlib": zlib.ZLIB_RUNTIME_VERSION,
        "native_zlib": [native.zlib_header_version, native.zlib_runtime_version], "native_libraries": native_libraries(),
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "clock": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    # JSON normalization ensures tuple/list case rows compare across resume.
    manifest = json.loads(json.dumps(manifest))
    mf = out/"manifest.json"
    if mf.exists():
        if not args.resume: raise ValueError("existing experiment; use --resume with identical provenance")
        old = json.loads(mf.read_text()); validate_manifest(old, manifest); manifest = old
    else: atomic_json(mf, manifest)
    index, rows = [], []
    for repeat in range(manifest["repeats"]):
        ordered = cases if repeat % 2 == 0 else list(reversed(cases))
        for mode, rate, label in ordered:
            row = one(out, mode, rate, repeat, manifest, args.resume, label)
            rows.append(row); index.append(row["key"]); atomic_json(out/"index.json", index)
            summary(row, len(index), len(cases)*manifest["repeats"])
    if args.phase == "calibration":
        brackets = {}
        for mode in MODES:
            cap = next(r["client"]["rps"] for r in rows if r["mode"] == mode and r["rate"] == 0)
            # Explore only for a useful formal bracket; these are not result claims.
            low, high = 0., math.ceil(cap*1.1)
            high_row = one(out, mode, high, 0, manifest, args.resume, "bracket")
            summary(high_row, 0, 0)
            # A short closed-loop pilot is an estimate, not a capacity bound.
            # Expand empirically until there is a failing side to bisect.
            for expansion in range(6):
                if not high_row["slo_pass"]: break
                low = high
                high = math.ceil(high*1.25)
                high_row = one(out, mode, high, 0, manifest, args.resume, "bracket")
                summary(high_row, expansion+1, 6)
            if high_row["slo_pass"]: raise RuntimeError("no failing upper bound after six calibration expansions")
            for attempt in range(5):
                rate = round((low+high)/2, 1)
                row = one(out, mode, rate, 0, manifest, args.resume, "bracket")
                summary(row, attempt+1, 5)
                if row["slo_pass"]: low = rate
                else: high = rate
                if low and high/low <= 1.18: break
            if not low: raise RuntimeError("no passing calibration rate")
            brackets[mode] = {"low": round(low*.9, 1), "high": round(high*1.05, 1),
                              "pilot_pass": low, "pilot_fail": high}
        atomic_json(out/"brackets.json", brackets)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=["smoke", "calibration", "formal"], required=True)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--suffix", default="", help="separate diagnostic output directory")
    main(parser.parse_args())
