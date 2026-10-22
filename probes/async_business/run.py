"""Same-core POST benchmark, exact response + post-business header validation."""
import argparse
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import random
import select
import socket
import subprocess
import sys
import time
import zlib

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from probes.final.run import threads as base_threads, read_line, read_json, send, atomic_json, process_sample, stop_process, cgroup_stats, native_libraries

MODES = ["seastar", "seastar-dedicated", "seastar-cooperative", "seastar-inline", "multiprocess", "multiprocess-zlib", "single", "native", "native-cooperative", "native-inline"]


def threads(pids):
    rows = base_threads(pids)
    for tid, row in rows.items():
        stat = Path(f"/proc/{row['pid']}/task/{tid}/stat").read_text().rsplit(")", 1)[1].split()
        row.update(minor_faults=int(stat[7]), user_ticks=int(stat[11]), system_ticks=int(stat[12]))
    return rows


def one(args, mode, out, available):
    server_cpus = available[:args.server_cores]
    client_cpus = available[args.server_cores:args.server_cores + args.client_cores]
    key = f"compress-{mode}-r{args.round}"
    is_native, hybrid = mode.startswith("native"), mode.startswith("seastar")
    cooperative = mode.endswith("cooperative")
    inline = mode.endswith("inline")
    reactor_cpus = (server_cpus if cooperative or inline else server_cpus[:1]) if is_native or hybrid else []
    assert not (out / (key+".json")).exists(), "refuse to overwrite measurements"
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0)); port = sock.getsockname()[1]
    server = client = None
    try:
        with (out / (key+".server.log")).open("wb") as serr, (out / (key+".client.log")).open("wb") as cerr:
            command = ([str(ROOT/"build/async_native"), str(port), ",".join(map(str, reactor_cpus)),
                        "" if cooperative or inline else ",".join(map(str, server_cpus)),
                        *(["cooperative"] if cooperative else ["inline"] if inline else [])]
                if is_native else [sys.executable, str(ROOT/"probes/async_business/server.py"), "--port", str(port), "--mode", mode])
            server_cmd = ["taskset", "-c", ",".join(map(str, server_cpus))] + command
            server = subprocess.Popen(server_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=serr, bufsize=0, start_new_session=True)
            ready = read_json(server)
            assert ready["ready"]
            if hybrid or is_native:
                observed = [s["cpu"] if is_native else s["affinity"][0] for s in ready["workers"][0]["shards"]]
                assert observed == reactor_cpus
            if not is_native:
                assert all("uvloop.Loop" in w["event_loop"] for w in ready["workers"])
                assert len(ready["pids"]) == (args.server_cores if mode.startswith("multiprocess") else 1)
            endpoint = "reference" if is_native else "python" if hybrid else "hybrid"
            client_cmd = ["taskset", "-c", ",".join(map(str, client_cpus)), str(ROOT/"build/http_load"),
                "--smp", str(args.client_cores), "--cpuset", ",".join(map(str, client_cpus)),
                "--thread-affinity", "1", "--reactor-backend", "epoll", "--overprovisioned",
                "--default-log-level", "warn", "--enable-glibc-exception-scaling-workaround", "0",
                "--port", str(port), "--target-pids", ",".join(map(str, ready["pids"])),
                "--path", f"/{endpoint}/compress?level=6", "--method", "POST",
                "--input", str(out/"input.bin"), "--expected", str(out/"expected.bin"),
                "--result-tag", str((out/"expected.bin").stat().st_size), "--concurrency", str(args.connections),
                "--rate", str(args.rate), "--warmup", str(args.warmup), "--seconds", str(args.seconds)]
            client = subprocess.Popen(client_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=cerr, bufsize=0, start_new_session=True)
            while read_line(client) != "MEASURE_READY": pass
            send(server, "begin"); assert read_json(server)["begun"]
            before_threads = threads(ready["service_pids"])
            samples = [process_sample(ready["service_pids"])]; cg0 = cgroup_stats()
            start = time.monotonic(); send(client, "start"); next_sample = start+1
            while True:
                if select.select([client.stdout], [], [], max(0, min(.1, next_sample-time.monotonic())))[0]:
                    if read_line(client) == "MEASURE_DONE": break
                now = time.monotonic()
                if now >= next_sample:
                    samples.append(process_sample(ready["service_pids"])); next_sample = now+1
                if now-start > args.seconds+20: raise TimeoutError("measurement exceeded bound")
            finish = time.monotonic(); samples.append(process_sample(ready["service_pids"]))
            after_threads = threads(ready["service_pids"])
            send(server, "end"); result = read_json(server); cg1 = cgroup_stats()
            c = read_json(client)
            send(server, "quit"); closed = read_json(server)
            rc = [server.wait(10), client.wait(10)]
            tickrate = os.sysconf("SC_CLK_TCK")
            old = {p["pid"]: p["cpu_ticks"] for p in samples[0]["processes"]}
            cpu = sum(p["cpu_ticks"]-old[p["pid"]] for p in samples[-1]["processes"])/tickrate/(finish-start)
            for tid, row in after_threads.items():
                row["cpu_cores"] = (row["ticks"]-before_threads.get(tid, row)["ticks"])/tickrate/(finish-start)
                row["measurement_delta"] = {k: row[k]-before_threads.get(tid, row)[k]
                    for k in ("minor_faults", "user_ticks", "system_ticks")}
            row = dict(key=key, mode=mode, round=args.round, commands=[server_cmd, client_cmd],
                server_cpus=server_cpus, reactor_cpus=reactor_cpus, client_cpus=client_cpus,
                warmup=args.warmup, seconds=args.seconds, payload_bytes=args.size, rate=args.rate,
                concurrency=args.connections, ready=ready, client=c, server=result, closed=closed,
                validated=False, returncodes=rc, server_cpu_cores=cpu, threads=after_threads,
                cgroup_delta={k:v-cg0[k] for k,v in cg1.items()}, memory_samples=samples,
                max_sampled_pss_mib=max(s["pss_bytes"] for s in samples)/1048576)
            atomic_json(out/(key+".json"), row)
            measured = sum(w["delta"]["responses"] for w in result["workers"])
            assert rc == [0, 0] and c["errors"] == c["integrity_errors"] == c["dropped"] == 0
            assert c["requests"] == c["scheduled"] == measured == sum(c["server_pids"].values())
            assert c["requests"] > 0 and c["response_bytes"] == (out/"expected.bin").stat().st_size*c["requests"]
            assert c["new_connections"] == 0 and c["total_connections"] == args.connections
            for field in ("python_http_started", "python_http_resumed", "python_http_finished"):
                assert sum(w["delta"].get(field, 0) for w in result["workers"]) == (c["requests"] if hybrid else 0)
            decisions = sum(w["delta"].get("python_decisions", 0) for w in result["workers"])
            assert decisions == (c["requests"] if not hybrid and not is_native else 0)
            if hybrid:
                assert sum(w["delta"]["body_bytes_to_python"] for w in result["workers"]) == 0
                assert all(w["delta"]["python_http_errors"] == 0 for w in result["workers"])
            assert closed["closed"] and all(w.get(k, 0) == 0 for w in closed["workers"]
                for k in ("active_requests", "reserved_bytes", "pending_decisions", "queued_decisions"))
            row["validated"] = True
            atomic_json(out/(key+".json"), row)
            print(json.dumps({"mode":mode, "round":args.round, "rps":c["rps"], "p99_ms":c["p99_us"]/1000,
                "server_cpu":cpu, "client_cpu":c["cpu_cores"], "errors":c["errors"],
                "pss_mib":row["max_sampled_pss_mib"]}), flush=True)
    finally:
        stop_process(client); stop_process(server)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--round", type=int, default=0)
    parser.add_argument("--warmup", type=float, default=2)
    parser.add_argument("--seconds", type=float, default=6)
    parser.add_argument("--connections", type=int, default=32)
    parser.add_argument("--server-cores", type=int, default=3)
    parser.add_argument("--client-cores", type=int, default=2)
    parser.add_argument("--size", type=int, default=262144)
    parser.add_argument("--rate", type=float, default=0)
    parser.add_argument("--modes", nargs="+", choices=MODES, default=["seastar", "multiprocess", "native"])
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    available = sorted(os.sched_getaffinity(0))
    assert len(available) >= args.server_cores + args.client_cores + 1
    assert 0 < args.size <= 1048576
    os.sched_setaffinity(0, {available[-1]})
    out = ROOT/args.out; out.mkdir(parents=True, exist_ok=True)
    rng = random.Random(61)
    text = (b'{"id":12345,"kind":"reading","value":27.5,"unit":"C"}\n'*400)[:16384]
    payload = b"".join(rng.randbytes(16384) + text for _ in range((args.size+32767)//32768))[:args.size]
    expected = zlib.compress(payload, 6)
    for name, data in (("input.bin", payload), ("expected.bin", expected)):
        path = out/name
        if path.exists(): assert path.read_bytes() == data
        else: path.write_bytes(data)
    manifest = out/"manifest.json"
    if not manifest.exists():
        paths = [ROOT/"build/http_load", ROOT/"build/async_native", *list((ROOT/"build").glob("_seastar_probe*.so")),
                 *list((ROOT/"probes/async_business").glob("*.py")), ROOT/"probes/async_business/native_main.cc",
                 *list((ROOT/"native").glob("*.cc")), *list((ROOT/"native").glob("*.hh")), ROOT/"src/oldman_server/native/engine.py",
                 ROOT/"scripts/comparison_server.py", ROOT/"scripts/comparison_metrics.py", ROOT/"tests/test_async_http.py"]
        atomic_json(manifest, dict(created=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            python=sys.version, kernel=platform.release(), cpu_max=Path("/sys/fs/cgroup/cpu.max").read_text().strip(),
            malloc_env={k:os.environ.get(k) for k in ("MALLOC_TRIM_THRESHOLD_", "MALLOC_MMAP_THRESHOLD_", "MALLOC_ARENA_MAX", "GLIBC_TUNABLES")},
            versions={p:importlib.metadata.version(p) for p in ("aiohttp", "uvloop")},
            fingerprints={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths},
            input_sha256=hashlib.sha256(payload).hexdigest(), expected_sha256=hashlib.sha256(expected).hexdigest(),
            native_libraries=native_libraries(), server_budget_cpus=available[:args.server_cores],
            payload_bytes=args.size, output_bytes=len(expected), protocol="HTTP/1.1 POST; keepalive; no pipeline",
            acceptance="Actual per-request Python await native + result decision; same-core uvloop multiprocess minimum"))
    current = json.loads(manifest.read_text())
    assert current["server_budget_cpus"] == available[:args.server_cores]
    for relative, expected_hash in current["fingerprints"].items():
        assert hashlib.sha256((ROOT/relative).read_bytes()).hexdigest() == expected_hash, relative
    modes = args.modes if args.round%2 == 0 else list(reversed(args.modes))
    for mode in modes:
        one(args, mode, out, available)
