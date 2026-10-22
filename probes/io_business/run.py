"""One workload: GET -> actual pooled upstream GET -> checked 8 KiB body."""
import argparse
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import select
import socket
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from probes.async_business.run import threads
from probes.final.run import (read_line, read_json, send, atomic_json, process_sample,
                              stop_process, cgroup_stats, native_libraries)


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def one(args, mode, out, cpus):
    server_cpus, client_cpus, upstream_cpus = cpus[:3], cpus[3:5], cpus[5:7]
    key = f"fetch-{mode}-r{args.round}"
    assert not (out/(key+".json")).exists(), "refuse to overwrite measurement"
    upstream = server = client = None
    logs = []
    commands = []
    def start(command, name):
        log = (out/(key+"."+name+".log")).open("wb"); logs.append(log)
        commands.append(command)
        return subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=log, bufsize=0, start_new_session=True)
    def native_command(cpus, listen, origin):
        affinity = ",".join(map(str, cpus))
        return ["taskset", "-c", affinity, str(ROOT/"build/io_native"), str(listen), affinity, origin]
    try:
        up_port, gateway_port = port(), port()
        upstream = start(native_command(upstream_cpus, up_port, ""), "upstream")
        up_ready = read_json(upstream); assert up_ready["ready"]
        direct = mode == "upstream"
        if direct:
            server, ready, gateway_port = upstream, up_ready, up_port
        else:
            command = (native_command(server_cpus, gateway_port, str(up_port)) if mode == "native" else
                ["taskset", "-c", ",".join(map(str, server_cpus)), sys.executable,
                 str(ROOT/"probes/io_business/server.py"), "--mode", mode, "--port", str(gateway_port),
                 "--upstream", str(up_port)])
            server = start(command, "server"); ready = read_json(server)
            assert ready["ready"]
            assert len(ready["pids"]) == (3 if mode == "multiprocess" else 1)
            if mode != "native":
                assert all("uvloop.Loop" in w["event_loop"] for w in ready["workers"])
            if mode.startswith("seastar"):
                assert [s["affinity"][0] for s in ready["workers"][0]["shards"]] == (
                    server_cpus[1:] if mode in ("seastar-dedicated", "seastar-retire") else server_cpus)
                assert not ready["workers"][0]["compute_workers"]
                assert ready["workers"][0]["retire_http_contexts"] == (mode == "seastar-retire")
        path = "/native/fixed?size=8192" if direct else f"/{'reference' if mode == 'native' else 'python'}/fetch?upstream=0"
        client_cmd = ["taskset", "-c", ",".join(map(str, client_cpus)), str(ROOT/"build/http_load"),
            "--smp", "2", "--cpuset", ",".join(map(str, client_cpus)), "--thread-affinity", "1",
            "--reactor-backend", "epoll", "--overprovisioned", "--default-log-level", "warn",
            "--enable-glibc-exception-scaling-workaround", "0", "--port", str(gateway_port),
            "--target-pids", ",".join(map(str, ready["pids"])), "--path", path, "--method", "GET",
            "--expected", str(out/"expected.bin"), "--concurrency", str(args.connections),
            "--rate", "0", "--warmup", str(args.warmup), "--seconds", str(args.seconds)]
        if not direct: client_cmd += ["--result-tag", "200:8192"]
        client = start(client_cmd, "client")
        while read_line(client) != "MEASURE_READY": pass
        send(upstream, "begin"); assert read_json(upstream)["begun"]
        if not direct: send(server, "begin"); assert read_json(server)["begun"]
        before_threads = threads(ready["service_pids"])
        samples = [process_sample(ready["service_pids"])]
        up_samples = [process_sample(up_ready["service_pids"])]; cg0 = cgroup_stats()
        start_time = time.monotonic(); send(client, "start"); next_sample = start_time+1
        while True:
            if select.select([client.stdout], [], [], max(0, min(.1, next_sample-time.monotonic())))[0]:
                if read_line(client) == "MEASURE_DONE": break
            now = time.monotonic()
            if now >= next_sample:
                samples.append(process_sample(ready["service_pids"]))
                up_samples.append(process_sample(up_ready["service_pids"]))
                next_sample = now+1
            if now-start_time > args.seconds+20: raise TimeoutError("measurement exceeded bound")
        finish = time.monotonic()
        samples.append(process_sample(ready["service_pids"]))
        up_samples.append(process_sample(up_ready["service_pids"]))
        after_threads = threads(ready["service_pids"])
        send(server, "end"); result = read_json(server)
        if direct: up_result = result
        else: send(upstream, "end"); up_result = read_json(upstream)
        cg1 = cgroup_stats(); c = read_json(client)
        send(server, "quit"); closed = read_json(server)
        if direct: up_closed = closed
        else: send(upstream, "quit"); up_closed = read_json(upstream)
        rc = [server.wait(10), client.wait(10), upstream.wait(10)]
        tickrate = os.sysconf("SC_CLK_TCK")
        def cpu_use(samples):
            old = {p["pid"]:p["cpu_ticks"] for p in samples[0]["processes"]}
            return sum(p["cpu_ticks"]-old[p["pid"]] for p in samples[-1]["processes"])/tickrate/(finish-start_time)
        for tid, row in after_threads.items():
            row["cpu_cores"] = (row["ticks"]-before_threads.get(tid, row)["ticks"])/tickrate/(finish-start_time)
        row = dict(key=key, mode=mode, round=args.round, commands=commands, ready=ready, client=c,
            server=result, closed=closed, upstream_ready=up_ready, upstream=up_result, upstream_closed=up_closed,
            server_cpus=server_cpus if not direct else upstream_cpus, client_cpus=client_cpus,
            upstream_cpus=upstream_cpus, warmup=args.warmup, seconds=args.seconds, concurrency=args.connections,
            server_cpu_cores=cpu_use(samples), upstream_cpu_cores=cpu_use(up_samples), threads=after_threads,
            cgroup_delta={k:v-cg0[k] for k,v in cg1.items()}, memory_samples=samples,
            max_sampled_pss_mib=max(s["pss_bytes"] for s in samples)/1048576,
            returncodes=rc, validated=False)
        atomic_json(out/(key+".json"), row)
        def total(stats, field): return sum(w["delta"].get(field, 0) for w in stats["workers"])
        assert rc == [0, 0, 0] and c["errors"] == c["integrity_errors"] == c["dropped"] == 0
        assert c["requests"] == c["scheduled"] == total(result, "responses") == total(up_result, "responses")
        assert c["requests"] == sum(c["server_pids"].values()) > 0
        assert c["response_bytes"] == 8192*c["requests"]
        assert c["new_connections"] == 0 and c["total_connections"] == args.connections
        if not direct:
            assert total(result, "upstream_requests") == total(result, "upstream_responses") == c["requests"]
            assert total(result, "upstream_bytes") == c["response_bytes"]
        for field in ("python_http_started", "python_http_resumed", "python_http_finished"):
            assert total(result, field) == (c["requests"] if mode.startswith("seastar") or mode == "multiprocess" else 0)
        assert total(result, "python_http_errors") == total(result, "rejected") == 0
        if mode.startswith("seastar"):
            assert total(result, "body_bytes_to_python") == 0
            assert total(result, "python_terminal_events") == (0 if mode == "seastar-retire" else c["requests"])
        for state in (closed, up_closed):
            assert state["closed"] and all(w.get(k, 0) == 0 for w in state["workers"]
                for k in ("active_requests", "reserved_bytes", "pending_decisions", "queued_decisions"))
        row["validated"] = True; atomic_json(out/(key+".json"), row)
        print(json.dumps({"mode":mode, "round":args.round, "rps":c["rps"], "p99_ms":c["p99_us"]/1000,
            "server_cpu":row["server_cpu_cores"], "upstream_cpu":row["upstream_cpu_cores"],
            "client_cpu":c["cpu_cores"], "errors":c["errors"]}), flush=True)
    finally:
        stop_process(client)
        if server is not upstream: stop_process(server)
        stop_process(upstream)
        for log in logs: log.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    parser.add_argument("--round", type=int, default=0)
    parser.add_argument("--seconds", type=float, default=6)
    parser.add_argument("--warmup", type=float, default=2)
    parser.add_argument("--connections", type=int, default=192)
    parser.add_argument("--modes", nargs="+", choices=["upstream", "native", "multiprocess", "seastar", "seastar-dedicated", "seastar-retire"],
                        default=["native", "multiprocess", "seastar-dedicated"])
    args = parser.parse_args()
    cpus = sorted(os.sched_getaffinity(0)); assert len(cpus) >= 8
    os.sched_setaffinity(0, {cpus[-1]})
    out = ROOT/"results"/args.out; out.mkdir(parents=True, exist_ok=True)
    expected = out/"expected.bin"
    if expected.exists(): assert expected.read_bytes() == b"x"*8192
    else: expected.write_bytes(b"x"*8192)
    manifest = out/"manifest.json"
    paths = [ROOT/"build/http_load", ROOT/"build/io_native", *list((ROOT/"build").glob("_seastar_probe*.so")),
        *list((ROOT/"probes/io_business").glob("*.py")), ROOT/"probes/io_business/native_main.cc",
        *list((ROOT/"native").glob("*.cc")), *list((ROOT/"native").glob("*.hh")), ROOT/"src/oldman_server/native/engine.py",
        ROOT/"scripts/comparison_server.py", ROOT/"scripts/comparison_metrics.py", ROOT/"tests/test_async_fetch.py",
        ROOT/"tests/test_async_fetch_framing.py", ROOT/"tests/test_http_retirement.py"]
    if not manifest.exists():
        atomic_json(manifest, dict(created=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            python=sys.version, kernel=platform.release(), cpu_max=Path("/sys/fs/cgroup/cpu.max").read_text().strip(),
            cpu_model=next((s.split(":", 1)[1].strip() for s in Path("/proc/cpuinfo").read_text().splitlines()
                            if s.startswith("model name")), "unknown"),
            versions={p:importlib.metadata.version(p) for p in ("aiohttp", "uvloop")},
            diagnostic_env={k:os.environ.get(k) for k in ("PROBE_PROFILE_DIR", "PROBE_BRIDGE_TIMING_DIR")},
            fingerprints={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths},
            native_libraries=native_libraries(), server_cpus=cpus[:3], client_cpus=cpus[3:5], upstream_cpus=cpus[5:7],
            protocol="HTTP/1.1 GET, pooled upstream GET, no pipeline, exact 8192-byte response",
            acceptance="Per-request Python before/after await; at least same-core uvloop multiprocess throughput"))
    current = json.loads(manifest.read_text())
    assert current["server_cpus"] == cpus[:3]
    for relative, digest in current["fingerprints"].items():
        assert hashlib.sha256((ROOT/relative).read_bytes()).hexdigest() == digest, relative
    modes = args.modes if args.round%2 == 0 else list(reversed(args.modes))
    for mode in modes: one(args, mode, out, cpus)
