"""Equal three-CPU budget, separately labelled dynamic Python and native policy paths."""
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
sys.path.insert(0, str(ROOT / "scripts"))
from run_http_benchmarks import read_line, read_json, send
from run_comparison import atomic_json, process_sample, stop_process, native_libraries
from benchmark_work import cgroup_stats

MODES = ["seastar-batch", "seastar-coalesced", "seastar-shared", "seastar-policy", "seastar-policy1", "single", "multiprocess", "multiprocess-asyncio", "native", "native2", "native1"]


def threads(pids):
    rows = {}
    for pid in pids:
        for task in Path(f"/proc/{pid}/task").iterdir():
            try:
                stat = (task / "stat").read_text().rsplit(")", 1)[1].split()
                status = (task / "status").read_text().splitlines()
                rows[task.name] = {"pid": pid, "ticks": int(stat[11])+int(stat[12]),
                    "affinity": next(s.split(":", 1)[1].strip() for s in status if s.startswith("Cpus_allowed_list:"))}
            except (FileNotFoundError, ProcessLookupError): pass  # the thread exited meanwhile
    return rows


def one(args, mode, out, available):
    server_cpus, client_cpus = available[:3], available[3:3+args.client_cores]
    key = f"{args.workload}-{mode}-r{args.round}-client{args.client_cores}"
    is_native = mode.startswith("native")
    is_policy = mode.startswith("seastar-policy")
    reactor_cpus = (server_cpus[:1] if mode.endswith("1") else server_cpus[1:] if mode in ("native2", "seastar-batch", "seastar-coalesced") else server_cpus) if is_native or mode.startswith("seastar") else []
    if (out / (key+".json")).exists(): raise ValueError("refuse to overwrite measurement")
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]
    server = client = None
    try:
        with (out / (key+".server.log")).open("wb") as serr, (out / (key+".client.log")).open("wb") as cerr:
            prefix = ["taskset", "-c", ",".join(map(str, server_cpus))]
            command = ([str(ROOT/"build/fixed_native"), str(port), ",".join(map(str, reactor_cpus)), str(out/"expected.bin")]
                if is_native else [sys.executable, str(ROOT/"probes/final/server.py"), "--port", str(port), "--mode", mode, "--file", str(out/"expected.bin")])
            server_cmd = prefix + command
            server = subprocess.Popen(server_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=serr, bufsize=0, start_new_session=True)
            ready = read_json(server)
            assert ready["ready"]
            if is_native or mode.startswith("seastar"):
                shards = ready["workers"][0]["shards"]
                observed = [s["cpu"] if is_native else s["affinity"][0] for s in shards]
                assert observed == reactor_cpus, (observed, reactor_cpus)
            if not is_native:
                expected_loop = "SelectorEventLoop" if mode.endswith("asyncio") else "uvloop.Loop"
                assert all(expected_loop in w["event_loop"] for w in ready["workers"])
            endpoint = "native" if is_native else "policy" if is_policy else "hybrid"
            path = f"/{endpoint}/{args.workload}"
            if args.workload == "fixed": path += "?size=1024"
            client_cmd = ["taskset", "-c", ",".join(map(str, client_cpus)), str(ROOT/"build/http_load"),
                "--smp", str(args.client_cores), "--cpuset", ",".join(map(str, client_cpus)),
                "--thread-affinity", "1", "--reactor-backend", "epoll", "--overprovisioned",
                "--default-log-level", "warn", "--enable-glibc-exception-scaling-workaround", "0",
                "--port", str(port), "--target-pids", ",".join(map(str, ready["pids"])), "--path", path,
                "--method", "GET", "--expected", str(out/"expected.bin"), "--concurrency", str(args.connections),
                "--rate", "0", "--warmup", str(args.warmup), "--seconds", str(args.seconds)]
            if is_policy:
                client_cmd += ["--policy-version", str(ready["workers"][0]["policy_version"])]
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
            row = dict(key=key, mode=mode, round=args.round, commands=[server_cmd, client_cmd],
                server_cpus=server_cpus, reactor_cpus=reactor_cpus, client_cpus=client_cpus, warmup=args.warmup, seconds=args.seconds,
                concurrency=args.connections, workload=args.workload, ready=ready, client=c, server=result, closed=closed,
                validated=False, returncodes=rc, server_cpu_cores=cpu, threads=after_threads,
                cgroup_delta={k:v-cg0[k] for k,v in cg1.items()}, memory_samples=samples,
                max_sampled_pss_mib=max(s["pss_bytes"] for s in samples)/1048576)
            atomic_json(out/(key+".json"), row)
            measured = sum(w["delta"]["responses"] for w in result["workers"])
            decisions = sum(w["delta"].get("python_decisions", 0) for w in result["workers"])
            assert rc == [0, 0] and c["errors"] == c["integrity_errors"] == c["dropped"] == 0
            assert c["requests"] == c["scheduled"] == measured == sum(c["server_pids"].values())
            assert c["requests"] > 0 and c["response_bytes"] == (out/"expected.bin").stat().st_size*c["requests"]
            assert c["new_connections"] == 0 and c["total_connections"] == args.connections
            assert decisions == (0 if is_native or is_policy else c["requests"])
            policy_count = sum(w["delta"].get("native_policy_decisions", 0) for w in result["workers"])
            assert policy_count == (c["requests"] if is_policy else 0)
            assert all(w["delta"].get("native_policy_denied", 0) == 0 for w in result["workers"])
            totals = {k:sum(w["delta"].get(k, 0) for w in result["workers"]) for k in
                      ("decision_return_batches", "decision_return_items", "decision_return_posts")}
            if mode.startswith("seastar") and not is_policy:
                assert totals["decision_return_items"] == decisions
                assert 0 < totals["decision_return_batches"] <= totals["decision_return_posts"] < decisions
                assert totals["decision_return_posts"] <= len(reactor_cpus)*totals["decision_return_batches"]
            else:
                assert all(v == 0 for v in totals.values())
            assert closed["closed"] and all(w.get(k, 0) == 0 for w in closed["workers"]
                for k in ("active_requests", "reserved_bytes", "pending_decisions", "queued_decisions"))
            row["validated"] = True
            atomic_json(out/(key+".json"), row)
            print(json.dumps({"mode":mode, "round":args.round, "rps":c["rps"], "p99_ms":c["p99_us"]/1000,
                "server_cpu":cpu, "client_cpu":c["cpu_cores"], "errors":c["errors"], "pss_mib":row["max_sampled_pss_mib"]}), flush=True)
    finally:
        stop_process(client); stop_process(server)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--round", type=int, default=0)
    parser.add_argument("--warmup", type=float, default=3)
    parser.add_argument("--seconds", type=float, default=10)
    parser.add_argument("--connections", type=int, default=128)
    parser.add_argument("--client-cores", type=int, default=5, choices=[4,5])
    parser.add_argument("--modes", nargs="+", choices=MODES, default=MODES)
    parser.add_argument("--out", required=True)
    parser.add_argument("--workload", choices=["fixed", "file"], default="fixed")
    args = parser.parse_args()
    available = sorted(os.sched_getaffinity(0))
    assert len(available) >= 3 + args.client_cores + 1, "need a separate controller CPU"
    os.sched_setaffinity(0, {available[3 + args.client_cores]})
    out = ROOT/"results"/args.out; out.mkdir(parents=True, exist_ok=True)
    (out/"expected.bin").write_bytes(b"x"*1024 if args.workload == "fixed" else bytes(range(256))*256)
    manifest = out/"manifest.json"
    if not manifest.exists():
        paths = [ROOT/"build/http_load", ROOT/"build/fixed_native", *list((ROOT/"build").glob("_seastar_probe*.so")),
                 *list((ROOT/"probes/fixed").glob("*.py")), *list((ROOT/"probes/final").glob("*.py")), ROOT/"probes/fixed/native_main.cc",
                 *list((ROOT/"native").glob("*.cc")), *list((ROOT/"native").glob("*.hh")),
                 ROOT/"tests/test_batch_decisions.py", ROOT/"scripts/comparison_server.py", ROOT/"python/probe_engine.py"]
        atomic_json(manifest, dict(created=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            python=sys.version, kernel=platform.release(), cpu_max=Path("/sys/fs/cgroup/cpu.max").read_text().strip(),
            parent_commit=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
            versions={p:importlib.metadata.version(p) for p in ("aiohttp", "uvloop")},
            fingerprints={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths},
            native_libraries=native_libraries(), server_budget_cpus=available[:3], workload=args.workload, payload_bytes=(out/"expected.bin").stat().st_size, protocol="HTTP/1.1 GET; keepalive; no pipeline"))
    current = json.loads(manifest.read_text())
    assert current["workload"] == args.workload
    for relative, expected in current["fingerprints"].items():
        assert hashlib.sha256((ROOT/relative).read_bytes()).hexdigest() == expected, relative
    modes = args.modes if args.round%2 == 0 else list(reversed(args.modes))
    for mode in modes: one(args, mode, out, available)
