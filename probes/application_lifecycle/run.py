"""One explicit frozen-input lifecycle measurement per invocation."""
import argparse
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from probes.framework_json import run as framework
from probes.application_lifecycle import host
from probes.application_lifecycle.inputs import (check_identity, fingerprint, harness_fingerprint,
    library_environment, process_identity, runtime_fingerprint, verify_loaded,
    verify_seastar_libraries, verify_snapshot)
from scripts.cpu_budget import describe_cpu, cpu_topology, require_budget

LIFECYCLE_ROLES = ("baseline", "predecessor", "candidate")
ROLE_MODES = {**dict.fromkeys(LIFECYCLE_ROLES, "candidate3"),
              "uvloop-stdlib": "multiprocess", "uvloop-codec": "multiprocess", "native": "native3"}
NATIVE_COUNTERS = ("native_response_json_encoded", "native_response_json_fallback",
                   "native_response_queued", "native_response_flushed")


def server_command(snapshot, role, port, cpus, *, profile=None, library_env=None):
    affinity = ",".join(map(str, cpus))
    prefix = ["taskset", "-c", affinity]
    if library_env:
        prefix += ["env", *(f"{key}={value}" for key, value in library_env.items())]
    if role == "native":
        return prefix + [str(snapshot / "build/framework_native"), str(port), affinity]
    # libuv 1.48 SQPOLL helpers can escape taskset and cannot be repinned.
    # Apply the same local loop configuration to every Python comparison.
    command = prefix + ["env", "UV_USE_IO_URING=0", sys.executable, "-P",
                        str(Path(__file__).with_name("server.py")),
                        "--snapshot", str(snapshot), "--mode", ROLE_MODES[role], "--port", str(port)]
    if role in LIFECYCLE_ROLES:
        command += ["--native-responses"]
    else:
        command += ["--json-codec", "native" if role == "uvloop-codec" else "stdlib"]
    if profile is not None:
        command += ["--profile", str(profile)]
    return command


def validate_experiment(row):
    framework.validate_measurement(row, row["concurrency"])
    if "host_observer" in row.get("comparison_inputs", {}):
        if row["comparison_inputs"]["host_observer"] != host.CONFIG:
            raise ValueError("unknown host observer configuration")
        samples = row.get("host_samples", [])
        if not samples or "host_window" not in row:
            raise ValueError("missing declared host observations")
        host.validate_window(samples, row["server_cpus"], row["host_window"])
    role = row["role"]
    if role not in ROLE_MODES or row["mode"] != ROLE_MODES[role]:
        raise ValueError("incorrect experiment role/mode")
    if (row["warmup"], row["seconds"], row["concurrency"]) != (3, 10, 256):
        raise ValueError("experiment workload changed")
    if any(not math.isfinite(row["client"][key]) for key in ("rps", "p99_us")):
        raise ValueError("invalid performance value")
    for key in ("server_cpu_per_request_us", "python_main_cpu_per_request_us"):
        if role == "native" and key.startswith("python"):
            continue
        value = row.get(key)
        if not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise ValueError(f"missing/invalid CPU cost: {key}")
    for worker in row["ready"]["workers"]:
        if role in LIFECYCLE_ROLES and worker.get("native_responses") is not True:
            raise ValueError("native-response opt-in missing")
        if role.startswith("uvloop-") and worker.get("json_codec") != (
                "native" if role == "uvloop-codec" else "stdlib"):
            raise ValueError("incorrect reference JSON codec")
    if role in LIFECYCLE_ROLES:
        for worker in row["server"]["workers"]:
            for key in NATIVE_COUNTERS:
                expected = 0 if key.endswith("json_fallback") else worker["delta"]["responses"]
                if worker["delta"].get(key) != expected:
                    raise ValueError(f"incorrect/missing {key}")


def _cpu_set(text):
    result = set()
    for part in text.split(","):
        ends = [int(number) for number in part.split("-")]
        result.update(range(ends[0], ends[-1] + 1))
    return result


def execute(snapshot, role, round_number, out, *, baseline_index, profiler=None, observe_host=False):
    """Run a single fixed workload; diagnostic callers cannot create formal rows."""
    out, snapshot = Path(out).resolve(), Path(snapshot).resolve()
    out.mkdir(parents=True, exist_ok=False)
    original_affinity = os.sched_getaffinity(0)
    perf = perf_log = None
    switches = None
    row = None
    host_samples = []
    try:
        if profiler in ("switches", "switches-shards", "cpu-shards") and not observe_host:
            raise ValueError("switch attribution requires host window anchors")
        record = verify_snapshot(snapshot)
        baseline_path = Path(json.loads(Path(baseline_index).read_text())["snapshot"])
        baseline = verify_snapshot(baseline_path)
        if json.loads(Path(baseline_index).read_text()) != baseline:
            raise ValueError("baseline manifest changed since freeze")
        if role == "baseline" and record != baseline:
            raise ValueError("baseline role requires the frozen migration snapshot")
        client = baseline_path / "build/http_load"
        server_library_env = library_environment(record)
        client_library_env = library_environment(baseline)
        budget = describe_cpu()
        require_budget(budget, 8)
        topology = cpu_topology(budget["effective_cpus"])
        layout = framework.cpu_layout(budget["effective_cpus"], topology)
        harness, runtime = harness_fingerprint(ROOT), runtime_fingerprint()
        common = {"client": fingerprint(client), "runtime": runtime, "harness": harness,
                  "cpu_budget": budget, "cpu_topology": topology, "cpu_layout": layout,
                  "loop_environment": {"UV_USE_IO_URING": "0"}}
        if observe_host:
            common["host_observer"] = host.CONFIG
        manifest = {"role": role, "round": round_number, "snapshot": record,
                    "server_library_environment": server_library_env,
                    "client_library_environment": client_library_env,
                    "diagnostic_only": profiler is not None, "profiler": profiler,
                    "comparison_inputs": common, "created_unix_ns": time.time_ns(),
                    "workload": {"path": "/json", "body": framework.EXPECTED.decode(),
                                 "warmup": 3, "seconds": 10, "concurrency": 256}}
        framework.atomic_json(out / "manifest.json", manifest)
        (out / "expected.bin").write_bytes(framework.EXPECTED)
        os.sched_setaffinity(0, {layout["controller"]})
        # Import these only after pure CLI/output validation; no native lifecycle
        # is started in the controller, and none of its modules serves requests.
        from probes.final.run import process_sample, threads, stop_process
        identities = {}
        excluded_pids = {os.getpid()}

        def ready(server, load, info):
            nonlocal perf, perf_log, switches
            executable = snapshot / "build/framework_native" if role == "native" else Path(sys.executable)
            identities[server.pid] = process_identity(server.pid, os.getpid(), executable)
            expected_service = set(info["pids"]) | {server.pid}
            excluded_pids.update(expected_service | {load.pid})
            if set(info["service_pids"]) != expected_service:
                raise ValueError("service CPU accounting omitted a process")
            for worker, pid in zip(info["workers"], info["pids"], strict=True):
                parent = server.pid if ROLE_MODES[role] == "multiprocess" else os.getpid()
                identities[pid] = process_identity(pid, parent, executable)
                worker["loaded_seastar_verified"] = verify_seastar_libraries(pid, record)
                if role != "native":
                    if worker["pid"] != pid or worker["procfs_pid"] != pid:
                        raise ValueError("worker identity mismatch")
                    verify_loaded(worker["loaded_modules"], snapshot, record)
                    if worker["runtime_state"] != runtime["state"]:
                        raise ValueError("worker interpreter/GIL/JIT/runtime mismatch")
                    if worker["loop_environment"] != common["loop_environment"]:
                        raise ValueError("worker loop configuration mismatch")
                worker["process_identity"] = identities[pid]
            info["service_threads"] = threads(info["service_pids"])
            if any(not _cpu_set(t["affinity"]) <= set(layout[ROLE_MODES[role]])
                   for t in info["service_threads"].values()):
                raise ValueError("server thread escaped its CPU budget")
            info["client_identity"] = process_identity(load.pid, os.getpid(), client)
            info["client_loaded_seastar"] = verify_seastar_libraries(load.pid, baseline)
            info["client_threads"] = threads([load.pid])
            if any(not _cpu_set(t["affinity"]) <= set(layout["client"])
                   for t in info["client_threads"].values()):
                raise ValueError("client thread escaped its CPU budget")
            if profiler == "perf":
                perf_log = (out / "perf.log").open("wb")
                command = ["perf", "record", "-e", "cpu-clock:u", "-F", "199", "-g",
                           "--call-graph", "dwarf,8192", "-p", str(info["pids"][0]),
                           "-o", str(out / "perf.data"), "--", "sleep", "10"]
                framework.atomic_json(out / "perf-command.json", command)
                perf = subprocess.Popen(command, stdout=perf_log, stderr=perf_log, start_new_session=True)
            elif profiler in ("switches", "switches-shards", "cpu-shards"):
                if role not in LIFECYCLE_ROLES or info["pids"] != [server.pid]:
                    raise ValueError("switch attribution requires one frozen Python service")
                from probes.blocking_attribution.record import Recorder
                extra_tids = ()
                if profiler != "switches":
                    extra_tids = tuple(shard["tid"] for shard in info["workers"][0]["shards"])
                    for tid in extra_tids:
                        status = Path(f"/proc/{server.pid}/task/{tid}/status").read_text()
                        if int(next(line.split()[1] for line in status.splitlines()
                                    if line.startswith("Tgid:"))) != server.pid:
                            raise ValueError("reactor belongs to another process")
                switches = Recorder(server.pid, out, extra_tids=extra_tids,
                                    event="cpu" if profiler == "cpu-shards" else "switches")

        def sample(pids):
            for pid in pids:
                check_identity(identities[pid])
            return process_sample(pids)

        def observe(pids):
            for pid in pids:
                check_identity(identities[pid])
            result = host.sample(pids, excluded_pids, budget["effective_cpus"],
                                 frequency_cpus=layout[ROLE_MODES[role]])
            host_samples.append(result)
            return result

        args = SimpleNamespace(round=round_number, warmup=3, seconds=10, connections=256)
        started = time.time_ns()
        row = framework.one(args, ROLE_MODES[role], out, layout, cpu_budget=budget,
            make_server_command=lambda port, cpus: server_command(snapshot, role, port, cpus,
                profile=out if profiler == "cprofile" else None, library_env=server_library_env),
            client_binary=client, client_environment=os.environ | client_library_env,
            on_ready=ready, sample_processes=sample,
            observe_processes=observe if observe_host else None)
        row.update(role=role, diagnostic_only=profiler is not None, profiler=profiler,
                   experiment_validated=False, comparison_inputs=common,
                   source_commit=record["source_commit"], snapshot=str(snapshot),
                   snapshot_manifest=fingerprint(snapshot / "SNAPSHOT.json"),
                   process_identities=list(identities.values()),
                   started_unix_ns=started, finished_unix_ns=time.time_ns())
        path = out / (row["key"] + ".json")
        framework.atomic_json(path, row)
        if verify_snapshot(snapshot) != record or verify_snapshot(baseline_path) != baseline:
            raise ValueError("snapshot changed during measurement")
        if harness_fingerprint(ROOT) != harness or runtime_fingerprint() != runtime:
            raise ValueError("harness/runtime inputs changed during measurement")
        for task in row["threads"].values():
            if not _cpu_set(task["affinity"]) <= set(row["server_cpus"]):
                raise ValueError("server thread escaped its CPU budget")
        main_cpu = 0
        if role != "native":
            for worker in row["server"]["workers"]:
                main = next(t for t in worker["threads"] if t["tid"] == worker["pid"])
                main_cpu += main["cpu_seconds"]
                if abs(sum(t["cpu_seconds"] for t in worker["threads"]) - worker["cpu_seconds"]) > .15:
                    raise ValueError("thread/process CPU crosscheck failed")
        row["python_main_cpu_seconds"] = main_cpu if role != "native" else None
        row["python_main_cpu_per_request_us"] = main_cpu * 1e6 / row["client"]["requests"] if role != "native" else None
        validate_experiment(row)
        if switches is not None:
            row["perf_control"] = switches.finish()
            if not (row["perf_control"]["enabled_ack_ns"] < row["host_window"]["begin_sent_ns"] <
                    row["host_window"]["end_received_ns"] < row["perf_control"]["finish_requested_ns"]):
                raise ValueError("perf capture did not bracket service window")
            row["perf_data"] = fingerprint(out / "perf.data")
        if perf is not None:
            if perf.wait(5) != 0:
                raise ValueError("perf recording failed; inspect perf.log")
            row["perf_data"] = fingerprint(out / "perf.data")
        row["experiment_validated"] = True
        framework.atomic_json(path, row)
        return row
    except Exception as error:
        if observe_host:
            if isinstance(error, host.ObservationError):
                host_samples.append(error.observation)
            framework.atomic_json(out / "host-observations.json", host_samples)
        framework.atomic_json(out / "failure.json", {"error": str(error), "type": type(error).__name__})
        if row is not None:
            row["experiment_validated"] = False
            row["experiment_error"] = str(error)
            framework.atomic_json(out / (row["key"] + ".json"), row)
        raise
    finally:
        if switches is not None:
            switches.close()
        if perf is not None:
            stop_process(perf)
        if perf_log is not None:
            perf_log.close()
        os.sched_setaffinity(0, original_affinity)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--role", choices=ROLE_MODES, required=True)
    parser.add_argument("--round", type=int, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--baseline-index", type=Path, required=True,
                        help="snapshot.json of the frozen baseline snapshot")
    parser.add_argument("--observe-host", action="store_true", help="record external controller observations")
    args = parser.parse_args(argv)
    if args.round < 0:
        parser.error("round must be nonnegative")
    execute(args.snapshot, args.role, args.round, args.out, observe_host=args.observe_host,
            baseline_index=args.baseline_index)


if __name__ == "__main__":
    main()
