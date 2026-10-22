"""Exercise the diagnostic worker protocol and frozen imports without a benchmark."""
import argparse
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from probes.application_lifecycle.inputs import library_environment, verify_loaded, verify_snapshot
from probes.drain_allocation.observer import validate
from probes.drain_allocation.run import server_command
from probes.framework_json.run import choose_port, cpu_layout, preflight
from scripts.cpu_budget import cpu_topology, describe_cpu, require_budget
from probes.final.run import read_json, send, stop_process

parser = argparse.ArgumentParser()
parser.add_argument("--snapshot", type=Path, required=True)
parser.add_argument("--role", required=True)
parser.add_argument("--out", type=Path, required=True)
args = parser.parse_args()
args.out.mkdir(parents=True, exist_ok=False)
snapshot = args.snapshot.resolve()
record = verify_snapshot(snapshot)
budget = describe_cpu()
require_budget(budget, 8)
layout = cpu_layout(budget["effective_cpus"], cpu_topology(budget["effective_cpus"]))
port = choose_port()
trace = (args.out / "trace.json").resolve()
command = server_command(snapshot, args.role, port, layout["candidate3"],
    library=ROOT / "build/drain_trace.so", trace_out=trace, library_env=library_environment(record))
with (args.out / "server.log").open("xb") as log:
    child = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log,
                             bufsize=0, start_new_session=True)
    try:
        ready = read_json(child)
        (args.out / "ready.json").write_text(json.dumps(ready, indent=2)+"\n")
        verify_loaded(ready["workers"][0]["loaded_modules"], snapshot, record)
        send(child, "begin")
        assert read_json(child)["begun"]
        preflight(port, ready["pids"])
        send(child, "end")
        ended = read_json(child)
        send(child, "quit")
        closed = read_json(child)
        assert child.wait(10) == 0 and closed["closed"]
        for worker in closed["workers"]:
            assert all(worker[key] == 0 for key in ("active_requests", "reserved_bytes", "pending_decisions", "queued_decisions"))
        observed = validate(json.loads(trace.read_text()), role=args.role)
        assert sum(r["size"] for r in observed) >= 2
        (args.out / "verified.json").write_text(json.dumps({"command": command, "ended": ended,
            "closed": closed, "observed_drains": len(observed), "valid": True}, indent=2)+"\n")
    finally:
        stop_process(child)
