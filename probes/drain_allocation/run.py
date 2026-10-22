"""Run the fixed allocation diagnostic separately from formal measurements."""
import argparse
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from probes.application_lifecycle import run as lifecycle
from probes.application_lifecycle.inputs import fingerprint, verify_snapshot
from probes.drain_allocation.observer import validate
from probes.framework_json.run import atomic_json


def server_command(snapshot, role, port, cpus, *, library, trace_out, profile=None, library_env=None):
    command = original_command(snapshot, role, port, cpus, profile=profile,
                               library_env=(library_env or {}) | {"LD_PRELOAD": str(library)})
    index = command.index(str(ROOT / "probes/application_lifecycle/server.py"))
    command[index] = str(Path(__file__).with_name("server.py"))
    return command + ["--observer-library", str(library), "--observer-out", str(trace_out)]


original_command = lifecycle.server_command


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, required=True, help="directory containing the committed sequence.json")
    parser.add_argument("--baseline-index", type=Path, required=True,
                        help="snapshot.json of the frozen baseline snapshot")
    args = parser.parse_args(argv)
    out = args.out.resolve()
    sequence = json.loads((out / "sequence.json").read_text())
    for number, role in enumerate(sequence["order"]):
        folder = out / f"r{number:02d}-{role}"
        if folder.exists():
            raise FileExistsError(folder)
    if os.environ.get("LD_PRELOAD"):
        raise ValueError("controller/client must not inherit a preload")
    for path, digest in sequence["observer_inputs"].items():
        if fingerprint(path)["sha256"] != digest:
            raise ValueError(f"diagnostic input changed: {path}")
    for binding in sequence["snapshots"].values():
        record = verify_snapshot(binding["path"])
        if (record["source_commit"] != binding["source_commit"] or
                fingerprint(Path(binding["path"]) / "SNAPSHOT.json")["sha256"] != binding["manifest_sha256"]):
            raise ValueError("frozen input changed")
    library = ROOT / "build/drain_trace.so"
    for number, role in enumerate(sequence["order"]):
        folder = out / f"r{number:02d}-{role}"
        trace = folder / "drain-trace.json"
        def command(*positional, **kwargs):
            return server_command(*positional, **kwargs, library=library, trace_out=trace)
        lifecycle.server_command = command
        print(f"START {number} {role}", flush=True)
        try:
            row = lifecycle.execute(sequence["snapshots"][role]["path"], role, number, folder,
                                    profiler="allocation", baseline_index=args.baseline_index)
            observed = json.loads(trace.read_text())
            records = validate(observed, role=role)
            if row["diagnostic_only"] is not True or sequence["declared_unix_ns"] >= row["started_unix_ns"]:
                raise ValueError("undeclared or non-diagnostic row")
            for path, digest in sequence["observer_inputs"].items():
                if fingerprint(path)["sha256"] != digest:
                    raise ValueError(f"diagnostic input changed: {path}")
            atomic_json(folder / "trace-validation.json", {"diagnostic_only": True, "trace": fingerprint(trace),
                "drains": len(records), "events": sum(r["size"] for r in records),
                "observer_inputs": sequence["observer_inputs"], "valid": True})
        except Exception as error:
            folder.mkdir(parents=True, exist_ok=True)
            failure = folder / "trace-failure.json"
            if not failure.exists():
                atomic_json(failure, {"error": str(error), "type": type(error).__name__})
            raise
        finally:
            lifecycle.server_command = original_command
        print(f"DONE {number} {role}", flush=True)


if __name__ == "__main__":
    main()
