"""Run one existing real queue lifecycle in a fresh process and verify capture."""
import argparse
import importlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from probes.application_lifecycle.inputs import fingerprint, verify_loaded, verify_snapshot
from probes.drain_allocation.observer import Observer, validate

parser = argparse.ArgumentParser()
parser.add_argument("--snapshot", type=Path, required=True)
parser.add_argument("--role", choices=("predecessor", "candidate"), required=True)
parser.add_argument("--out", type=Path, required=True)
args = parser.parse_args()
args.out.mkdir(parents=True, exist_ok=False)
record = verify_snapshot(args.snapshot)
sys.path[:0] = [str(args.snapshot.resolve() / part) for part in ("python", "build")]
import _seastar_probe as native  # the frozen snapshot's extension
for name in ("probe_engine", "seastar_http"):
    importlib.import_module(name)
import pytest

observer = Observer(ROOT / "build/drain_trace.so", native.__file__, period=1)
observer.start()
code = pytest.main(["-q", str(ROOT / "tests/test_queue_handoff.py")])
report = observer.finish(args.out / "trace.json")
rows = validate(report, role=args.role)
partial = [r for r in rows if r["size"] == 3]
assert partial, "actual partial drain was not intercepted"
assert any(not r["size"] for r in rows)
assert code == 0
loaded = {name: fingerprint(sys.modules[name].__file__) for name in ("_seastar_probe", "probe_engine", "seastar_http")}
verify_loaded(loaded, args.snapshot, record)
evidence = {"snapshot": record, "observer": fingerprint(ROOT / "build/drain_trace.so"),
            "loaded_modules": loaded, "pytest_returncode": int(code),
            "observed_drains": len(rows), "partial_three": partial, "capture_validated": True}
with (args.out / "check.json").open("x") as output:
    json.dump(evidence, output, indent=2)
    output.write("\n")
