"""Run measured processes one at a time. No simultaneous builds or load generators."""
import json
from pathlib import Path
import subprocess
import sys
import time

root = Path(__file__).resolve().parents[1]
target = root / "results/phase2"
target.mkdir(exist_ok=True)
configs = [("direct", 1)]
configs += [(mode, n) for n in (1, 2, 4, 6) for mode in ("threadpool", "seastar_pool")]
configs += [("threadpool", 7)] + [("cooperative", n) for n in (1, 2, 4, 7)]
records = []
for mode, lanes in configs:
    name = f"{mode}-{lanes}"
    start = time.time()
    with (target / f"{name}.log").open("w") as log:
        result = subprocess.run([sys.executable, "scripts/benchmark_work.py", "--mode", mode,
                                 "--lanes", str(lanes), "--output", str(target / f"{name}.json")],
                                cwd=root, stdout=log, stderr=subprocess.STDOUT, timeout=180)
    record = {"mode": mode, "lanes": lanes, "returncode": result.returncode, "wall_seconds": time.time() - start}
    records.append(record)
    print(json.dumps(record), flush=True)
    (target / "runs.json").write_text(json.dumps(records, indent=2) + "\n")
    if result.returncode:
        raise SystemExit(f"benchmark failed; inspect {target / (name + '.log')}")
