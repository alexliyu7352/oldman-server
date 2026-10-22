"""Run retirement regressions while preserving earlier protocol measurements."""
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[1]
out = root / "results/io-retirement-final-correctness"
out.mkdir(parents=True, exist_ok=True)
paths = [root / "results/phase3-tcp.json", root / "results/phase3-udp.json"]
before = {p: p.read_bytes() for p in paths if p.exists()}
try:
    with (out / "runner.log").open("w") as log:
        process = subprocess.Popen(
            [sys.executable, "scripts/run_tests.py", "--out", str(out)],
            cwd=root, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in process.stdout:
            log.write(line)
            log.flush()
            print(line, end="", flush=True)
        code = process.wait()
finally:
    for path in paths:
        if path.exists():
            (out / path.name).write_bytes(path.read_bytes())
        if path in before:
            path.write_bytes(before[path])
        elif path.exists():
            path.unlink()
raise SystemExit(code)
