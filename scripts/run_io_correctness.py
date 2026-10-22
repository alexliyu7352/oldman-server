"""Run current lifecycle tests without replacing historical TCP/UDP measurements."""
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[1]
out = root/"results/io-final-correctness"
out.mkdir(parents=True, exist_ok=True)
paths = [root/"results/phase3-tcp.json", root/"results/phase3-udp.json"]
before = {p:p.read_bytes() for p in paths if p.exists()}
try:
    with (out/"runner.log").open("w") as log:
        process = subprocess.Popen([sys.executable, "scripts/run_tests.py", "--out", str(out)],
            cwd=root, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in process.stdout:
            log.write(line)
            log.flush()
            print(line, end="", flush=True)
        code = process.wait()
finally:
    for p in paths:
        if p.exists(): (out/p.name).write_bytes(p.read_bytes())
        if p in before: p.write_bytes(before[p])
        elif p.exists(): p.unlink()
raise SystemExit(code)
