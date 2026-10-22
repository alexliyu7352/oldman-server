"""Each lifecycle test owns one fresh single-process service instance."""
import ast
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument("--out", type=Path, default=root / "build/tests")
output_dir = parser.parse_args().out
output_dir.mkdir(parents=True, exist_ok=True)
tests = []
for filename in ("test_bridge.py", "test_work.py", "test_http.py", "test_async_http.py", "test_async_fetch.py", "test_async_fetch_framing.py", "test_http_retirement.py", "test_batch_decisions.py", "test_notifications.py", "test_native_policy.py", "test_protocols.py", "test_http_load.py", "test_benchmark_results.py", "test_comparison.py"):
    tree = ast.parse((root / "tests" / filename).read_text())
    tests.extend((filename, n.name) for n in tree.body
                 if isinstance(n, (ast.AsyncFunctionDef, ast.FunctionDef)) and n.name.startswith("test_"))
results = []
for filename, name in tests:
    start = time.monotonic()
    command = [sys.executable, "-m", "pytest", "-q", f"tests/{filename}::{name}"]
    try:
        run = subprocess.run(command, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=45)
        output, code = run.stdout, run.returncode
    except subprocess.TimeoutExpired as e:
        output, code = (e.stdout or b"").decode(errors="replace") + "\nTIMEOUT\n", 124
    (output_dir / f"{name}.log").write_text(output)
    row = {"test": name, "returncode": code, "seconds": time.monotonic() - start}
    results.append(row)
    print(json.dumps(row), flush=True)
    if code:
        print(output[-6000:], flush=True)
# Build-lock regressions do not create a runtime and can share a process.
run = subprocess.run([sys.executable, "-m", "pytest", "-q", "tests/test_bootstrap_lock.py"],
                     cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=60)
(output_dir / "bootstrap-lock-tests.log").write_text(run.stdout)
results.append({"test": "bootstrap_lock_3_cases", "returncode": run.returncode})
print(run.stdout, flush=True)
(output_dir / "correctness.json").write_text(json.dumps(results, indent=2) + "\n")
raise SystemExit(any(x["returncode"] for x in results))
