"""The native host tests, each test function in a process of its own (one native runtime per process)."""
import argparse
import ast
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sanic", action="store_true", help="include Sanic integration (requires the sanic uv group)")
    parser.add_argument("--out", type=Path, default=ROOT / "build/framework-correctness")
    args = parser.parse_args()
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    rows = []
    files = ["test_native_boundary.py", "test_framework_dispatch.py", "test_native_request_metadata.py",
             "test_engine_application_tasks.py", "test_native_runtime_lifecycle.py", "test_bridge.py",
             "test_http.py", "test_notifications.py", "test_python314_compat.py"]
    if args.sanic:
        files.extend(["test_sanic_native.py", "test_sanic_touchup.py",
                      "test_sanic_request.py", "test_sanic_response.py", "test_sanic_flat.py", "test_sanic_native_calls.py", "test_sanic_http1.py", "test_sanic_native_run.py",
                      "test_sanic_websocket.py", "test_sanic_files.py", "test_sanic_multipart.py",
                      "test_sanic_native_workers.py", "test_native_fixed_cache.py", "test_native_cors.py",
                      "test_native_inline.py"])
    for filename in files:
        tree = ast.parse((ROOT / "tests" / filename).read_text())
        names = [node.name for node in tree.body
                 if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_")]
        cases = [(name, f"tests/{filename}::{name}") for name in names]
        for name, selector in cases:
            started = time.monotonic()
            command = [sys.executable, "-m", "pytest", "-q", selector]
            try:
                result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=45)
                code, output = result.returncode, result.stdout + result.stderr
            except subprocess.TimeoutExpired as exc:
                code = 124
                output = (exc.stdout or b"").decode(errors="replace") + "\nTIMEOUT\n"
            (out / f"{name}.log").write_text(output)
            row = {"test": selector, "returncode": code,
                   "seconds": time.monotonic() - started, "command": command}
            rows.append(row)
            print(json.dumps(row), flush=True)
            if code:
                print(output[-6000:], flush=True)
    (out / "correctness.json").write_text(json.dumps(rows, indent=2) + "\n")
    return int(any(row["returncode"] for row in rows))


if __name__ == "__main__":
    raise SystemExit(main())
