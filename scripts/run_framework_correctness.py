"""Isolated framework tests and optional legacy gate, preserving old measurements."""
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
    parser.add_argument("--legacy", action="store_true")
    parser.add_argument("--sanic", action="store_true", help="include Sanic integration (requires the sanic uv group)")
    parser.add_argument("--out", type=Path, default=ROOT / "build/framework-correctness")
    args = parser.parse_args()
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    rows = []
    files = ["test_framework.py", "test_framework_native.py", "test_native_boundary.py",
             "test_framework_benchmark.py", "test_queue_handoff.py", "test_shard_handoff.py",
             "test_framework_dispatch.py", "test_native_request_metadata.py", "test_engine_application_tasks.py",
             "test_native_response_codec.py", "test_native_runtime_lifecycle.py"]
    if args.sanic:
        files.extend(["test_sanic_native.py", "test_sanic_touchup.py",
                      "test_sanic_request.py", "test_sanic_response.py", "test_sanic_flat.py", "test_sanic_http1.py", "test_sanic_native_run.py",
                      "test_sanic_websocket.py", "test_sanic_files.py", "test_sanic_multipart.py",
                      "test_sanic_native_workers.py"])
    for filename in files:
        tree = ast.parse((ROOT / "tests" / filename).read_text())
        names = [node.name for node in tree.body
                 if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_")]
        cases = [(name, f"tests/{filename}::{name}") for name in names]
        if filename == "test_framework_benchmark.py":
            cases = [("benchmark_contract", f"tests/{filename}")]
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
    if args.legacy:
        paths = [ROOT / "build/evidence/tcp-frames.json", ROOT / "build/evidence/udp-flows.json"]
        legacy = out / "legacy"
        legacy.mkdir(exist_ok=True)
        try:
            command = [sys.executable, "scripts/run_tests.py", "--out", str(legacy)]
            with (legacy / "runner.log").open("w") as log:
                process = subprocess.Popen(command, cwd=ROOT, text=True,
                                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                for line in process.stdout:
                    log.write(line)
                    log.flush()
                    print(line, end="", flush=True)
                code = process.wait()
            rows.append({"test": "legacy_isolated_suite", "returncode": code, "command": command})
        finally:
            for path in paths:
                if path.exists():
                    (legacy / path.name).write_bytes(path.read_bytes())
    (out / "correctness.json").write_text(json.dumps(rows, indent=2) + "\n")
    return int(any(row["returncode"] for row in rows))


if __name__ == "__main__":
    raise SystemExit(main())
