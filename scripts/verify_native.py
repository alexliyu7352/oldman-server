#!/usr/bin/env python3
"""Run current correctness gates without reusing a Seastar process or old results."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import sysconfig
import time


ROOT = Path(__file__).resolve().parents[1]
CODEC = "tests/test_native_response_codec.py"
GATES = [CODEC, "tests/test_native_response_framework.py", "tests/test_framework.py",
         "tests/test_native_boundary.py", "tests/test_shard_handoff.py", "tests/test_python314_compat.py"]


def verify(root, out, files, evidence=None):
    out.mkdir(parents=True, exist_ok=False)
    report = {"completed": False, "planned_cases": 0, "rows": [], "inputs": evidence or {}}
    collection = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", *files],
        cwd=root, capture_output=True, text=True, timeout=60)
    (out / "collection.log").write_text(collection.stdout + collection.stderr)
    selectors = []
    for line in collection.stdout.splitlines():
        if "::" in line and line.split("::", 1)[0] in files:
            # Pure codec tests can share a process; every lifecycle node id,
            # including each parametrized case, must get a fresh interpreter.
            selector = CODEC if line.startswith(CODEC + "::") else line
            if selector not in selectors:
                selectors.append(selector)
    report["collection_returncode"] = collection.returncode
    report["planned_cases"] = len(selectors)
    if collection.returncode == 0 and selectors:
        for number, selector in enumerate(selectors):
            started = time.monotonic()
            try:
                result = subprocess.run([sys.executable, "-m", "pytest", "-q", selector],
                                        cwd=root, capture_output=True, text=True, timeout=60)
                code, output = result.returncode, result.stdout + result.stderr
            except subprocess.TimeoutExpired as error:
                code = 124
                output = (error.stdout or b"").decode(errors="replace")
                output += (error.stderr or b"").decode(errors="replace") + "\nTIMEOUT\n"
            log = f"{number:03d}.log"
            (out / log).write_text(output)
            row = {"selector": selector, "returncode": code,
                   "seconds": time.monotonic() - started, "log": log}
            report["rows"].append(row)
            # Preserve completed evidence even if the controller is interrupted.
            (out / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
            print(json.dumps(row), flush=True)
            if code:
                print(output[-7000:], flush=True)
                break
        report["completed"] = (len(report["rows"]) == len(selectors)
                               and all(row["returncode"] == 0 for row in report["rows"]))
    (out / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
    return 0 if report["completed"] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True, help="New output directory; never overwritten")
    args = parser.parse_args()
    extensions = list((ROOT / "build").glob("_seastar_probe*" + sysconfig.get_config_var("EXT_SUFFIX")))
    if len(extensions) != 1:
        parser.error("build exactly one native extension for this interpreter before verification")
    paths = [*extensions, ROOT / "CMakeLists.txt", ROOT / "dependency-lock.json", ROOT / "uv.lock",
             *sorted((ROOT / "native").glob("*")), *sorted((ROOT / "python").glob("*.py")),
             *sorted((ROOT / "tests").rglob("*.py"))]
    evidence = {
        "python": sys.version, "executable": sys.executable,
        "soabi": sysconfig.get_config_var("SOABI"),
        "git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "tracked_diff_sha256": hashlib.sha256(subprocess.check_output(["git", "diff", "HEAD"], cwd=ROOT)).hexdigest(),
        "sha256": {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                   for path in paths if path.is_file()},
    }
    return verify(ROOT, args.out.resolve(), GATES, evidence)


if __name__ == "__main__":
    raise SystemExit(main())
