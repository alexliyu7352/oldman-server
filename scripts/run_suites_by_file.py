"""The framework and router suites (tests/framework, tests/routing) one test
file per process, each with a time limit, for the compatible wheel on macOS
and Windows: a hung test there ends its whole process (pytest-timeout's
thread method on Windows), so the other files still report. Prints each
file's result (and the end of the output of one that did not pass), then the
failed tests and the files that timed out or crashed; exits non-zero if any."""
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    files = sorted([*(ROOT / "tests/framework").rglob("test_*.py"), *(ROOT / "tests/routing").rglob("test_*.py")])
    failed, broken = [], []
    for path in files:
        name = path.relative_to(ROOT).as_posix()
        started = time.monotonic()
        try:
            result = subprocess.run(
                [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-rfE", name],
                cwd=ROOT, capture_output=True, text=True, timeout=900)
            output, code = result.stdout + result.stderr, result.returncode
        except subprocess.TimeoutExpired as expired:
            output = (expired.stdout or b"").decode(errors="replace") if isinstance(expired.stdout, bytes) \
                else (expired.stdout or "")
            code = "timeout"
        lines = output.splitlines()
        summary = next((line for line in reversed(lines)
                        if re.search(r"\b(passed|failed|errors?|skipped|deselected|no tests ran)\b", line)), "")
        print(f"{name}: {summary.strip('= ')} [{code}, {time.monotonic() - started:.0f} s]", flush=True)
        if code in (0, 5):  # 5: nothing collected
            continue
        ids = [line.split(" ", 1)[1] for line in lines if line.startswith(("FAILED ", "ERROR "))]
        if ids:
            failed += ids
        else:
            broken.append(name)
        print(output[-8000:], flush=True)
    if failed:
        print(f"\n{len(failed)} failed:")
        for test in failed:
            print("  " + test)
    if broken:
        print(f"\n{len(broken)} files timed out or crashed: " + ", ".join(broken))
    return 1 if failed or broken else 0


if __name__ == "__main__":
    raise SystemExit(main())
