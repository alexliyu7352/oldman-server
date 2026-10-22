"""Exercise the isolation runner with real pytest subprocesses, without Seastar."""
import importlib.util
import json
from pathlib import Path

import pytest


def runner():
    path = Path(__file__).resolve().parents[1] / "scripts/verify_native.py"
    spec = importlib.util.spec_from_file_location("verify_native", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_parametrized_lifecycles_get_distinct_processes(tmp_path):
    test_file = tmp_path / "test_lifecycle.py"
    test_file.write_text('''import os
from pathlib import Path
import pytest
started = False
@pytest.mark.parametrize("case", [1, 2])
def test_once(case):
    global started
    assert not started, "a native runtime cannot be restarted in the same process"
    started = True
    with Path(__file__).with_name("pids.txt").open("a") as f:
        f.write(str(os.getpid()) + "\\n")
''')
    out = tmp_path / "verification"
    assert runner().verify(tmp_path, out, [test_file.name]) == 0
    pids = (tmp_path / "pids.txt").read_text().splitlines()
    assert len(pids) == len(set(pids)) == 2
    result = json.loads((out / "summary.json").read_text())
    assert result["completed"] and result["planned_cases"] == 2
    assert all(row["returncode"] == 0 for row in result["rows"])


def test_existing_verification_directory_is_never_overwritten(tmp_path):
    out = tmp_path / "verification"
    out.mkdir()
    summary = out / "summary.json"
    summary.write_text("frozen results")
    with pytest.raises(FileExistsError):
        runner().verify(tmp_path, out, ["missing.py"])
    assert summary.read_text() == "frozen results"


def test_failed_case_preserves_log_and_marks_incomplete(tmp_path):
    (tmp_path / "test_failed.py").write_text(
        'def test_failure():\n    assert False, "intentional regression"\n')
    out = tmp_path / "verification"
    assert runner().verify(tmp_path, out, ["test_failed.py"]) == 1
    result = json.loads((out / "summary.json").read_text())
    assert not result["completed"]
    row = result["rows"][0]
    assert row["returncode"] != 0
    assert "intentional regression" in (out / row["log"]).read_text()
