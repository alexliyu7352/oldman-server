"""Experiment role, validity and paired-comparison contracts (no native runtime)."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from probes.application_lifecycle import run, summarize
import test_framework_benchmark as base_tests


def measurement(role="baseline", round_number=0, rps=100, main_cpu=10):
    row = base_tests.FrameworkBenchmarkTests().measurement()
    row.update(role=role, round=round_number, diagnostic_only=False, experiment_validated=True,
               warmup=3, seconds=10, concurrency=256, validated=True,
               python_main_cpu_per_request_us=main_cpu, server_cpu_per_request_us=20,
               snapshot_manifest={"sha256": "fixed-source-and-binaries"},
               comparison_inputs={"client": "frozen", "runtime": "same", "layout": "same"})
    row["client"]["rps"] = rps
    row["server"]["workers"][0]["delta"].update(
        native_response_json_encoded=42, native_response_json_fallback=0,
        native_response_queued=42, native_response_flushed=42)
    row["ready"] = {"workers": [{"native_responses": True}]}
    return row


@pytest.mark.parametrize("role,flag", [
    ("baseline", "--native-responses"), ("candidate", "--native-responses"),
    ("predecessor", "--native-responses"),
    ("uvloop-stdlib", "stdlib"), ("uvloop-codec", "native"),
])
def test_role_selects_actual_server_behavior(role, flag):
    command = run.server_command(Path("/frozen"), role, 4321, [2, 4, 6])
    assert flag in command
    assert command[command.index("--snapshot") + 1] == "/frozen"
    assert command[command.index("--port") + 1] == "4321"
    assert command[:3] == ["taskset", "-c", "2,4,6"]


def test_native_role_selects_frozen_native_binary():
    assert run.server_command(Path("/frozen"), "native", 4321, [2, 4, 6]) == [
        "taskset", "-c", "2,4,6", "/frozen/build/framework_native", "4321", "2,4,6"]


@pytest.mark.parametrize("role", ["baseline", "uvloop-stdlib", "uvloop-codec"])
def test_python_launch_keeps_libuv_helpers_inside_cpu_budget(role, monkeypatch):
    cpu = min(os.sched_getaffinity(0))
    command = run.server_command(Path("/frozen"), role, 4321, [cpu])
    monkeypatch.setenv("UV_USE_IO_URING", "1")
    # Exercise the actual launcher prefix, with a small loop instead of HTTP.
    code = """
import asyncio, json, os
from pathlib import Path
import uvloop
async def check():
    await asyncio.sleep(.01)
    print(json.dumps({'setting':os.environ.get('UV_USE_IO_URING'),
      'affinities':[sorted(os.sched_getaffinity(int(p.name))) for p in Path('/proc/self/task').iterdir()]}))
uvloop.run(check())
"""
    command = command[:command.index(sys.executable) + 1] + ["-c", code]
    result = subprocess.run(command, capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["setting"] == "0"
    assert all(affinity == [cpu] for affinity in report["affinities"])


def test_existing_output_is_refused_before_starting_work(tmp_path):
    marker = tmp_path / "marker"
    marker.write_text("preserve")
    with pytest.raises(FileExistsError):
        run.main(["--snapshot", "/not-needed", "--role", "baseline", "--round", "0", "--out", str(tmp_path)])
    assert marker.read_text() == "preserve"


def test_server_launcher_does_not_shadow_stdlib_profiler(tmp_path):
    (tmp_path / "profile.py").write_text("raise RuntimeError('local CLI shadowed stdlib')")
    probe = tmp_path / "server.py"
    probe.write_text("import cProfile; print(cProfile.Profile.__name__)")
    cpu = min(os.sched_getaffinity(0))
    command = run.server_command(Path("/frozen"), "baseline", 4321, [cpu])
    server_index = command.index(str(Path(run.__file__).with_name("server.py")))
    result = subprocess.run(command[:server_index] + [str(probe)],
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "Profile"


def test_missing_counter_and_wrong_opt_in_invalidate_measurement():
    row = measurement()
    run.validate_experiment(row)
    del row["server"]["workers"][0]["delta"]["native_response_flushed"]
    with pytest.raises(ValueError, match="native_response_flushed"):
        run.validate_experiment(row)
    row = measurement()
    row["ready"]["workers"][0]["native_responses"] = False
    with pytest.raises(ValueError, match="native.response"):
        run.validate_experiment(row)


def paired_rows():
    return [measurement(role, number) for number in range(3) for role in ("baseline", "candidate")]


def test_summary_requires_valid_unprofiled_complete_pairs():
    rows = paired_rows()
    assert summarize.summarize(rows)["aa_within_five_percent"] is True
    for change in ("diagnostic", "unvalidated", "missing-counter", "runtime-change"):
        bad = copy.deepcopy(rows)
        if change == "diagnostic":
            bad[0]["diagnostic_only"] = True
        elif change == "unvalidated":
            bad[0]["experiment_validated"] = False
        elif change == "missing-counter":
            del bad[0]["server"]["workers"][0]["delta"]["native_response_flushed"]
        else:
            bad[0]["comparison_inputs"]["runtime"] = "different"
        with pytest.raises(ValueError):
            summarize.summarize(bad)
    with pytest.raises(ValueError, match="paired"):
        summarize.summarize(rows[:-1])


def test_aa_noise_gate_checks_every_pair_and_main_cpu():
    rows = paired_rows()
    rows[3]["client"]["rps"] = 106
    assert summarize.summarize(rows)["aa_within_five_percent"] is False
    rows = paired_rows()
    rows[3]["python_main_cpu_per_request_us"] = 10.6
    assert summarize.summarize(rows)["aa_within_five_percent"] is False


def test_summary_does_not_hide_a_slow_candidate_round():
    rows = paired_rows()
    for row in rows:
        if row["role"] == "candidate":
            row["client"]["rps"] = 130 if row["round"] != 1 else 90
    result = summarize.summarize(rows)
    assert result["median_candidate_over_baseline"] == 1.3
    assert result["all_candidate_rounds_faster"] is False


@pytest.mark.parametrize("field", ["python_main_cpu_per_request_us", "server_cpu_per_request_us"])
@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf")])
def test_invalid_cpu_cost_cannot_enter_summary(field, value):
    rows = paired_rows()
    rows[0][field] = value
    with pytest.raises(ValueError, match="CPU"):
        summarize.summarize(rows)


def test_noise_calibration_requires_identical_frozen_sources():
    rows = paired_rows()
    for row in rows:
        if row["role"] == "candidate":
            row["snapshot_manifest"]["sha256"] = "changed-source"
    result = summarize.summarize(rows)
    assert result["comparison_kind"] == "A/B"
    assert result["aa_within_five_percent"] is None
    rows[3]["snapshot_manifest"]["sha256"] = "third-source"
    with pytest.raises(ValueError, match="snapshot"):
        summarize.summarize(rows)


def header_experiment():
    rows = []
    for number in range(6):
        order = ("baseline", "candidate") if number % 2 == 0 else ("candidate", "baseline")
        for position, role in enumerate(order):
            row = measurement(role, number, rps=110 if role == "candidate" else 100)
            row.update(started_unix_ns=(number*2+position)*100,
                       finished_unix_ns=(number*2+position)*100+90)
            if role == "candidate":
                row["snapshot_manifest"]["sha256"] = "header-candidate"
                row["server_cpu_per_request_us"] = 19
            rows.append(row)
    return rows


def test_header_decision_uses_native_cost_and_every_pair():
    rows = header_experiment()
    decision = summarize.summarize(rows, calibration=paired_rows())["header_decision"]
    assert decision["keep_candidate"] is True
    assert decision["median_native_cpu_reduction"] == pytest.approx(.1)
    # A positive median must not conceal a worse native-cost pair.
    rows[1]["server_cpu_per_request_us"] = 20.1
    decision = summarize.summarize(rows, calibration=paired_rows())["header_decision"]
    assert decision["keep_candidate"] is False
    assert decision["native_cost_proven"] is False


@pytest.mark.parametrize("failure", ["sequence", "overlap", "short", "noise", "runtime", "p99"])
def test_header_decision_rejects_invalid_evidence_and_tail_regression(failure):
    rows, calibration = header_experiment(), paired_rows()
    if failure == "sequence":
        rows[0]["started_unix_ns"], rows[1]["started_unix_ns"] = 100, 0
    elif failure == "overlap":
        rows[0]["finished_unix_ns"] = rows[1]["started_unix_ns"]+1
    elif failure == "short":
        rows = rows[:-2]
    elif failure == "noise":
        calibration[1]["client"]["rps"] = 110
    elif failure == "runtime":
        for row in calibration:
            row["comparison_inputs"]["runtime"] = "different"
    elif failure == "p99":
        for row in rows:
            if row["role"] == "candidate":
                row["client"]["p99_us"] *= 1.2
        decision = summarize.summarize(rows, calibration=calibration)["header_decision"]
        assert decision["keep_candidate"] is False
        assert decision["stable_regressions"] == ["p99"]
        return
    with pytest.raises(ValueError):
        summarize.summarize(rows, calibration=calibration)


def test_partial_reference_matrix_is_rejected():
    rows = header_experiment()
    reference = measurement("uvloop-stdlib")
    reference["mode"] = "multiprocess"
    reference["ready"]["workers"][0]["json_codec"] = "stdlib"
    with pytest.raises(ValueError, match="reference"):
        summarize.summarize(rows + [reference])


@pytest.mark.parametrize("field", ["rps", "p99_us"])
def test_infinite_reported_performance_is_rejected(field):
    row = measurement()
    row["client"][field] = float("inf")
    with pytest.raises(ValueError, match="performance"):
        run.validate_experiment(row)


def three_version_experiment():
    from itertools import permutations
    roles = ("baseline", "predecessor", "candidate")
    sequence = {"declared_unix_ns": 1, "order": [], "snapshots": {
        role: {"path": "/frozen/" + role, "source_commit": role + "-commit",
               "manifest_sha256": role + "-digest"} for role in roles}}
    rows = []
    for number, order in enumerate(permutations(roles)):
        for role in order:
            row = measurement(role, number, rps={"baseline": 100, "predecessor": 110, "candidate": 105}[role])
            binding = sequence["snapshots"][role]
            row.update(snapshot=binding["path"], source_commit=binding["source_commit"],
                       snapshot_manifest={"sha256": binding["manifest_sha256"]},
                       started_unix_ns=100+len(rows)*100, finished_unix_ns=190+len(rows)*100)
            sequence["order"].append([number, role])
            rows.append(row)
    return rows, sequence


def test_three_versions_keep_incremental_and_cumulative_changes_separate():
    rows, sequence = three_version_experiment()
    result = summarize.summarize(list(reversed(rows)), sequence=sequence)
    assert result["comparison_kind"] == "A/P/C"
    assert result["paired_metrics"]["rps"]["median_ratio"] == pytest.approx(1.05)
    assert result["incremental_paired_metrics"]["rps"]["ratios"] == pytest.approx([105/110]*6)
    assert result["sequence"] == sequence
    assert result["aa_within_five_percent"] is None


@pytest.mark.parametrize("failure", ["missing", "duplicate", "snapshot", "binding", "source", "path",
    "order", "overlap", "timestamp", "late-declaration", "no-sequence", "old-gate", "inputs", "counter", "opt-in"])
def test_three_version_summary_rejects_invalid_attribution(failure):
    rows, sequence = three_version_experiment()
    row = rows[1]  # predecessor must meet the same native-response contract.
    calibration = None
    if failure == "missing":
        rows.pop(1)
    elif failure == "duplicate":
        rows.append(copy.deepcopy(row))
    elif failure == "snapshot":
        row["snapshot_manifest"]["sha256"] = "another-digest"
    elif failure == "binding":
        sequence["snapshots"]["predecessor"]["manifest_sha256"] = "wrong-frozen-version"
    elif failure == "source":
        row["source_commit"] = "another-commit"
    elif failure == "path":
        row["snapshot"] = "/another/snapshot"
    elif failure == "order":
        sequence["order"][0], sequence["order"][1] = sequence["order"][1], sequence["order"][0]
    elif failure == "overlap":
        row["started_unix_ns"] = rows[0]["finished_unix_ns"] - 1
    elif failure == "timestamp":
        row["finished_unix_ns"] = row["started_unix_ns"]
    elif failure == "late-declaration":
        sequence["declared_unix_ns"] = rows[0]["started_unix_ns"] + 1
    elif failure == "no-sequence":
        sequence = None
    elif failure == "old-gate":
        calibration = paired_rows()
    elif failure == "inputs":
        row["comparison_inputs"]["client"] = "changed"
    elif failure == "counter":
        del row["server"]["workers"][0]["delta"]["native_response_flushed"]
    elif failure == "opt-in":
        row["ready"]["workers"][0]["native_responses"] = False
    with pytest.raises(ValueError):
        summarize.summarize(rows, sequence=sequence, calibration=calibration)


def test_three_version_cli_preserves_evidence_and_refuses_overwrite(tmp_path):
    rows, sequence = three_version_experiment()
    for row in rows:
        path = tmp_path / row["role"]
        path.mkdir(exist_ok=True)
        (path / f'json-candidate3-r{row["round"]}.json').write_text(json.dumps(row))
    declaration = tmp_path / "sequence.json"
    declaration.write_text(json.dumps(sequence))
    args = ["--out", str(tmp_path), "--sequence", str(declaration)]
    summarize.main(args)
    result = (tmp_path / "summary.json").read_text()
    assert json.loads(result)["incremental_paired_metrics"]["rps"]["median_ratio"] < 1
    with pytest.raises(FileExistsError):
        summarize.main(args)
    assert (tmp_path / "summary.json").read_text() == result


@pytest.mark.parametrize("field", ["declared_unix_ns", "started_unix_ns", "finished_unix_ns"])
@pytest.mark.parametrize("value", [float("nan"), float("inf"), True, 1.5, 0, -1])
def test_three_version_timestamps_require_positive_integer_nanoseconds(field, value):
    rows, sequence = three_version_experiment()
    target = sequence if field == "declared_unix_ns" else rows[0]
    target[field] = value
    with pytest.raises(ValueError):
        summarize.summarize(rows, sequence=sequence)
