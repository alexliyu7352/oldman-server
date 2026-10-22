"""Host observation contracts; no Seastar lifecycle is started here."""
import os
from pathlib import Path
import time

import pytest

from probes.application_lifecycle import run


def test_host_observation_is_explicit_opt_in():
    import inspect
    assert "observe_host" in inspect.signature(run.execute).parameters
    assert inspect.signature(run.execute).parameters["observe_host"].default is False


def test_stat_parser_keeps_identity_with_parentheses_and_spaces():
    from probes.application_lifecycle.host import parse_stat
    fields = ["S"] + ["0"] * 49
    fields[11], fields[12], fields[19], fields[36] = "12", "34", "567", "3"
    row = parse_stat("42 (name with ) and ( spaces) " + " ".join(fields))
    assert row == {"pid": 42, "comm": "name with ) and ( spaces", "start_ticks": 567,
                   "cpu_ticks": 46, "processor": 3}


def live_samples():
    from probes.application_lifecycle.host import sample
    cpus = sorted(os.sched_getaffinity(0))
    a = sample([os.getpid()], {os.getpid()}, cpus)
    time.sleep(.01)
    b = sample([os.getpid()], {os.getpid()}, cpus)
    return [a, b], cpus


def test_real_process_observation_has_monotonic_thread_counters():
    from probes.application_lifecycle.host import validate
    samples, cpus = live_samples()
    result = validate(samples, cpus)
    main = result["threads"][str(os.getpid())]
    assert main["voluntary_ctxt_switches"] >= 1
    assert main["cpu_ticks"] >= 0
    assert result["observer_cpu_ns"] > 0
    assert result["observer_wall_ns"] > 0
    assert samples[0]["excluded_pids"] == [os.getpid()]
    assert str(os.getpid()) not in samples[0]["other_processes"]


@pytest.mark.parametrize("field", ["start_ticks", "cpu_ticks", "voluntary_ctxt_switches",
                                  "nonvoluntary_ctxt_switches", "affinity"])
def test_service_identity_reset_or_escape_is_rejected(field):
    from probes.application_lifecycle.host import validate
    samples, cpus = live_samples()
    tid = str(os.getpid())
    if field == "start_ticks":
        samples[-1]["threads"][tid][field] += 1
    elif field == "affinity":
        samples[-1]["threads"][tid][field] = [max(cpus) + 1]
    else:
        samples[-1]["threads"][tid][field] = -1
    with pytest.raises(ValueError):
        validate(samples, cpus)


def test_missing_samples_and_threads_are_not_treated_as_zero_cost():
    from probes.application_lifecycle.host import validate
    samples, cpus = live_samples()
    with pytest.raises(ValueError):
        validate(samples[:1], cpus)
    del samples[-1]["threads"][str(os.getpid())]
    with pytest.raises(ValueError):
        validate(samples, cpus)


def test_unavailable_optional_inputs_are_explicit(tmp_path):
    from probes.application_lifecycle.host import sample, validate
    cpus = sorted(os.sched_getaffinity(0))
    a = sample([os.getpid()], {os.getpid()}, cpus, sys_cpu=tmp_path)
    b = sample([os.getpid()], {os.getpid()}, cpus, sys_cpu=tmp_path)
    assert a["frequency"][str(cpus[0])]["scaling_cur_freq"] is None
    assert any("scaling_cur_freq" in item["path"] for item in a["unavailable"])
    validate([a, b], cpus)


def test_frequency_sampling_avoids_unused_smt_cpus(tmp_path):
    from probes.application_lifecycle.host import sample
    cpus = sorted(os.sched_getaffinity(0))
    row = sample([os.getpid()], {os.getpid()}, cpus, sys_cpu=tmp_path, frequency_cpus=cpus[:1])
    assert set(row["frequency"]) == {str(cpus[0])}
    assert set(row["cpu_ticks"]) == set(map(str, cpus))


def test_declared_observation_cannot_be_missing_from_formal_row():
    from probes.application_lifecycle.host import CONFIG
    from test_application_lifecycle_benchmark import measurement
    row = measurement()
    row["comparison_inputs"]["host_observer"] = CONFIG
    with pytest.raises(ValueError, match="host"):
        run.validate_experiment(row)


def test_positive_counter_regression_is_rejected():
    from probes.application_lifecycle.host import validate
    samples, cpus = live_samples()
    tid = str(os.getpid())
    samples[0]["threads"][tid]["cpu_ticks"] = 100
    samples[1]["threads"][tid]["cpu_ticks"] = 99
    with pytest.raises(ValueError, match="counter reset"):
        validate(samples, cpus)


@pytest.mark.parametrize("bad", [None, "first", "last"])
def test_host_samples_bracket_service_statistics(bad):
    from probes.application_lifecycle.host import validate_window
    samples, cpus = live_samples()
    anchors = {"begin_sent_ns": samples[0]["finished_ns"] + 1,
               "end_received_ns": samples[-1]["started_ns"] - 1}
    if bad == "first":
        anchors["begin_sent_ns"] = samples[0]["started_ns"]
    if bad == "last":
        anchors["end_received_ns"] = samples[-1]["finished_ns"]
    if bad:
        with pytest.raises(ValueError, match="statistics window"):
            validate_window(samples, cpus, anchors)
    else:
        validate_window(samples, cpus, anchors)


def test_required_read_failure_carries_partial_observation(monkeypatch):
    from probes.application_lifecycle.host import sample, ObservationError
    read = Path.read_text
    def fail(path, *args, **kwargs):
        if str(path).startswith(f"/proc/{os.getpid()}/task/") and path.name == "status":
            raise FileNotFoundError(2, "injected missing thread", str(path))
        return read(path, *args, **kwargs)
    monkeypatch.setattr(Path, "read_text", fail)
    with pytest.raises(ObservationError) as caught:
        sample([os.getpid()], {os.getpid()}, sorted(os.sched_getaffinity(0)))
    partial = caught.value.observation
    assert partial["threads"]
    assert partial["failure"]["path"].endswith("/status")
    assert partial["finished_ns"] >= partial["started_ns"]


def test_sequence_cannot_accept_a_whole_unobserved_experiment():
    from probes.application_lifecycle import host, summarize
    from test_application_lifecycle_benchmark import three_version_experiment
    rows, sequence = three_version_experiment()
    sequence["host_observer"] = host.CONFIG
    with pytest.raises(ValueError, match="declared host"):
        summarize.validate_sequence(rows, sequence)


def test_sequence_binds_declared_tool_hashes():
    from probes.application_lifecycle import host, summarize
    from test_application_lifecycle_benchmark import three_version_experiment
    rows, sequence = three_version_experiment()
    sequence.update(host_observer=host.CONFIG, tool_inputs={"observer.py": "expected"})
    for row in rows:
        row["comparison_inputs"].update(host_observer=host.CONFIG,
                                      harness={"sha256": {"observer.py": "wrong"}})
    with pytest.raises(ValueError, match="declared tool"):
        summarize.validate_sequence(rows, sequence)
