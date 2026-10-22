"""CPU limits must follow controller membership, including ancestor limits."""
from unittest import mock

import pytest

from scripts import cpu_budget


class Hierarchy:
    def __init__(self, tmp_path, version=1, mount_name="cgroup", mount_root="/"):
        self.proc = tmp_path / "proc"
        self.proc.mkdir()
        self.mount = tmp_path / mount_name
        self.mount.mkdir()
        self.version = version
        escaped = str(self.mount).replace(" ", r"\040")
        kind = "cgroup" if version == 1 else "cgroup2"
        options = "rw,cpu,cpuacct" if version == 1 else "rw"
        (self.proc / "mountinfo").write_text(
            f"1 0 0:1 {mount_root} {escaped} rw - {kind} cgroup {options}\n")
        (self.proc / "cgroup").write_text(
            "4:cpu,cpuacct:/parent/leaf\n" if version == 1 else "0::/parent/leaf\n")
        for name in ("", "parent", "parent/leaf"):
            self.level(name)

    def level(self, name, quota=None, period=100000):
        path = self.mount / name
        path.mkdir(exist_ok=True)
        if self.version == 1:
            (path / "cpu.cfs_quota_us").write_text(str(-1 if quota is None else quota))
            (path / "cpu.cfs_period_us").write_text(str(period))
            (path / "cpu.stat").write_text("nr_periods 10\nnr_throttled 0\nthrottled_time 0\n")
        else:
            (path / "cgroup.controllers").write_text("cpu cpuset\n")
            (path / "cgroup.subtree_control").write_text("cpu cpuset\n")
            (path / "cpuset.cpus").write_text("\n")
            (path / "cpuset.cpus.effective").write_text("0-15\n")
            if name:
                (path / "cpu.max").write_text(f"{'max' if quota is None else quota} {period}\n")
            (path / "cpu.stat").write_text("usage_usec 100\nuser_usec 60\nsystem_usec 40\n" +
                ("nr_periods 10\nnr_throttled 0\nthrottled_usec 0\n" if name else ""))

    def describe(self, affinity=range(16)):
        return cpu_budget.describe_cpu(proc=self.proc, affinity=affinity)

    def snapshot(self):
        with mock.patch.object(cpu_budget.os, "sched_getaffinity", return_value=set(range(16))):
            return cpu_budget.cpu_snapshot(proc=self.proc)


@pytest.mark.parametrize("version", [1, 2])
def test_unlimited_leaf_is_still_limited_by_parent(tmp_path, version):
    fs = Hierarchy(tmp_path, version)
    fs.level("parent", quota=250000)
    budget = fs.describe()
    assert budget["quota_cores"] == 2.5
    assert budget["available_cores"] == 2.5
    assert len(budget["cpu"]["levels"]) == 3
    with pytest.raises(ValueError, match="budget"):
        cpu_budget.require_budget(budget, 8)


def test_hybrid_uses_v1_cpu_even_with_unified_systemd_membership(tmp_path):
    fs = Hierarchy(tmp_path)
    (tmp_path / "unified").mkdir()
    (tmp_path / "unified/cgroup.controllers").write_text("")
    with (fs.proc / "cgroup").open("a") as out:
        out.write("0::/different/systemd/path\n")
    with (fs.proc / "mountinfo").open("a") as out:
        out.write(f"2 0 0:2 / {tmp_path}/unified rw - cgroup2 cgroup rw\n")
    assert fs.describe()["available_cores"] == 16
    assert fs.describe()["cpu"]["version"] == 1


def test_v1_cpuset_inheritance_and_affinity_intersect(tmp_path):
    fs = Hierarchy(tmp_path)
    mount = tmp_path / "cpuset"
    for name, cpus in (("", "0-15"), ("parent", "2-7,10"), ("parent/leaf", "")):
        path = mount / name
        path.mkdir(parents=True)
        (path / "cpuset.cpus").write_text(cpus)
    with (fs.proc / "cgroup").open("a") as out:
        out.write("6:cpuset:/parent/leaf\n")
    with (fs.proc / "mountinfo").open("a") as out:
        out.write(f"2 0 0:2 / {mount} rw - cgroup cgroup rw,cpuset\n")
    budget = fs.describe(range(4, 12))
    assert budget["effective_cpus"] == [4, 5, 6, 7, 10]
    assert budget["available_cores"] == 5


def test_v2_effective_cpuset_overrides_empty_configured_set(tmp_path):
    fs = Hierarchy(tmp_path, 2)
    (fs.mount / "parent/leaf/cpuset.cpus.effective").write_text("4-7\n")
    assert fs.describe()["effective_cpus"] == [4, 5, 6, 7]


def test_escaped_mountpoint_is_decoded(tmp_path):
    fs = Hierarchy(tmp_path, mount_name="cpu mount")
    fs.level("parent", quota=800000)
    assert fs.describe()["available_cores"] == 8


def test_hidden_ancestors_are_not_assumed_unlimited(tmp_path):
    fs = Hierarchy(tmp_path, mount_root="/parent")
    with pytest.raises(ValueError, match="ancestor"):
        fs.describe()


@pytest.mark.parametrize("filename,value", [
    ("cpu.cfs_quota_us", None), ("cpu.cfs_quota_us", "invalid"),
    ("cpu.cfs_quota_us", "0"), ("cpu.cfs_period_us", "0"),
])
def test_unknown_or_invalid_budget_is_rejected(tmp_path, filename, value):
    fs = Hierarchy(tmp_path)
    path = fs.mount / "parent" / filename
    if value is None:
        path.unlink()
    else:
        path.write_text(value)
    with pytest.raises(ValueError):
        fs.describe()


def test_v2_missing_quota_when_cpu_controller_enabled_is_unknown(tmp_path):
    fs = Hierarchy(tmp_path, 2)
    (fs.mount / "parent/cpu.max").unlink()
    with pytest.raises(ValueError):
        fs.describe()


def test_v2_missing_effective_cpuset_is_unknown(tmp_path):
    fs = Hierarchy(tmp_path, 2)
    (fs.mount / "parent/leaf/cpuset.cpus.effective").unlink()
    with pytest.raises(ValueError, match="cpuset"):
        fs.describe()


def test_v2_disabled_leaf_controller_inherits_parent_limit(tmp_path):
    fs = Hierarchy(tmp_path, 2)
    fs.level("parent", quota=800000)
    (fs.mount / "parent/cgroup.subtree_control").write_text("cpuset\n")
    (fs.mount / "parent/leaf/cpu.max").unlink()
    (fs.mount / "parent/leaf/cpu.stat").write_text("usage_usec 10\nuser_usec 5\nsystem_usec 5\n")
    assert fs.describe()["available_cores"] == 8
    cpu_budget.require_unthrottled(fs.snapshot(), fs.snapshot())


@pytest.mark.parametrize("version", [1, 2])
def test_parent_throttling_is_rejected_even_if_leaf_has_none(tmp_path, version):
    fs = Hierarchy(tmp_path, version)
    before = fs.snapshot()
    time_field = "throttled_time 125000" if version == 1 else "throttled_usec 125"
    (fs.mount / "parent/cpu.stat").write_text(
        f"nr_periods 11\nnr_throttled 1\n{time_field}\n")
    after = fs.snapshot()
    delta = cpu_budget.cpu_delta(before, after)
    assert delta[str(fs.mount / "parent")]["throttled_usec"] == 125
    assert delta[str(fs.mount / "parent/leaf")]["nr_throttled"] == 0
    with pytest.raises(ValueError, match="throttl"):
        cpu_budget.require_unthrottled(before, after)


def test_counter_reset_and_changed_quota_invalidate_window(tmp_path):
    fs = Hierarchy(tmp_path)
    before = fs.snapshot()
    (fs.mount / "parent/cpu.stat").write_text("nr_periods 0\nnr_throttled 0\nthrottled_time 0\n")
    with pytest.raises(ValueError, match="counter"):
        cpu_budget.cpu_delta(before, fs.snapshot())
    fs.level("parent", quota=800000)
    with pytest.raises(ValueError, match="changed"):
        cpu_budget.cpu_delta(before, fs.snapshot())


def test_missing_throttle_counter_is_not_zero(tmp_path):
    fs = Hierarchy(tmp_path)
    (fs.mount / "parent/cpu.stat").write_text("nr_periods 10\n")
    with pytest.raises(ValueError, match="counter"):
        fs.snapshot()


def test_unknown_budget_cannot_pass_gate():
    with pytest.raises(ValueError, match="budget"):
        cpu_budget.require_budget({}, 8)
