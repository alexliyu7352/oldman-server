"""Read visible Linux CPU limits; unknown limits never mean unlimited.

This measures the current host hierarchy, not a reservation of its CPUs. Sibling
groups can consume a shared parent budget. Keep every ancestor's counters and
reject throttled windows instead of summing overlapping throttle durations.
"""
import math
import os
from pathlib import Path
import re


def _read(path):
    try:
        return path.read_text().strip()
    except OSError as error:
        raise ValueError(f"unknown CPU budget/counters: cannot read {path}") from error


def _unescape(value):
    return re.sub(r"\\(040|011|012|134)", lambda match: chr(int(match[1], 8)), value)


def _hierarchy(proc, controller):
    memberships = {}
    for line in _read(proc / "cgroup").splitlines():
        _, controllers, member = line.split(":", 2)
        for name in controllers.split(","):
            memberships[name] = member
    # In a hybrid hierarchy, the unified systemd group need not own CPU at all.
    version = 1 if controller in memberships else 2
    member = memberships.get(controller if version == 1 else "")
    if member is None:
        return None
    if not member.startswith("/") or ".." in Path(member).parts:
        raise ValueError("unknown CPU budget: invalid cgroup membership")
    matches = []
    for line in _read(proc / "mountinfo").splitlines():
        left, right = line.split(" - ", 1)
        fields, detail = left.split(), right.split()
        if ((version == 1 and detail[0] == "cgroup" and controller in detail[2].split(",")) or
                (version == 2 and detail[0] == "cgroup2")):
            matches.append((_unescape(fields[3]), Path(_unescape(fields[4]))))
    # A subtree mount conceals ancestor limits. Prefer a full hierarchy mount;
    # never turn the absence of a visible parent into an unlimited budget.
    roots = [mount for root, mount in matches if root == "/"]
    if not roots:
        raise ValueError(f"unknown CPU budget: {controller} ancestors are not fully mounted")
    mount = roots[0]
    if version == 2 and controller not in _read(mount / "cgroup.controllers").split():
        return None
    current = mount / member.lstrip("/")
    paths = [current]
    while current != mount:
        current = current.parent
        paths.append(current)
    return {"version": version, "mount": str(mount), "membership": member,
            "paths": paths}


def _enabled_at(hierarchy, path, controller):
    return (hierarchy["version"] == 1 or
            (path != Path(hierarchy["mount"]) and
             controller in _read(path.parent / "cgroup.subtree_control").split()))


def _cpus(value):
    cpus = set()
    for item in value.split(",") if value else ():
        if not re.fullmatch(r"\d+(?:-\d+)?", item):
            raise ValueError(f"invalid cpuset: {value!r}")
        bounds = [int(part) for part in item.split("-")]
        if bounds[0] > bounds[-1]:
            raise ValueError(f"invalid cpuset range: {item}")
        cpus.update(range(bounds[0], bounds[-1] + 1))
    return cpus


def describe_cpu(*, proc=Path("/proc/self"), affinity=None):
    """Return JSON-compatible limits, before the benchmark pins its controller."""
    proc = Path(proc)
    affinity = set(os.sched_getaffinity(0) if affinity is None else affinity)
    cpu = _hierarchy(proc, "cpu")
    if cpu is None:
        raise ValueError("unknown CPU budget: CPU controller unavailable")
    quotas, levels = [], []
    for path in cpu.pop("paths"):
        enabled = _enabled_at(cpu, path, "cpu")
        if cpu["version"] == 1:
            quota = int(_read(path / "cpu.cfs_quota_us"))
            period = int(_read(path / "cpu.cfs_period_us"))
            quota = None if quota == -1 else quota
        elif enabled:
            quota_text, period_text = _read(path / "cpu.max").split()
            quota = None if quota_text == "max" else int(quota_text)
            period = int(period_text)
        else:
            # v2 root has no cpu.max; descendants without the CPU controller
            # enabled still inherit every enabled ancestor's limit.
            quota, period = None, None
        if (period is not None and period <= 0) or (quota is not None and quota <= 0):
            raise ValueError(f"invalid CPU quota/period at {path}")
        if quota is not None:
            quotas.append(quota / period)
        levels.append({"path": str(path), "quota_us": quota, "period_us": period,
                       "bandwidth": enabled})
    cpu["levels"] = levels
    cpuset = _hierarchy(proc, "cpuset")
    effective = set(affinity)
    if cpuset is not None:
        levels = []
        for path in cpuset.pop("paths"):
            effective_file = path / ("cpuset.effective_cpus" if cpuset["version"] == 1
                                     else "cpuset.cpus.effective")
            configured = None
            if _enabled_at(cpuset, path, "cpuset"):
                configured = _cpus(_read(path / "cpuset.cpus"))
                if cpuset["version"] == 2 and not effective_file.exists():
                    raise ValueError(f"unknown effective cpuset at {path}")
            actual = _cpus(_read(effective_file)) if effective_file.exists() else None
            if actual is not None:
                effective &= actual
            elif configured:
                effective &= configured
            elif path == Path(cpuset["mount"]):
                raise ValueError("unknown CPU budget: missing root cpuset")
            levels.append({"path": str(path),
                           "configured": sorted(configured) if configured is not None else None,
                           "effective": sorted(actual) if actual is not None else None})
        cpuset["levels"] = levels
    if not effective:
        raise ValueError("empty effective CPU affinity/cpuset")
    quota = min(quotas) if quotas else None
    return {"affinity": sorted(affinity), "effective_cpus": sorted(effective),
            "quota_cores": quota,
            "available_cores": min(len(effective), quota) if quota is not None else len(effective),
            "cpu": cpu, "cpuset": cpuset}


def require_budget(budget, cores):
    available = budget.get("available_cores")
    if (not isinstance(available, (int, float)) or not math.isfinite(available) or
            available < cores):
        raise ValueError(f"CPU budget {available!r} is unknown or below {cores} active CPUs")


def cpu_topology(cpus):
    rows = []
    for cpu in sorted(cpus):
        path = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
        row = {"cpu": cpu, "core_id": _read(path / "core_id"),
               "package_id": _read(path / "physical_package_id"),
               "thread_siblings_list": _read(path / "thread_siblings_list")}
        if (int(row["core_id"]) < 0 or int(row["package_id"]) < 0 or
                cpu not in _cpus(row["thread_siblings_list"])):
            raise ValueError(f"unknown CPU topology at {path}")
        rows.append(row)
    return rows


def cpu_snapshot(*, proc=Path("/proc/self")):
    """Capture limits and counters together, independent of controller affinity."""
    budget = describe_cpu(proc=proc)
    counters = {}
    version = budget["cpu"]["version"]
    for level in budget["cpu"]["levels"]:
        path = Path(level["path"]) / "cpu.stat"
        values = dict(line.split() for line in _read(path).splitlines())
        names = ("nr_periods", "nr_throttled",
                 "throttled_time" if version == 1 else "throttled_usec")
        if level["bandwidth"] and not all(name in values for name in names):
            raise ValueError(f"unknown throttle counters at {path}")
        numbers = [int(values.get(name, 0)) for name in names]
        if any(number < 0 for number in numbers):
            raise ValueError(f"negative CPU counter at {path}")
        counters[level["path"]] = {"nr_periods": numbers[0], "nr_throttled": numbers[1],
                                  "throttled_nsec": numbers[2] * (1 if version == 1 else 1000)}
    return {"cpu": budget["cpu"], "cpuset": budget["cpuset"], "counters": counters}


def cpu_delta(before, after):
    if (before["cpu"] != after["cpu"] or before["cpuset"] != after["cpuset"] or
            before["counters"].keys() != after["counters"].keys()):
        raise ValueError("CPU hierarchy or budget changed during measurement")
    deltas = {}
    for path, old in before["counters"].items():
        new = after["counters"][path]
        delta = {name: new[name] - old[name]
                 for name in ("nr_periods", "nr_throttled", "throttled_nsec")}
        if any(number < 0 for number in delta.values()):
            raise ValueError(f"CPU counter reset at {path}")
        delta["throttled_usec"] = delta["throttled_nsec"] / 1000
        deltas[path] = delta
    if not deltas:
        raise ValueError("missing CPU counters")
    return deltas


def require_unthrottled(before, after):
    for path, delta in cpu_delta(before, after).items():
        if delta["nr_throttled"] or delta["throttled_nsec"]:
            raise ValueError(f"CPU throttling at {path}: {delta}")
