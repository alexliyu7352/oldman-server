"""Read-only controller observations; never loaded into a measured service."""
import os
from pathlib import Path
import time

CONFIG = {"schema": 1, "cadence": "before server begin, existing about-1s samples, after server end",
          "sources": ["proc task stat/status/schedstat", "proc stat", "proc process stat",
                      "sysfs cpufreq", "kernel sched_schedstats"],
          "frequency_semantics": "driver-reported samples, not effective hardware frequency",
          "frequency_scope": "service CPUs only; avoid reading idle SMT sibling frequencies",
          "background_semantics": "visible processes excluding service/client/controller; short-lived work can be missed"}
COUNTERS = ("cpu_ticks", "voluntary_ctxt_switches", "nonvoluntary_ctxt_switches")


class ObservationError(RuntimeError):
    def __init__(self, error, observation):
        super().__init__(str(error))
        self.observation = observation


def parse_stat(text):
    prefix, tail = text.rsplit(")", 1)
    pid, comm = prefix.split("(", 1)
    fields = tail.split()
    return {"pid": int(pid), "comm": comm, "cpu_ticks": int(fields[11]) + int(fields[12]),
            "start_ticks": int(fields[19]), "processor": int(fields[36])}


def sample(service_pids, excluded_pids, cpus, *, sys_cpu=Path("/sys/devices/system/cpu"),
           frequency_cpus=None):
    started, cpu_started = time.monotonic_ns(), time.thread_time_ns()
    observation = {"started_ns": started}
    try:
        _collect(observation, service_pids, excluded_pids, cpus, sys_cpu, frequency_cpus)
    except (OSError, ValueError) as error:
        observation["failure"] = {"path": getattr(error, "filename", None),
                                  "type": type(error).__name__, "error": str(error)}
        raise ObservationError(error, observation) from error
    finally:
        observation["observer_cpu_ns"] = time.thread_time_ns() - cpu_started
        observation["finished_ns"] = time.monotonic_ns()
    return observation


def _collect(observation, service_pids, excluded_pids, cpus, sys_cpu, frequency_cpus):
    unavailable = []
    threads = {}
    observation.update(unavailable=unavailable, threads=threads)

    def optional(path):
        try:
            return path.read_text().strip()
        except OSError as error:
            unavailable.append({"path": str(path), "error": type(error).__name__})
            return None

    for pid in service_pids:
        for task in sorted(Path(f"/proc/{pid}/task").iterdir()):
            # Service data is required: unlike background races, do not suppress
            # missing/changed threads and silently treat their cost as zero.
            row = parse_stat((task / "stat").read_text())
            threads[task.name] = row
            status = dict(line.split(":", 1) for line in (task / "status").read_text().splitlines())
            row.update(pid=pid, affinity=sorted(os.sched_getaffinity(int(task.name))))
            row.update({name: int(status[name]) for name in COUNTERS[1:]})
            raw = optional(task / "schedstat")
            row["schedstat_raw"] = None if raw is None else list(map(int, raw.split()))

    cpu_ticks = {}
    for line in Path("/proc/stat").read_text().splitlines():
        name, *fields = line.split()
        if name.startswith("cpu") and name[3:].isdigit() and int(name[3:]) in cpus:
            cpu_ticks[name[3:]] = list(map(int, fields))
    frequency = {}
    for cpu in cpus if frequency_cpus is None else frequency_cpus:
        root = sys_cpu / f"cpu{cpu}" / "cpufreq"
        frequency[str(cpu)] = {key: optional(root / key) for key in (
            "scaling_driver", "scaling_governor", "scaling_cur_freq", "cpuinfo_cur_freq")}

    others = {}
    for path in sorted(Path("/proc").iterdir()):
        if path.name.isdigit() and int(path.name) not in excluded_pids:
            raw = optional(path / "stat")
            if raw is not None:
                others[path.name] = parse_stat(raw)
    observation.update({"threads": threads, "cpu_ticks": cpu_ticks,
           "frequency": frequency, "other_processes": others,
           "excluded_pids": sorted(excluded_pids), "unavailable": unavailable,
           "sched_schedstats": optional(Path("/proc/sys/kernel/sched_schedstats")),
           "tick_hz": os.sysconf("SC_CLK_TCK")})


def validate(samples, service_cpus):
    """Validate every sample, returning deltas for the *outer* observation window."""
    if len(samples) < 2 or not samples[0].get("threads"):
        raise ValueError("missing host observations/threads")
    first, last = samples[0], samples[-1]
    previous = None
    for row in samples:
        if (type(row["started_ns"]) is not int or type(row["finished_ns"]) is not int or
                not 0 < row["started_ns"] < row["finished_ns"] or
                row["observer_cpu_ns"] < 0 or
                set(row["threads"]) != set(first["threads"]) or
                row["excluded_pids"] != first["excluded_pids"] or row["tick_hz"] != first["tick_hz"]):
            raise ValueError("host observation identity/window changed")
        if previous is not None and row["started_ns"] <= previous["finished_ns"]:
            raise ValueError("host observations overlap or reverse")
        for tid, thread in row["threads"].items():
            before = first["threads"][tid]
            if (thread["pid"] != before["pid"] or thread["start_ticks"] != before["start_ticks"] or
                    not thread["affinity"] or not set(thread["affinity"]) <= set(service_cpus)):
                raise ValueError("host service thread identity/affinity changed")
            for key in COUNTERS:
                value = thread[key]
                if type(value) is not int or value < 0 or (previous is not None and
                        value < previous["threads"][tid][key]):
                    raise ValueError("host service thread counter reset")
        if not set(map(str, service_cpus)) <= row["cpu_ticks"].keys():
            raise ValueError("missing host CPU accounting")
        previous = row
    return {"seconds": (last["finished_ns"] - first["started_ns"]) / 1e9,
            "observer_cpu_ns": sum(row["observer_cpu_ns"] for row in samples),
            "observer_wall_ns": sum(row["finished_ns"] - row["started_ns"] for row in samples),
            "threads": {tid: {key: thread[key] - first["threads"][tid][key] for key in COUNTERS}
                        for tid, thread in last["threads"].items()},
            "runqueue_wait_available": all(row["sched_schedstats"] == "1" and
                all(t["schedstat_raw"] is not None for t in row["threads"].values()) for row in samples)}


def validate_window(samples, service_cpus, anchors):
    result = validate(samples, service_cpus)
    begin, end = (anchors.get(key) for key in ("begin_sent_ns", "end_received_ns"))
    if (type(begin) is not int or type(end) is not int or
            not samples[0]["finished_ns"] < begin < end < samples[-1]["started_ns"] or
            any(not begin < row["started_ns"] < row["finished_ns"] < end for row in samples[1:-1])):
        raise ValueError("host samples cross the service statistics window")
    return result
