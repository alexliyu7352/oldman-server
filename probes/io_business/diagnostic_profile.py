"""Intrusive CPU attribution for diagnosis only; never use its RPS as a result."""
import cProfile
import json
import os
from pathlib import Path
import pstats
import time

from comparison_metrics import Snapshot

TIMINGS = {}
ENABLED = False


def timed(name, function):
    def call(*args, **kwargs):
        if not ENABLED:
            return function(*args, **kwargs)
        start_wall, start_cpu = time.perf_counter_ns(), time.thread_time_ns()
        result = function(*args, **kwargs)
        cpu, wall = time.thread_time_ns() - start_cpu, time.perf_counter_ns() - start_wall
        row = TIMINGS.setdefault(name, dict(calls=0, cpu_ns=0, wall_ns=0, items=0))
        row["calls"] += 1; row["cpu_ns"] += cpu; row["wall_ns"] += wall
        if name in ("native.drain", "native.drain_requests"):
            row["items"] += len(result)
        elif name == "native.resolve_requests":
            row["items"] += len(args[0])
        return result
    return call


class TimedNative:
    def __init__(self, native):
        self.native = native
        for name in ("drain", "drain_requests", "resolve_requests", "stats"):
            setattr(self, name, timed("native."+name, getattr(native, name)))

    def __getattr__(self, name):
        return getattr(self.native, name)


class BridgeSnapshot(Snapshot):
    def begin(self):
        global ENABLED
        super().begin(); TIMINGS.clear(); ENABLED = True
        self.cpu_start = time.thread_time_ns()

    def end(self):
        global ENABLED
        ENABLED = False
        elapsed_cpu = time.thread_time_ns() - self.cpu_start
        result = super().end()
        report = dict(instrumented=True, python_cpu_ns=elapsed_cpu, timings=TIMINGS.copy())
        folder = Path(os.environ["PROBE_BRIDGE_TIMING_DIR"]); folder.mkdir(parents=True, exist_ok=True)
        (folder/f"bridge-{os.getpid()}.json").write_text(json.dumps(report,indent=2)+"\n")
        result["bridge_timing"] = report
        return result


def install_bridge_timers(engine_type):
    engine_type._drain = timed("engine._drain", engine_type._drain)
    engine_type._flush_http = timed("engine._flush_http", engine_type._flush_http)


class ProfilingSnapshot(Snapshot):
    def begin(self):
        super().begin()
        self.profiler = cProfile.Profile(time.thread_time)
        self.profiler.enable()

    def end(self):
        self.profiler.disable()
        result = super().end()
        folder = Path(os.environ["PROBE_PROFILE_DIR"])
        folder.mkdir(parents=True, exist_ok=True)
        self.profiler.dump_stats(str(folder/f"python-{os.getpid()}.prof"))
        stats = pstats.Stats(self.profiler)
        rows = []
        for (file, line, function), (primitive, total, own, cumulative, callers) in stats.stats.items():
            rows.append(dict(file=file, line=line, function=function, calls=total,
                             self_cpu=own, cumulative_cpu=cumulative))
        rows.sort(key=lambda r:r["self_cpu"], reverse=True)
        report = dict(intrusive=True, timer="time.thread_time", total_cpu=stats.total_tt,
                      total_calls=stats.total_calls, rows=rows)
        (folder/f"python-{os.getpid()}.json").write_text(json.dumps(report,indent=2)+"\n")
        result["profile"] = {k:v for k,v in report.items() if k != "rows"}
        return result
