"""Strict decoding of the pinned perf-script context-switch output format."""
from collections import defaultdict
from pathlib import Path
import re

HEADER = re.compile(r"^\s*(.*?)\s+(\d+)/(\d+)\s+(\d+)\.(\d{9}):\s+(.*?)\s*$")
FRAME = re.compile(r"^\s*([0-9a-fA-F]+)\s+(.*?)\s+\((.*?)\)\s*$")


def parse(stream, *, sample_event="context-switches"):
    events = []
    for number, line in enumerate(stream, 1):
        if not line.strip():
            continue
        match = HEADER.match(line)
        if match:
            comm, pid, tid, sec, nano, kind = match.groups()
            event = {"comm": comm, "pid": int(pid), "tid": int(tid),
                     "time_ns": int(sec)*10**9+int(nano), "frames": []}
            if kind == sample_event + ":":
                event["kind"] = "sample"
            elif sample_event == "context-switches" and kind in ("PERF_RECORD_SWITCH OUT", "PERF_RECORD_SWITCH OUT preempt"):
                event.update(kind="out", preempt=kind.endswith("preempt"))
            elif sample_event == "context-switches" and kind == "PERF_RECORD_SWITCH IN":
                event["kind"] = "in"
            else:
                raise ValueError(f"unknown/lost perf event at line {number}: {kind}")
            events.append(event)
            continue
        match = FRAME.match(line)
        if not match or not events or events[-1]["kind"] != "sample":
            raise ValueError(f"unexpected perf text at line {number}: {line.strip()}")
        ip, symbol, dso = match.groups()
        events[-1]["frames"].append({"ip": int(ip, 16), "symbol": symbol, "dso": dso})
    if not events:
        raise ValueError("empty context-switch capture")
    return events


def classify(frames):
    # This capture targets x86-64 user addresses. Kernel symbols are deliberately
    # not used; a missing user symbol remains visible in the raw/example stack.
    user = [f for f in frames if f["ip"] < 1 << 63]
    symbols = "\n".join(f["symbol"] for f in user)
    caller = next((f["symbol"] for f in user if "_seastar_probe" in Path(f["dso"]).name), None)
    if "_int_free" in symbols:
        category = "allocator_free"
    elif "_int_malloc" in symbols:
        category = "allocator_malloc"
    elif "pthread_mutex_lock" in symbols:
        category = "mutex"
    elif "pthread_cond" in symbols:
        category = "condition"
    elif "epoll_wait" in symbols or "epoll_pwait" in symbols:
        category = "poll"
    elif "clock_nanosleep" in symbols or "time_sleep" in symbols:
        category = "sleep"
    elif "lll_lock_wait" in symbols or "futex" in symbols:
        category = "unresolved_futex"
    else:
        category = "other_or_unknown"
    return category, caller, user


def summarize(events, tid, begin_ns, end_ns, *, pid=None):
    if type(begin_ns) is not int or type(end_ns) is not int or not 0 <= begin_ns < end_ns:
        raise ValueError("invalid attribution window")
    pending = active = None
    pid = tid if pid is None else pid
    previous = -1
    buckets = {}
    counts = defaultdict(int)
    delays = []

    def account(out, incoming):
        if not begin_ns <= out["time_ns"] < end_ns:
            if out["time_ns"] < begin_ns and (incoming is None or incoming >= begin_ns):
                counts["left_censored"] += 1
                if incoming is None or incoming > end_ns:
                    counts["right_censored"] += 1
            return
        kind = "preempt" if out["preempt"] else "voluntary"
        counts[kind] += 1
        category, caller, user = classify(out["sample"]["frames"])
        key = kind, category, caller
        bucket = buckets.setdefault(key, {"kind": kind, "category": category, "caller": caller,
            "switches": 0, "complete_intervals": 0, "offcpu_ns": 0, "max_offcpu_ns": 0,
            "example_user_stack": user})
        bucket["switches"] += 1
        delays.append(out["time_ns"] - out["sample"]["time_ns"])
        if incoming is None or incoming > end_ns:
            counts["right_censored"] += 1
        else:
            duration = incoming - out["time_ns"]
            bucket["complete_intervals"] += 1
            bucket["offcpu_ns"] += duration
            bucket["max_offcpu_ns"] = max(bucket["max_offcpu_ns"], duration)

    for event in events:
        if event["tid"] != tid or event["pid"] != pid or event["time_ns"] < previous:
            raise ValueError("wrong target thread or reversed event time")
        previous = event["time_ns"]
        if event["kind"] == "sample":
            if pending is not None or active is not None or not event["frames"]:
                raise ValueError("unmatched/empty sample while thread is off CPU")
            pending = event
            counts["all_samples"] += 1
        elif event["kind"] == "out":
            if pending is None or active is not None:
                raise ValueError("switch out lacks an unconsumed sample")
            active = event | {"sample": pending}
            pending = None
            counts["all_out"] += 1
        else:
            counts["all_in"] += 1
            if pending is not None:
                raise ValueError("sample followed by switch in instead of out")
            if active is None:
                if counts["all_in"] != 1 or counts["all_out"]:
                    raise ValueError("unmatched switch in inside attribution window")
                counts["leading_in"] += 1
                if begin_ns <= event["time_ns"]:
                    counts["left_censored"] += 1
                    if event["time_ns"] > end_ns:
                        counts["right_censored"] += 1
            else:
                account(active, event["time_ns"])
                active = None
    if pending is not None:
        if begin_ns <= pending["time_ns"] < end_ns:
            raise ValueError("unmatched sample inside attribution window")
        counts["trailing_sample"] += 1
    if active is not None:
        account(active, None)
        counts["trailing_out"] += 1
    return {key: counts[key] for key in ("voluntary", "preempt", "all_samples", "all_out", "all_in",
             "left_censored", "right_censored", "leading_in", "trailing_out", "trailing_sample")} | {
        "diagnostic_only": True, "tid": tid, "begin_ns": begin_ns, "end_ns": end_ns,
        "max_sample_to_out_ns": max(delays, default=0),
        "buckets": sorted(buckets.values(), key=lambda b: b["switches"], reverse=True)}


def summarize_threads(events, pid, tids, begin_ns, end_ns):
    grouped = {tid: [] for tid in tids}
    if len(grouped) != len(tids) or pid not in grouped:
        raise ValueError("invalid target threads")
    for event in events:
        if event["pid"] != pid or event["tid"] not in grouped:
            raise ValueError("wrong target process or thread")
        grouped[event["tid"]].append(event)
    if any(not values for values in grouped.values()):
        raise ValueError("missing target thread events")
    threads = {str(tid): summarize(values, tid, begin_ns, end_ns, pid=pid)
               for tid, values in grouped.items()}
    counts = {key: sum(t[key] for t in threads.values()) for key in
              ("voluntary", "preempt", "all_samples", "all_out", "all_in")}
    return counts | {"diagnostic_only": True, "pid": pid, "tids": tids,
                     "begin_ns": begin_ns, "end_ns": end_ns, "threads": threads}


def event_stats(text):
    section = text.split("context-switches stats:")[0]
    stats = {name: int(count) for name, count in re.findall(r"^\s*(\w+) events:\s+(\d+)", section, re.M)}
    if not stats.get("SAMPLE") or not stats.get("SWITCH"):
        raise ValueError("missing perf event accounting")
    if any(stats.get(name, 0) for name in ("LOST", "LOST_SAMPLES", "THROTTLE", "UNTHROTTLE")):
        raise ValueError("lost or throttled context-switch capture")
    return stats
