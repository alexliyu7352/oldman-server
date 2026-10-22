"""Control the preloaded observer from its owning Python loop thread."""
import ctypes
import json
from pathlib import Path


class Observer:
    def __init__(self, library, module, period=64):
        self.library = ctypes.PyDLL(str(Path(library).resolve()))
        for name, args, result in (
            ("init", [ctypes.c_char_p, ctypes.c_size_t], ctypes.c_int),
            ("start", [], None), ("stop", [], None),
            ("dump", [ctypes.c_char_p], ctypes.c_int)):
            function = getattr(self.library, "drain_trace_" + name)
            function.argtypes, function.restype = args, result
        code = self.library.drain_trace_init(str(Path(module).resolve()).encode(), period)
        if code:
            raise RuntimeError(f"observer could not bind the actual frozen module: {code}")

    def start(self):
        self.library.drain_trace_start()

    def finish(self, path):
        self.library.drain_trace_stop()
        code = self.library.drain_trace_dump(str(Path(path).resolve()).encode())
        if code:
            raise RuntimeError(f"observer output failed: {code}")
        report = json.loads(Path(path).read_text())
        validate(report)
        return report


def validate(report, *, role=None):
    if report["diagnostic_only"] is not True or report["errors"] or not report["records"]:
        raise ValueError("missing or invalid drain observation")
    records = [dict(zip(report["record_columns"], r, strict=True)) for r in report["records"]]
    buckets = report["allocations"]
    if (sum(r["allocations"] for r in records) != sum(b["calls"] for b in buckets) or
            sum(r["bytes"] for r in records) != sum(b["calls"]*b["slots"]*report["event_bytes"] for b in buckets) or
            sum(r["allocation_ns"] for r in records) != sum(b["ns"] for b in buckets)):
        raise ValueError("allocation accounting mismatch")
    if any(r["size"] > r["count"] or r["size"] > r["capacity"] or
           (not r["size"] and (r["allocations"] or r["bytes"])) or
           (r["size"] and not r["allocations"]) for r in records):
        raise ValueError("drain size/allocation mismatch")
    if any(b["other_callers"] or sum(b["ns_log2_histogram"]) != b["samples"] or
           b["usable_min"] < b["slots"]*report["event_bytes"] for b in buckets):
        raise ValueError("allocation size/timing mismatch")
    if role is not None:
        if role not in ("predecessor", "candidate"):
            raise ValueError("unknown allocation version")
        expected = {}
        for row in records:
            n = row["size"]
            capacity = n if role == "candidate" else (1 << (n-1).bit_length() if n else 0)
            slots = [n] if role == "candidate" and n else (
                [1 << i for i in range(capacity.bit_length())] if role == "predecessor" else [])
            if (row["capacity"], row["allocations"], row["bytes"]) != (
                    capacity, len(slots), sum(slots)*report["event_bytes"]):
                raise ValueError("allocation pattern differs from frozen version")
            for size in slots:
                expected[size] = expected.get(size, 0) + 1
        if {b["slots"]: b["calls"] for b in buckets} != expected:
            raise ValueError("allocation sizes differ from frozen version")
    return records
