"""Describe allocation evidence; never decide throughput gains from tracing."""
import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path
from statistics import mean, median
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from probes.application_lifecycle.inputs import fingerprint
from probes.application_lifecycle.run import validate_experiment
from probes.application_lifecycle.summarize import summarize
from probes.drain_allocation.observer import validate

parser = argparse.ArgumentParser()
parser.add_argument("--out", type=Path, required=True)
args = parser.parse_args()
out = args.out.resolve()
sequence = json.loads((out / "sequence.json").read_text())
histogram = Counter()
timed = defaultdict(lambda: defaultdict(list))
windows, rows = [], []
for number, role in enumerate(sequence["order"]):
    folder = out / f"r{number:02d}-{role}"
    [path] = folder.glob("json-*-r*.json")
    row = json.loads(path.read_text())
    validate_experiment(row)
    if not row["diagnostic_only"] or not row["experiment_validated"] or row["profiler"] != "allocation":
        raise ValueError("requires validated allocation diagnostic")
    if row["role"] != role or row["round"] != number or row["source_commit"] != sequence["snapshots"][role]["source_commit"]:
        raise ValueError("declared order/source changed")
    if rows and (rows[0]["comparison_inputs"] != row["comparison_inputs"] or
                 rows[-1]["finished_unix_ns"] > row["started_unix_ns"]):
        raise ValueError("input mismatch or overlapping diagnostics")
    if row["started_unix_ns"] <= sequence["declared_unix_ns"] or row["finished_unix_ns"] <= row["started_unix_ns"]:
        raise ValueError("invalid diagnostic timestamps")
    check = json.loads((folder / "trace-validation.json").read_text())
    if check["valid"] is not True or check["trace"] != fingerprint(folder / "drain-trace.json"):
        raise ValueError("trace changed after validation")
    trace = json.loads((folder / "drain-trace.json").read_text())
    records = validate(trace, role=role)
    if trace["period"] != sequence["sample_period"]:
        raise ValueError("sample period changed")
    for i, record in enumerate(records):
        if record["sampled"] != int(i % trace["period"] == 0):
            raise ValueError("sample selection changed")
    nonempty = [r for r in records if r["size"]]
    samples = [r for r in nonempty if r["sampled"]]
    delta = row["server"]["workers"][0]["delta"]
    events = sum(r["size"] for r in records)
    if events != delta["python_application_started"] or events != delta["responses"]:
        raise ValueError("trace events do not match the actual business window")
    for record in nonempty:
        n = record["size"]
        histogram[n] += 1
        if record["sampled"]:
            timed[role][n].append(record)
    windows.append({"window": folder.name, "role": role, "events": events,
        "drains": len(records), "nonempty_drains": len(nonempty), "timed_nonempty_drains": len(samples),
        "mean_batch_size": events/len(nonempty),
        "allocation_calls": sum(r["allocations"] for r in records),
        "mean_allocations_per_nonempty_drain": mean(r["allocations"] for r in nonempty),
        "requested_bytes": sum(r["bytes"] for r in records),
        "requested_bytes_per_event": sum(r["bytes"] for r in records)/events,
        "distinct_requested_sizes": len(trace["allocations"]),
        "mean_sampled_allocation_ns": mean(r["allocation_ns"] for r in samples),
        "mean_sampled_drain_ns": mean(r["drain_ns"] for r in samples),
        "median_sampled_drain_ns": median(r["drain_ns"] for r in samples),
        "notifications_per_1000_responses": delta["notification_writes"]*1000/events,
        "return_batches_per_1000_responses": delta["decision_return_batches"]*1000/events,
        "posts_per_1000_responses": delta["decision_return_posts"]*1000/events})
    rows.append(row)
common_sizes = sorted(set(timed["predecessor"]) & set(timed["candidate"]))
details = {n: {role: {"samples": len(timed[role][n]),
            **{key: mean(r[key] for r in timed[role][n]) for key in ("allocation_ns", "drain_ns")}}
            for role in ("predecessor", "candidate")} for n in common_sizes}
weight = sum(histogram[n] for n in common_sizes)
normalized = {role: {key: sum(histogram[n]*details[n][role][key] for n in common_sizes)/weight
            for key in ("allocation_ns", "drain_ns")} for role in ("predecessor", "candidate")}
try:
    summarize(rows)
except ValueError as error:
    if "diagnostic" not in str(error):
        raise
else:
    raise AssertionError("formal summary accepted diagnostic rows")
report = {"diagnostic_only": True, "windows": windows, "batch_histogram": dict(sorted(histogram.items())),
    "same_batch_sample_means": details, "pooled_batch_weighted_sample_means": normalized,
    "common_batch_coverage": weight/sum(histogram.values()),
    "formal_summary_rejects_diagnostics": True, "driver_commit": rows[0]["comparison_inputs"]["harness"]["git_head"],
    "timing_limit": "systematic 1/64 sampling; instrumented wall times, not CPU cost or unprofiled speedup"}
with (out / "analysis.json").open("x") as stream:
    json.dump(report, stream, indent=2)
    stream.write("\n")
print(json.dumps({"normalized_samples": normalized, "coverage": report["common_batch_coverage"]}, indent=2))
