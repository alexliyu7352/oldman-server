"""Validate every paired row before comparing unprofiled measurements."""
import argparse
import json
from pathlib import Path
from statistics import median
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from probes.application_lifecycle.run import LIFECYCLE_ROLES, validate_experiment
from probes.framework_json.run import atomic_json


def paired_metrics(by_round, denominator="baseline"):
    def values(row):
        server, main = row["server_cpu_per_request_us"], row["python_main_cpu_per_request_us"]
        if server <= main:
            raise ValueError("invalid native CPU cost")
        return {"rps": row["client"]["rps"], "p99": row["client"]["p99_us"],
                "main_cpu": main, "server_cpu": server, "native_cpu": server-main}

    ratios = {}
    for number in sorted(by_round):
        a, b = (values(by_round[number][role]) for role in (denominator, "candidate"))
        for key in a:
            ratios.setdefault(key, []).append(b[key]/a[key])
    return {key: {"ratios": numbers, "median_ratio": median(numbers),
                  "max_symmetric_deviation": max(max(n, 1/n)-1 for n in numbers)}
            for key, numbers in ratios.items()}


def header_decision(rows, result, calibration):
    noise = summarize(calibration)
    if len(calibration) != 6 or noise["aa_within_five_percent"] is not True:
        raise ValueError("three valid noise calibration pairs required")
    if result["comparison_kind"] != "A/B" or result["rounds"] != list(range(6)):
        raise ValueError("six A/B pairs required")
    common = lambda row: {k:v for k,v in row["comparison_inputs"].items() if k != "harness"}
    if common(rows[0]) != common(calibration[0]):
        raise ValueError("calibration runtime/workload inputs changed")
    a = next(row for row in rows if row["role"] == "baseline")
    if a["snapshot_manifest"]["sha256"] != calibration[0]["snapshot_manifest"]["sha256"]:
        raise ValueError("calibration baseline snapshot changed")
    # Driver provenance can change when adding reporting and explicit loader
    # selection; the Python business and timed worker accounting cannot.
    old = calibration[0]["comparison_inputs"].get("harness", {}).get("sha256", {})
    new = rows[0]["comparison_inputs"].get("harness", {}).get("sha256", {})
    for path in ("probes/framework_json/server.py", "scripts/comparison_server.py",
                 "scripts/comparison_metrics.py", "scripts/benchmark_work.py"):
        if old.get(path) != new.get(path):
            raise ValueError("calibration timed source changed")
    ordered = sorted((r for r in rows if r["role"] in ("baseline", "candidate")),
                     key=lambda r:r["started_unix_ns"])
    expected = [(number, role) for number in range(6)
                for role in (("baseline", "candidate") if number % 2 == 0 else ("candidate", "baseline"))]
    if [(r["round"], r["role"]) for r in ordered] != expected:
        raise ValueError("declared ABBA sequence changed")
    if (any(r["finished_unix_ns"] <= r["started_unix_ns"] for r in ordered) or
            any(a["finished_unix_ns"] > b["started_unix_ns"] for a,b in zip(ordered, ordered[1:]))):
        raise ValueError("measurement windows overlap or have invalid timestamps")
    metrics = result["paired_metrics"]
    deviations = {key:value["max_symmetric_deviation"] for key,value in noise["paired_metrics"].items()}
    cost_threshold = max(.05, 2*deviations["native_cpu"])
    rps_threshold = max(.03, 2*deviations["rps"])
    reduction = 1-metrics["native_cpu"]["median_ratio"]
    cost = all(r <= 1 for r in metrics["native_cpu"]["ratios"]) and reduction >= cost_threshold
    throughput = (metrics["rps"]["median_ratio"]-1 > rps_threshold and
                  min(metrics["rps"]["ratios"]) >= 1/(1+deviations["rps"]))
    regressions = [key for key in ("main_cpu", "server_cpu", "p99")
                   if metrics[key]["median_ratio"] > 1+deviations[key]]
    decision = {"target": "native CPU/request = service minus Python main", "noise": deviations,
                "minimum_native_cpu_reduction": cost_threshold, "minimum_rps_gain": rps_threshold,
                "median_native_cpu_reduction": reduction, "native_cost_proven": cost,
                "throughput_improvement_proven": throughput, "stable_regressions": regressions,
                "keep_candidate": cost and throughput and not regressions}
    selected = "candidate" if decision["keep_candidate"] else "baseline"
    digest = next(row["snapshot_manifest"]["sha256"] for row in rows if row["role"] == selected)
    if any(row["snapshot_manifest"]["sha256"] != digest for row in rows
           if row["role"] not in ("baseline", "candidate")):
        raise ValueError("reference snapshot must match the selected implementation")
    return decision


def validate_sequence(rows, sequence):
    """Bind each role to its declared frozen input and chronological window."""
    timestamps = [sequence["declared_unix_ns"]]
    timestamps.extend(row[field] for row in rows for field in ("started_unix_ns", "finished_unix_ns"))
    if any(type(value) is not int or value <= 0 for value in timestamps):
        raise ValueError("timestamps must be positive integer nanoseconds")
    if set(sequence["snapshots"]) != {row["role"] for row in rows}:
        raise ValueError("declared snapshot roles differ from measurements")
    ordered = sorted(rows, key=lambda row: row["started_unix_ns"])
    if [[row["round"], row["role"]] for row in ordered] != sequence["order"]:
        raise ValueError("declared measurement sequence changed")
    if sequence["declared_unix_ns"] >= ordered[0]["started_unix_ns"]:
        raise ValueError("sequence must be declared before measurements")
    if (any(row["finished_unix_ns"] <= row["started_unix_ns"] for row in ordered) or
            any(a["finished_unix_ns"] > b["started_unix_ns"] for a, b in zip(ordered, ordered[1:]))):
        raise ValueError("measurement windows overlap or have invalid timestamps")
    for row in rows:
        if "host_observer" in sequence:
            common = row["comparison_inputs"]
            if common.get("host_observer") != sequence["host_observer"]:
                raise ValueError("measurement differs from declared host observer")
            hashes = common.get("harness", {}).get("sha256", {})
            if any(hashes.get(path) != digest for path, digest in sequence.get("tool_inputs", {}).items()):
                raise ValueError("measurement differs from declared tool inputs")
        binding = sequence["snapshots"][row["role"]]
        if (row["snapshot"] != binding["path"] or row["source_commit"] != binding["source_commit"] or
                row["snapshot_manifest"]["sha256"] != binding["manifest_sha256"]):
            raise ValueError("measurement differs from declared frozen snapshot")


def summarize(rows, *, calibration=None, sequence=None):
    if not rows:
        raise ValueError("no paired measurements")
    common = rows[0]["comparison_inputs"]
    by_round = {}
    snapshots = {}
    references = {}
    for row in rows:
        if row.get("diagnostic_only") is not False or row.get("experiment_validated") is not True:
            raise ValueError("diagnostic or unvalidated measurement")
        validate_experiment(row)
        if row["comparison_inputs"] != common:
            raise ValueError("comparison input mismatch")
        digest = row["snapshot_manifest"]["sha256"]
        if snapshots.setdefault(row["role"], digest) != digest:
            raise ValueError("snapshot changed within a role")
        if row["role"] not in LIFECYCLE_ROLES:
            numbers = references.setdefault(row["role"], [])
            numbers.append(row["round"])
            continue
        pair = by_round.setdefault(row["round"], {})
        if row["role"] in pair:
            raise ValueError("duplicate paired measurement")
        pair[row["role"]] = row
    if references and (set(references) != {"uvloop-stdlib", "uvloop-codec", "native"} or
                       any(sorted(numbers) != [0, 1, 2] for numbers in references.values())):
        raise ValueError("incomplete or duplicate reference matrix")
    incremental = "predecessor" in snapshots
    required_roles = set(LIFECYCLE_ROLES) if incremental else {"baseline", "candidate"}
    numbers = sorted(by_round)
    if (len(numbers) < 3 or numbers != list(range(numbers[0], numbers[-1] + 1)) or
            any(set(pair) != required_roles for pair in by_round.values())):
        raise ValueError("missing paired round/role")
    if incremental and (sequence is None or calibration is not None or references):
        raise ValueError("three-version comparison requires a sequence and excludes the old header/reference gate")
    if sequence is not None:
        validate_sequence(rows, sequence)
    ratios, cpu_ratios = [], []
    for number in numbers:
        baseline, candidate = (by_round[number][role] for role in ("baseline", "candidate"))
        ratios.append(candidate["client"]["rps"] / baseline["client"]["rps"])
        cpu_ratios.append(candidate["python_main_cpu_per_request_us"] /
                          baseline["python_main_cpu_per_request_us"])
    aa = not incremental and snapshots["baseline"] == snapshots["candidate"]
    result = {"comparison_kind": "A/P/C" if incremental else ("A/A" if aa else "A/B"),
            "rounds": numbers, "candidate_over_baseline": ratios,
            "paired_metrics": paired_metrics(by_round), "references_complete": bool(references),
            "main_cpu_candidate_over_baseline": cpu_ratios,
            "median_candidate_over_baseline": median(ratios),
            "all_candidate_rounds_faster": all(ratio > 1 for ratio in ratios),
            "aa_within_five_percent": all(max(ratio, 1 / ratio) <= 1.05 for ratio in ratios + cpu_ratios) if aa else None,
            "roles": {role: {"median_rps": median(r["client"]["rps"] for r in rows if r["role"] == role),
                             "rps": [r["client"]["rps"] for r in rows if r["role"] == role],
                             "p99_us": [r["client"]["p99_us"] for r in rows if r["role"] == role],
                             "server_cpu_per_request_us": [r["server_cpu_per_request_us"] for r in rows if r["role"] == role],
                             "python_main_cpu_per_request_us": [r["python_main_cpu_per_request_us"] for r in rows if r["role"] == role]}
                      for role in sorted({row["role"] for row in rows})}}
    if incremental:
        result["incremental_paired_metrics"] = paired_metrics(by_round, "predecessor")
    if sequence is not None:
        result["sequence"] = sequence
    if calibration is not None:
        result["header_decision"] = header_decision(rows, result, calibration)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--calibration", type=Path, help="A/A directory for the fixed header experiment decision")
    parser.add_argument("--sequence", type=Path, help="predeclared window order and frozen role bindings (required with predecessor)")
    args = parser.parse_args(argv)
    path = args.out / "summary.json"
    if path.exists():
        raise FileExistsError(path)
    rows = [json.loads(p.read_text()) for p in sorted(args.out.rglob("json-*-r*.json"))]
    calibration = ([json.loads(p.read_text()) for p in sorted(args.calibration.rglob("json-*-r*.json"))]
                   if args.calibration else None)
    sequence = json.loads(args.sequence.read_text()) if args.sequence else None
    result = summarize(rows, calibration=calibration, sequence=sequence)
    atomic_json(path, result)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
