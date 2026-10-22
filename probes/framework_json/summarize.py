"""Summarize validated paired JSON comparison rows without hiding any round."""
import argparse
import json
from pathlib import Path
from statistics import median
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from probes.framework_json.run import MODES, atomic_json


def summarize(rows):
    if not rows:
        raise ValueError("no measurement rows")
    by_round = {}
    for row in rows:
        if row.get("validated") is not True:
            raise ValueError(f"unvalidated row {row.get('key')}")
        pair = by_round.setdefault(row["round"], {})
        mode = row["mode"]
        if mode not in MODES or mode in pair:
            raise ValueError(f"unexpected/duplicate mode {mode} in round {row['round']}")
        pair[mode] = row
    for number, pair in by_round.items():
        if set(pair) != set(MODES):
            raise ValueError(f"round {number} missing modes: {sorted(set(MODES)-set(pair))}")
    numbers = sorted(by_round)
    if numbers != list(range(numbers[0], numbers[-1]+1)):
        raise ValueError("missing paired round")
    modes = {}
    for mode in MODES:
        values = [by_round[n][mode] for n in numbers]
        modes[mode] = {"median_rps": median(v["client"]["rps"] for v in values),
                       "median_p99_ms": median(v["client"]["p99_us"]/1000 for v in values),
                       "median_server_cpu_per_request_us": median(v["server_cpu_per_request_us"] for v in values),
                       "rps_per_round": [v["client"]["rps"] for v in values],
                       "p99_ms_per_round": [v["client"]["p99_us"]/1000 for v in values]}
    ratios = [by_round[n]["candidate3"]["client"]["rps"] /
              by_round[n]["multiprocess"]["client"]["rps"] for n in numbers]
    all_rounds_ge1 = all(ratio >= 1 for ratio in ratios)
    return {"rounds": numbers, "modes": modes,
            "paired_candidate3_over_multiprocess": ratios,
            "median_paired_candidate3_over_multiprocess": median(ratios),
            "all_rounds_ge1": all_rounds_ge1,
            "candidate3_meets_multiprocess": all_rounds_ge1,
            "candidate1_is_one_cpu": True,
            "comparison": "local TFB-style JSON; not an official TFB result"}


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True, help="directory under results/")
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[2] / "results" / args.out
    rows = [json.loads(p.read_text()) for p in sorted(root.glob("json-*-r*.json"))]
    summary = summarize(rows)
    atomic_json(root / "summary.json", summary)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
