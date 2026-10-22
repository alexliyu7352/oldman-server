"""Separate intrusive Python and sampled native diagnostics from formal rows."""
import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from probes.application_lifecycle.run import execute


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--baseline-index", type=Path, required=True)
    args = parser.parse_args(argv)
    args.out.mkdir(parents=True, exist_ok=False)
    for profiler in ("cprofile", "perf"):
        execute(args.snapshot, "candidate", 0, args.out / profiler, profiler=profiler,
                baseline_index=args.baseline_index)


if __name__ == "__main__":
    main()
