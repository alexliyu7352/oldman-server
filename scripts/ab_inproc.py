"""Alternate scripts/bench_inproc.py between a base git ref and the working tree.

The base ref is checked out once as a detached worktree under build/ab-worktrees
(the native extension in build/ is shared, so A/B covers Python-side changes).
Desktop background load makes single runs noisy; compare the per-side minimum
and median over alternating rounds.

usage: ab_inproc.py BASE_REF ROUNDS CPU mode:workload [mode:workload ...]
"""
import json
import os
import statistics
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    ref, rounds, cpu, cases = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4:]
    sha = subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", ref], text=True).strip()
    # Both sides load the one native extension built in this tree, so a change
    # to native/ makes the base run against an interface it was not written for.
    if subprocess.run(["git", "-C", str(ROOT), "diff", "--quiet", sha, "--", "native"]).returncode:
        sys.exit("native/ differs from the base: in-process A/B shares one native build; "
                 "use scripts/calibrate.py instead")
    base = ROOT / "build/ab-worktrees" / sha[:10]
    if not base.exists():
        subprocess.check_call(["git", "-C", str(ROOT), "worktree", "add", "--detach", str(base), sha],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    extension = next((ROOT / "src/oldman_server/native").glob("_native.*.so"))
    if not (base / "src/oldman_server").is_dir():
        sys.exit("the base predates the oldman_server package layout")
    linked = base / "src/oldman_server/native" / extension.name
    if not linked.exists():
        linked.symlink_to(extension)

    def run(tree, mode, workload):
        env = dict(os.environ, PYTHONPATH=f"{tree}/src:{tree}/python:{ROOT}/build")
        out = subprocess.check_output(
            ["taskset", "-c", cpu, str(ROOT / ".venv/bin/python"), str(tree / "scripts/bench_inproc.py"),
             "--mode", mode, "--workload", workload, "--requests", "40000", "--repeats", "3"],
            env=env, text=True, cwd=str(tree), stderr=subprocess.DEVNULL, timeout=600)
        return json.loads(out.strip().splitlines()[-1])["cpu_us_min"]

    for case in cases:
        mode, workload = case.split(":")
        a, b = [], []
        for i in range(rounds):
            order = ((a, base), (b, ROOT)) if i % 2 == 0 else ((b, ROOT), (a, base))
            for side, tree in order:
                side.append(run(tree, mode, workload))
        print(json.dumps({"case": case, "base": sha[:10],
                          "base_min": min(a), "base_med": round(statistics.median(a), 3),
                          "new_min": min(b), "new_med": round(statistics.median(b), 3),
                          "delta_min": round(min(b) - min(a), 3),
                          "delta_med": round(statistics.median(b) - statistics.median(a), 3)}),
              flush=True)


if __name__ == "__main__":
    main()
