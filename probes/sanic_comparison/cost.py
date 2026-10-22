"""Fixed low-rate main-thread diagnosis; never a throughput benchmark."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time

from probes.sanic_comparison.run import (ROOT, EXPECTED, boundary, describe_cpu,
    fingerprints, runtime_fingerprint, atomic_json, execute)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--smoke', action='store_true')
    args = parser.parse_args()
    out = args.out.resolve(); out.mkdir(parents=True, exist_ok=False)
    budget = describe_cpu(); layout, topology = boundary.layout_for(budget)
    source, runtime = fingerprints(), runtime_fingerprint()
    modes = ('candidate', 'stock') if args.smoke else ('candidate', 'stock', 'stock', 'candidate')
    order = [[f'{workload}-{i}-{mode}', mode, workload]
             for workload in EXPECTED for i, mode in enumerate(modes)]
    settings = {'seconds': .3 if args.smoke else 2, 'warmup': .1 if args.smoke else 1,
                'rate': 1000, 'diagnostic': 'cprofile'}
    atomic_json(out/'manifest.json', {'source': source, 'runtime': runtime, 'order': order,
        'cpu_budget': budget, 'layout': layout, 'topology': topology, 'settings': settings,
        'smoke': args.smoke, 'created_ns': time.time_ns(), 'scope': 'Instrumented diagnosis only',
        'git_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()})
    affinity = os.sched_getaffinity(0); os.sched_setaffinity(0, set(layout['controller']))
    try:
        for key, mode, workload in order:
            row = execute(out/key, mode, workload, layout=layout, budget=budget, runtime=runtime, **settings)
            print(json.dumps({'key': key, 'requests': row['client']['requests'],
                              'profiles': len(row['server']['workers'])}), flush=True)
        if source != fingerprints() or runtime != runtime_fingerprint():
            raise ValueError('inputs changed')
        atomic_json(out/'complete.json', {'windows': len(order), 'fingerprints_unchanged': True})
    finally:
        os.sched_setaffinity(0, affinity)


if __name__ == '__main__': main()
