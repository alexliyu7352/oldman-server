"""One matched App/Sanic workload; fixed throughput windows precede diagnostics."""
import argparse
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import time

from probes.sanic_boundary import application
from probes.sanic_comparison import server
from probes.sanic_comparison.run import (ROOT, boundary, describe_cpu, fingerprints,
    runtime_fingerprint, atomic_json, execute, validate_counts, validate_profiles)


def case_order(smoke=False):
    order = [] if smoke else [('small-aa-0', 'candidate', 'none'),
                              ('small-aa-1', 'candidate', 'none')]
    for repeat in range(1 if smoke else 3):
        modes = ('app', 'candidate') if repeat % 2 == 0 else ('candidate', 'app')
        order.extend((f'small-r{repeat}-{mode}', mode, 'none') for mode in modes)
    order.extend((f'small-profile-{mode}', mode, 'cprofile') for mode in ('app', 'candidate'))
    return order


def validate_contract(row):
    mode = row['mode']
    if mode not in ('app', 'candidate') or row['workload'] != 'small':
        raise ValueError('unsupported attribution service/workload')
    if len(row['ready']['workers']) != 1:
        raise ValueError('attribution requires one worker')
    contract = row['ready']['workers'][0]['application_contract']
    file = Path(application.__file__).resolve()
    codec = 'stdlib' if mode == 'app' else 'oldman_server.response.json'
    if (contract['json_codec'] != codec or contract['native_responses'] is not False or
            contract['bytes_headers'] is not (mode == 'candidate') or
            contract['business'] != {'path': str(file),
                                     'sha256': hashlib.sha256(file.read_bytes()).hexdigest()}):
        raise ValueError('business/response boundary mismatch')
    actual = json.loads(json.dumps(contract['server_config']))
    actual['http_port'] = 0
    expected = json.loads(json.dumps(asdict(server.native_config(0, [0, 1, 2], mode).server)))
    if mode == 'app':
        expected['application_routes'] = [['GET', '/small'], ['HEAD', '/small']]
    if actual != expected:
        raise ValueError('native server configuration mismatch')
    if not row['preflight'] or any(item['framing'] != 'chunked' for item in row['preflight']):
        raise ValueError('attribution requires matching chunked responses')


def metrics(row):
    client = row['client']
    ticks = sum(row['threads_after'][str(pid)][str(pid)]['ticks'] -
                row['threads_before'][str(pid)][str(pid)]['ticks'] for pid in row['ready']['pids'])
    if ticks <= 0:
        raise ValueError('missing positive Python main-thread CPU')
    cpu = ticks/os.sysconf('SC_CLK_TCK')
    return {'rps': client['rps'], 'service_cpu_us': row['service_cpu_us_per_request'],
            'main_cpu_us': cpu*1e6/client['requests'], 'p99_us': client['p99_us'],
            'main_cores': cpu/client['seconds'],
            'service_cores': row['service_cpu_seconds']/client['seconds'],
            'client_cores': client['cpu_cores']}


def validate_settings(row, *, smoke):
    diagnostic = row.get('diagnostic', 'none')
    profile = diagnostic == 'cprofile'
    expected = (.1 if smoke else (1 if profile else 2),
                .3 if smoke else (2 if profile else 5), 1000 if profile else 0, 128)
    actual = (row['warmup'], row['seconds'], row.get('rate', 0), row['concurrency'])
    if diagnostic not in ('none', 'cprofile') or actual != expected:
        raise ValueError('attribution window settings mismatch')


def summarize(out):
    manifest = json.loads((out/'manifest.json').read_text())
    order = case_order(manifest['smoke'])
    if manifest['order'] != [list(item) for item in order]:
        raise ValueError('attribution order changed')
    if {p.parent.name for p in out.glob('*/row.json')} != {key for key, _, _ in order}:
        raise ValueError('missing or extra attribution window')
    rows, profiles, hashes = {}, {}, {}
    for key, mode, diagnostic in order:
        path = out/key/'row.json'
        row = json.loads(path.read_text())
        if (not row['validated'] or 'failure' in row or row['mode'] != mode or
                row.get('diagnostic', 'none') != diagnostic or row['layout'] != manifest['layout']):
            raise ValueError('invalid attribution window')
        validate_settings(row, smoke=manifest['smoke'])
        validate_counts(row)
        validate_profiles(row, out/key)
        validate_contract(row)
        hashes[str(path.relative_to(out))] = hashlib.sha256(path.read_bytes()).hexdigest()
        if diagnostic == 'none':
            rows[key] = metrics(row)
        else:
            worker = row['server']['workers'][0]
            profiles[mode] = {'requests': row['client']['requests'], 'calls': worker['profile']['calls']}
    pairs = []
    for repeat in range(1 if manifest['smoke'] else 3):
        app, sanic = rows[f'small-r{repeat}-app'], rows[f'small-r{repeat}-candidate']
        pairs.append({'repeat': repeat, 'app': app, 'sanic': sanic,
                      'sanic_over_app': {key: sanic[key]/app[key] for key in
                          ('rps', 'service_cpu_us', 'main_cpu_us', 'p99_us')}})
    result = {'scope': 'Current complete Sanic integration increment; admission and header representations differ',
              'unprofiled_windows': len(rows), 'diagnostic_windows': len(profiles),
              'pairs': pairs, 'profiles': profiles, 'input_sha256': hashes,
              'paired_median_sanic_over_app': {key: statistics.median(
                  pair['sanic_over_app'][key] for pair in pairs) for key in pairs[0]['sanic_over_app']}}
    if not manifest['smoke']:
        a, b = rows['small-aa-0'], rows['small-aa-1']
        result['aa_second_over_first'] = {key: b[key]/a[key] for key in
            ('rps', 'service_cpu_us', 'main_cpu_us', 'p99_us')}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--smoke', action='store_true')
    args = parser.parse_args()
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    budget = describe_cpu()
    layout, topology = boundary.layout_for(budget)
    source, runtime = fingerprints(), runtime_fingerprint()
    order = case_order(args.smoke)
    atomic_json(out/'manifest.json', {'source': source, 'runtime': runtime, 'order': order,
        'layout': layout, 'topology': topology, 'cpu_budget': budget, 'smoke': args.smoke,
        'created_ns': time.time_ns(), 'git_head': subprocess.check_output(
            ['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        'limits': ['App native admission differs from Sanic dispatch-all',
                   'App str headers differ from Sanic bytes headers',
                   'Profile timing and low-rate batching are not throughput evidence']})
    affinity = os.sched_getaffinity(0)
    os.sched_setaffinity(0, set(layout['controller']))
    try:
        for key, mode, diagnostic in order:
            profile = diagnostic == 'cprofile'
            row = execute(out/key, mode, 'small', layout=layout, budget=budget, runtime=runtime,
                warmup=.1 if args.smoke else (1 if profile else 2),
                seconds=.3 if args.smoke else (2 if profile else 5),
                rate=1000 if profile else 0, diagnostic=diagnostic)
            validate_contract(row)
            print(json.dumps({'key': key, 'requests': row['client']['requests'],
                              'rps': row['client']['rps']}), flush=True)
        if source != fingerprints() or runtime != runtime_fingerprint():
            raise ValueError('attribution inputs changed')
        atomic_json(out/'summary.json', summarize(out))
        atomic_json(out/'complete.json', {'windows': len(order), 'fingerprints_unchanged': True})
    finally:
        os.sched_setaffinity(0, affinity)


if __name__ == '__main__':
    main()
