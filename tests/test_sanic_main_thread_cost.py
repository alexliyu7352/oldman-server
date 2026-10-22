"""Diagnostic contracts; every native runtime is owned by a fresh child."""
import asyncio
from copy import deepcopy
import hashlib
import os
import pstats
import sys

import pytest

from test_sanic_comparison import valid_row
from probes.sanic_comparison import server, run


def inside_window():
    return sum(i*i for i in range(100))


def outside_window():
    return sum(i for i in range(100))


def test_profile_window_records_real_calls_and_owns_file(tmp_path):
    values = {'responses': 0, 'shards': []}
    window = server.ProfileWindow(lambda: dict(values), tmp_path)
    outside_window()
    try:
        window.begin()
        inside_window(); values['responses'] += 1
        result = window.end()
    finally:
        asyncio.run(window.close())
    profile = result['profile']
    file = tmp_path/profile['file']
    assert profile['pid'] == os.getpid()
    assert profile['timer'] == 'thread_time' and result['thread_cpu_ns'] > 0
    assert profile['sha256'] == hashlib.sha256(file.read_bytes()).hexdigest()
    entries = profile['calls']
    assert sum(e['calls'] for e in entries if e['function'] == 'inside_window') == 1
    assert not any(e['function'] == 'outside_window' for e in entries)
    assert len(pstats.Stats(str(file)).stats) == len(entries)
    assert result['delta'] == {'responses': 1} and sys.getprofile() is None


def test_profile_close_disables_even_without_end(tmp_path):
    window = server.ProfileWindow(lambda: {'responses': 0}, tmp_path)
    with pytest.raises(ValueError): window.end()
    try:
        window.begin()
        with pytest.raises(ValueError): window.begin()
    finally:
        asyncio.run(window.close())
    assert sys.getprofile() is None
    with pytest.raises(ValueError): window.end()


def test_fixed_rate_requires_exact_offered_count():
    row = valid_row()
    row['rate'] = row['client']['rate'] = 60
    run.validate_counts(row)
    row['rate'] = row['client']['rate'] = 61
    with pytest.raises(ValueError, match='offered'):
        run.validate_counts(row)


def test_profile_validator_checks_every_worker_and_actual_file(tmp_path):
    row = valid_row(); row['diagnostic'] = 'cprofile'
    for worker in row['server']['workers']:
        profiler = __import__('cProfile').Profile()
        profiler.runcall(inside_window)
        file = tmp_path/f'profile-{worker["pid"]}.pstats'
        profiler.dump_stats(str(file))
        worker['thread_cpu_ns'] = 1_000_000
        worker['profile'] = {'pid': worker['pid'], 'file': file.name, 'timer': 'thread_time',
            'sha256': hashlib.sha256(file.read_bytes()).hexdigest(),
            'calls': server.profile_records(pstats.Stats(str(file)))}
    run.validate_profiles(row, tmp_path)
    for change in ('missing', 'pid', 'hash', 'calls', 'clock', 'unprofiled'):
        bad = deepcopy(row)
        last = bad['server']['workers'][-1]
        if change == 'missing': del last['profile']
        if change == 'pid': last['profile']['pid'] = 999
        if change == 'hash': last['profile']['sha256'] = '0'*64
        if change == 'calls': last['profile']['calls'] = []
        if change == 'clock': last['thread_cpu_ns'] = 0
        if change == 'unprofiled': bad['diagnostic'] = 'none'
        with pytest.raises(ValueError): run.validate_profiles(bad, tmp_path)
    (tmp_path/row['server']['workers'][-1]['profile']['file']).write_bytes(b'changed')
    with pytest.raises(ValueError): run.validate_profiles(row, tmp_path)


@pytest.mark.parametrize('mode', ['stock', 'candidate'])
def test_real_workers_profile_real_http_and_close(tmp_path, mode):
    from probes.application_lifecycle.inputs import runtime_fingerprint
    from scripts.cpu_budget import describe_cpu
    budget = describe_cpu(); layout, _ = run.boundary.layout_for(budget)
    row = run.execute(tmp_path/mode, mode, 'sleep', layout=layout, budget=budget,
        runtime=runtime_fingerprint(), warmup=.1, seconds=.3, rate=300, diagnostic='cprofile')
    assert row['validated'] and row['client']['requests'] == 90
    for worker in row['server']['workers']:
        assert worker['delta']['responses'] > 0
        assert any(c['function'] == 'sleep' and c['file'].endswith('sanic_boundary/application.py')
                   for c in worker['profile']['calls'])
