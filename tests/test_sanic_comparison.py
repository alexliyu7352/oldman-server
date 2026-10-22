"""Comparison contracts; Seastar services are exercised as separate processes."""
from copy import deepcopy
import pytest

from probes.sanic_comparison.run import validate_response, validate_counts
from probes.sanic_comparison.server import verify_stock


def test_python_only_order_balances_both_services_without_native():
    from probes.sanic_comparison import run
    assert hasattr(run, 'case_order')
    order = run.case_order(python_only=True)
    assert len(order) == 30 and len({key for key, _, _ in order}) == 30
    for workload in run.EXPECTED:
        rows = [row for row in order if row[2] == workload]
        assert [row[1] for row in rows[:2]] == ['candidate', 'candidate']
        pairs = [rows[index:index+2] for index in range(2, 10, 2)]
        assert [[row[1] for row in pair] for pair in pairs] == [
            ['candidate', 'stock'], ['stock', 'candidate'],
            ['candidate', 'stock'], ['stock', 'candidate']]
        for repeat, pair in enumerate(pairs):
            assert {key for key, _, _ in pair} == {
                f'{workload}-r{repeat}-candidate', f'{workload}-r{repeat}-stock'}
    smoke = run.case_order(python_only=True, smoke=True)
    assert len(smoke) == 6
    assert {(mode, workload) for _, mode, workload in smoke} == {
        (mode, workload) for mode in ('candidate', 'stock') for workload in run.EXPECTED}


def test_default_comparison_order_preserves_historical_windows():
    import itertools
    from probes.sanic_comparison import run
    assert hasattr(run, 'case_order')
    expected = []
    for workload in run.EXPECTED:
        expected.extend((f'{workload}-aa-{i}', 'candidate', workload) for i in range(2))
        for repeat, modes in enumerate(itertools.permutations(run.MODES)):
            expected.extend((f'{workload}-r{repeat}-{mode}', mode, workload) for mode in modes)
    assert run.case_order() == expected
    assert len(run.case_order()) == 60
    assert len(run.case_order(smoke=True)) == 9


def test_stock_source_is_exact_upstream():
    root = verify_stock()
    assert (root/'sanic/app.py').is_file()


def test_stock_http_path_starts_without_fork_adapter(tmp_path):
    from probes.sanic_comparison.run import execute
    from probes.sanic_boundary.run import layout_for
    from probes.application_lifecycle.inputs import runtime_fingerprint
    from scripts.cpu_budget import describe_cpu
    budget = describe_cpu(); layout, _ = layout_for(budget)
    row = execute(tmp_path/'stock', 'stock', 'small', layout=layout, budget=budget,
                  runtime=runtime_fingerprint(), warmup=.1, seconds=.15)
    assert row['validated'] and len(row['ready']['pids']) == 3
    assert all(worker['sanic_version'] == '25.12.1' for worker in row['ready']['workers'])
    assert all(worker['exitcode'] == 0 and not worker['forced']
               for worker in row['closed']['worker_exits'])


def test_framing_preserves_complete_response_semantics():
    headers = {'content-type': 'application/json', 'content-length': '26', 'x-probe-pid': '123'}
    body = b'{"message":"Hello, World!"}'
    headers['content-length'] = str(len(body))
    assert validate_response(200, headers, body, [123], 'small') == 'content-length'
    chunked = headers.copy(); del chunked['content-length']; chunked['transfer-encoding'] = 'chunked'
    assert validate_response(200, chunked, body, [123], 'small') == 'chunked'
    for key, value in [('content-length', '1'), ('content-encoding', 'gzip'), ('connection', 'close'),
                       ('x-probe-pid', '456')]:
        bad = headers | {key: value}
        with pytest.raises(ValueError):
            validate_response(200, bad, body, [123], 'small')


def valid_row():
    return {'mode': 'stock', 'workload': 'small', 'concurrency': 128, 'seconds': 5,
      'ready': {'pids': [11, 12, 13], 'service_pids': [10, 11, 12, 13]},
      'client': {'requests': 300, 'scheduled': 300, 'errors': 0, 'integrity_errors': 0,
        'status_errors': 0, 'dropped': 0, 'new_connections': 0, 'total_connections': 128,
        'response_bytes': 300*27, 'server_pids': {'11': 100, '12': 100, '13': 100},
        'arrival_seconds': 5, 'seconds': 5.001, 'rate': 0, 'rps': 60, 'p99_us': 1000},
      'server': {'workers': [{'pid': pid, 'delta': {'responses': 100, 'started': 100, 'finished': 100},
                              'after': {'active': 0, 'errors': 0}} for pid in [11, 12, 13]]},
      'closed': {'closed': True, 'workers': [{'active': 0, 'errors': 0} for _ in range(3)]},
      'returncodes': [0, 0], 'service_cpu_seconds': .1}


def test_counts_reject_partial_workers_and_errors():
    row = valid_row(); validate_counts(row)
    for key, value in [('server_pids', {'11': 300}), ('requests', 299), ('arrival_seconds', 3),
                       ('p99_us', float('nan')), ('errors', 1)]:
        bad = deepcopy(row); bad['client'][key] = value
        with pytest.raises(ValueError): validate_counts(bad)
    row['closed']['workers'][0]['active'] = 1
    with pytest.raises(ValueError): validate_counts(row)


def test_counter_window_handles_non_numeric_engine_metadata():
    from probes.sanic_comparison.server import CounterWindow
    values = {'responses': 3, 'shards': [{'id': 0}]}
    window = CounterWindow(lambda: dict(values))
    window.begin()
    values['responses'] = 8
    result = window.end()
    assert result['delta'] == {'responses': 5}
    assert result['after']['shards'] == [{'id': 0}]


def test_native_reference_starts_and_closes_in_fresh_process():
    import json
    import os
    import subprocess
    from probes.sanic_comparison.run import ROOT
    from probes.sanic_spike.run import free_port
    cpus = ','.join(map(str, sorted(os.sched_getaffinity(0))[:3]))
    result = subprocess.run(['taskset', '-c', cpus, str(ROOT/'build/sanic_baseline_native'),
        str(free_port()), cpus], input='quit\n', capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    rows = [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
    assert rows[0]['ready'] and rows[-1]['closed']
    assert len(rows[0]['workers'][0]['shards']) == 3
    assert rows[-1]['workers'][0]['active'] == 0


def test_native_reference_serves_the_sleep_workload(tmp_path):
    from probes.sanic_comparison.run import execute
    from probes.sanic_boundary.run import layout_for
    from probes.application_lifecycle.inputs import runtime_fingerprint
    from scripts.cpu_budget import describe_cpu
    budget = describe_cpu(); layout, _ = layout_for(budget)
    row = execute(tmp_path/'sleep', 'native', 'sleep', layout=layout, budget=budget,
                  runtime=runtime_fingerprint(), warmup=.1, seconds=.15)
    assert row['validated'] and row['concurrency'] == 512 and row['client']['requests'] > 512


def test_stock_source_rejects_changed_original_file(tmp_path, monkeypatch):
    import hashlib
    import json
    from probes.sanic_comparison import server
    monkeypatch.setattr(server, 'ROOT', tmp_path)
    metadata = tmp_path/'upstream/sanic.json'
    metadata.parent.mkdir(parents=True)
    metadata.write_text(json.dumps({'commit': 'fixed', 'original_sha256': {
        'sanic/app.py': hashlib.sha256(b'original').hexdigest()}}))
    source = tmp_path/'build/sanic-upstream/sanic-fixed/sanic/app.py'
    source.parent.mkdir(parents=True); source.write_bytes(b'changed')
    with pytest.raises(ValueError, match='source mismatch'):
        server.verify_stock()


def test_prefork_rejects_worker_failure_after_closed_snapshot(monkeypatch, capsys):
    import io
    import json
    import os
    from types import SimpleNamespace
    import comparison_server as comparison
    def child(args, cpu, requests, byte_limit, pipe, inherited):
        for other in inherited: other.close()
        pipe.send({'pid': os.getpid()})
        assert pipe.recv() == 'quit'
        pipe.send({'closed': True, 'workers': [{'active': 0, 'errors': 0}]})
        pipe.close()
        raise SystemExit(7)
    monkeypatch.setattr(comparison, 'child', child)
    monkeypatch.setattr(comparison.sys, 'stdin', io.StringIO('quit\n'))
    affinity = os.sched_getaffinity(0)
    try:
        with pytest.raises(RuntimeError, match='worker'):
            comparison.prefork(SimpleNamespace(requests=1, bytes=1024, mode='stock'), [min(affinity)])
    finally:
        os.sched_setaffinity(0, affinity)
    result = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert not result['closed'] and result['worker_exits'][0]['exitcode'] == 7


def test_native_reference_reports_busy_port_without_abort():
    import os
    import socket
    import subprocess
    from probes.sanic_comparison.run import ROOT
    from probes.sanic_spike.run import free_port
    cpus = ','.join(map(str, sorted(os.sched_getaffinity(0))[:3]))
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0)); listener.listen()
        port = listener.getsockname()[1]
        result = subprocess.run(['taskset', '-c', cpus, str(ROOT/'build/sanic_baseline_native'),
            str(port), cpus, str(free_port())], input='quit\n', capture_output=True, text=True, timeout=20)
    assert result.returncode > 0, result.stderr
    assert 'Assertion' not in result.stderr and 'Aborting on shard' not in result.stderr


def test_forced_worker_exit_cannot_be_clean():
    import multiprocessing
    import time
    import comparison_server as comparison
    worker = multiprocessing.get_context('fork').Process(target=time.sleep, args=(60,))
    worker.start()
    result = comparison.finish_workers([worker], timeout=.01)
    assert result[0]['forced'] and result[0]['exitcode'] != 0
    assert not worker.is_alive()
