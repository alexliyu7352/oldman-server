"""Matched workload and native App gates; runtimes always live in child processes."""
import asyncio
from copy import deepcopy
import importlib
from types import SimpleNamespace

import pytest

from probes.sanic_boundary import application
from probes.sanic_comparison import run, server
from test_sanic_comparison import valid_row


def attribution():
    assert importlib.util.find_spec('probes.sanic_comparison.framework_attribution') is not None
    return importlib.import_module('probes.sanic_comparison.framework_attribution')


def app_row():
    row = valid_row()
    row['mode'] = 'app'
    row['ready']['pids'] = row['ready']['service_pids'] = [11]
    row['client']['server_pids'] = {'11': 300}
    stats = dict(active=0, errors=0, engine_python_application_errors=0)
    stats.update({'engine_'+key: 0 for key in run.boundary.ZERO})
    delta = dict(responses=300, started=300, finished=300,
                 engine_responses=300, engine_python_application_started=300,
                 engine_python_application_finished=300)
    row['server']['workers'] = [dict(pid=11, after=deepcopy(stats), delta=delta)]
    row['closed']['workers'] = [deepcopy(stats)]
    return row


def test_shared_small_business_serializes_fresh_values(monkeypatch):
    assert hasattr(application, 'small_body')
    values = []
    dumps = application.json.dumps
    def observe(value, **kwargs):
        values.append(value)
        assert kwargs == {'separators': (',', ':')}
        return dumps(value, **kwargs)
    monkeypatch.setattr(application.json, 'dumps', observe)
    assert [application.small_body() for _ in range(3)] == [application.EXPECTED['small']]*3
    assert len({id(value) for value in values}) == 3


def test_lightweight_handler_uses_shared_business_and_balanced_counts(monkeypatch):
    assert hasattr(server, 'make_lightweight_app')
    counts = dict(started=0, finished=0, responses=0, errors=0, active=0)
    calls = []
    monkeypatch.setattr(application, 'small_body', lambda: calls.append(1) or application.EXPECTED['small'])
    app = server.make_lightweight_app(counts)
    status, body, headers = asyncio.run(app._dispatch(SimpleNamespace(method='GET', path='/small')))
    assert (status, body, headers) == (200, application.EXPECTED['small'],
                                      (('Content-Type', 'application/json'),))
    assert calls == [1] and app._native_responses is False
    assert counts == dict(started=1, finished=1, responses=1, errors=0, active=0)


@pytest.mark.parametrize('error', [ValueError, asyncio.CancelledError])
def test_lightweight_failure_clears_active_and_counts_error(monkeypatch, error):
    assert hasattr(server, 'make_lightweight_app')
    def fail():
        raise error('injected')
    monkeypatch.setattr(application, 'small_body', fail)
    counts = dict(started=0, finished=0, responses=0, errors=0, active=0)
    app = server.make_lightweight_app(counts)
    with pytest.raises(error):
        asyncio.run(app._dispatch(SimpleNamespace(method='GET', path='/small')))
    assert counts == dict(started=1, finished=1, responses=0, errors=1, active=0)


@pytest.mark.parametrize('key', [*run.boundary.ZERO, 'python_application_errors'])
def test_app_mode_rejects_native_leaks_and_errors(key):
    row = app_row()
    run.validate_counts(row)
    row['closed']['workers'][0]['engine_'+key] = 1
    with pytest.raises(ValueError):
        run.validate_counts(row)


@pytest.mark.parametrize('key', ['responses', 'python_application_started', 'python_application_finished'])
def test_app_mode_rejects_native_count_mismatch(key):
    row = app_row()
    row['server']['workers'][0]['delta']['engine_'+key] -= 1
    with pytest.raises(ValueError):
        run.validate_counts(row)


def test_order_fixes_eight_unprofiled_and_two_mechanism_windows():
    tool = attribution()
    order = tool.case_order()
    assert len(order) == len({key for key, *_ in order}) == 10
    assert [mode for _, mode, diagnostic in order[:8]] == [
        'candidate', 'candidate', 'app', 'candidate', 'candidate', 'app', 'app', 'candidate']
    assert [diagnostic for _, _, diagnostic in order[:8]] == ['none']*8
    assert [(mode, diagnostic) for _, mode, diagnostic in order[8:]] == [
        ('app', 'cprofile'), ('candidate', 'cprofile')]
    assert len(tool.case_order(smoke=True)) == 4


def test_contract_rejects_codec_boundary_config_and_business_mismatch():
    tool = attribution()
    assert hasattr(server, 'native_config') and hasattr(server, 'application_contract')
    from dataclasses import replace
    config = server.native_config(1234, [0, 1, 2], 'app')
    config = replace(config, server=replace(config.server,
        application_routes=(('GET', '/small'), ('HEAD', '/small'))))
    engine = SimpleNamespace(config=config, _application_native_results=False,
                             _application_bytes_headers=False)
    contract = server.application_contract(engine, 'app')
    row = app_row()
    row['ready']['workers'] = [{'application_contract': contract}]
    row['preflight'] = [{'framing': 'chunked'}]
    tool.validate_contract(row)
    for field, value in [('json_codec', 'native'), ('native_responses', True), ('bytes_headers', True)]:
        bad = deepcopy(row)
        bad['ready']['workers'][0]['application_contract'][field] = value
        with pytest.raises(ValueError):
            tool.validate_contract(bad)
    bad = deepcopy(row)
    bad['ready']['workers'][0]['application_contract']['server_config']['max_bytes'] -= 1
    with pytest.raises(ValueError):
        tool.validate_contract(bad)
    bad = deepcopy(row)
    bad['ready']['workers'][0]['application_contract']['business']['sha256'] = '0'*64
    with pytest.raises(ValueError):
        tool.validate_contract(bad)


def test_legacy_engine_contract_preserves_str_headers():
    # The f2bcd29/3ed0e27 backend snapshots predate the bytes-header flag.
    engine = SimpleNamespace(config=server.native_config(1234, [0, 1, 2], 'candidate'),
                             _application_native_results=False)
    contract = server.application_contract(engine, 'candidate')
    assert contract['bytes_headers'] is False
    row = app_row()
    row['mode'] = 'candidate'
    row['ready']['workers'] = [{'application_contract': contract}]
    row['preflight'] = [{'framing': 'chunked'}]
    with pytest.raises(ValueError, match='boundary'):
        attribution().validate_contract(row)


def test_window_settings_reject_valid_but_different_measurement_conditions():
    tool = attribution()
    assert hasattr(tool, 'validate_settings')
    row = dict(diagnostic='none', rate=0, warmup=2, seconds=5, concurrency=128)
    tool.validate_settings(row, smoke=False)
    for key, value in [('warmup', 1), ('seconds', 6), ('rate', 1000), ('concurrency', 256)]:
        with pytest.raises(ValueError, match='settings'):
            tool.validate_settings(row | {key: value}, smoke=False)
    profile = row | dict(diagnostic='cprofile', rate=1000, warmup=1, seconds=2)
    tool.validate_settings(profile, smoke=False)
    with pytest.raises(ValueError, match='settings'):
        tool.validate_settings(profile | {'rate': 900}, smoke=False)


def test_real_app_http_checked_path_and_profile_close_in_child(tmp_path):
    tool = attribution()
    from probes.application_lifecycle.inputs import runtime_fingerprint
    from scripts.cpu_budget import describe_cpu
    budget = describe_cpu()
    layout, _ = run.boundary.layout_for(budget)
    row = run.execute(tmp_path/'app', 'app', 'small', layout=layout, budget=budget,
                      runtime=runtime_fingerprint(), warmup=.1, seconds=.3,
                      rate=300, diagnostic='cprofile')
    tool.validate_contract(row)
    assert row['validated'] and row['client']['requests'] == 90
    worker = row['server']['workers'][0]
    assert worker['delta']['engine_python_application_finished'] == 90
    assert any(call['function'] == 'small_body' for call in worker['profile']['calls'])
