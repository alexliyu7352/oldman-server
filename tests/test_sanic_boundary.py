"""Pure protocol checks; real native lifecycles run in the probe's subprocesses."""
import asyncio
from types import SimpleNamespace

import pytest

from probes.sanic_boundary.application import buffered_asgi


def request():
    return SimpleNamespace(target=b'/echo?a=1', method='POST', http_version='1.0',
        client=('127.0.0.1', 31234), server=('127.0.0.1', 8000),
        headers=((b'Host', b'127.0.0.1'),), body=b'abc')


def test_asgi_preserves_metadata_and_complete_body():
    async def app(scope, receive, send):
        assert scope['http_version'] == '1.0'
        assert scope['client'] == ('127.0.0.1', 31234)
        assert scope['server'] == ('127.0.0.1', 8000)
        assert scope['raw_path'] == b'/echo' and scope['query_string'] == b'a=1'
        assert scope['headers'] == [(b'host', b'127.0.0.1')]
        assert await receive() == {'type': 'http.request', 'body': b'abc', 'more_body': False}
        await send({'type': 'http.response.start', 'status': 200,
                    'headers': [(b'content-length', b'3'), (b'x-test', b'yes')]})
        await send({'type': 'http.response.body', 'body': b'abc'})
    assert asyncio.run(buffered_asgi(app, 16)(request())) == (200, b'abc', (('x-test', 'yes'),))


@pytest.mark.parametrize('headers,body,more', [
    ([(b'x', b'1'), (b'X', b'2')], b'abc', False),
    ([(b'content-length', b'4')], b'abc', False),
    ([(b'x', b'x'*8192)], b'abc', False),
    ([(str(i).encode(), b'v') for i in range(129)], b'abc', False),
    ([], b'x'*17, False), ([], b'abc', True),
])
def test_asgi_latches_invalid_complete_response(headers, body, more):
    async def app(scope, receive, send):
        try:
            await send({'type': 'http.response.start', 'status': 200, 'headers': headers})
            await send({'type': 'http.response.body', 'body': body, 'more_body': more})
        except ValueError:
            pass
    with pytest.raises(ValueError):
        asyncio.run(buffered_asgi(app, 16)(request()))


def valid_row():
    from probes.sanic_boundary.application import EXPECTED
    from probes.sanic_boundary.run import ZERO
    stats = dict.fromkeys(ZERO, 0)
    return {'mode': 'native', 'workload': 'small', 'diagnostic': 'none',
        'concurrency': 2, 'rate': 0, 'seconds': 2, 'returncodes': [0, 0],
        'client': {'requests': 10, 'scheduled': 10, 'errors': 0, 'status_errors': 0,
            'seconds': 2.001, 'arrival_seconds': 2, 'rate': 0, 'rps': 10/2.001,
            'p99_us': 1000, 'cpu_seconds': .1,
            'integrity_errors': 0, 'dropped': 0, 'new_connections': 0, 'total_connections': 2,
            'response_bytes': 10*len(EXPECTED['small']), 'server_pids': {'123': 10}},
        'ready': {'pid': 123},
        'server': {'after': stats, 'delta': {'responses': 10, 'python_application_started': 10,
            'python_application_finished': 10, 'python_application_errors': 0},
            'process_cpu_ns': 2000, 'python_thread_cpu_ns': 1000, 'diagnostics': {}},
        'closed': {'closed': True, 'stats': stats}}


def test_row_counts_and_diagnostic_separation():
    from copy import deepcopy
    from probes.sanic_boundary.run import validate_row
    row = valid_row()
    validate_row(row)
    for section, key, value in [('client', 'errors', 1), ('client', 'requests', 9),
        ('client', 'response_bytes', 0), ('client', 'new_connections', 1),
        ('server', 'python_thread_cpu_ns', -1), ('server', 'diagnostics', {'calls': []})]:
        bad = deepcopy(row)
        bad[section][key] = value
        with pytest.raises(ValueError):
            validate_row(bad)
    row['closed']['stats']['pending_application_nodes'] = 1
    with pytest.raises(ValueError):
        validate_row(row)


def test_fixture_leaves_diagnostic_headers_to_native_writer():
    from probes.sanic_boundary.application import make_app, EXPECTED
    app = make_app()
    _, response = app.test_client.get('/small')
    assert response.status == 200 and response.body == EXPECTED['small']
    assert not any(name.lower().startswith('x-probe-') for name in response.headers)


def test_failed_child_start_preserves_evidence_and_cleans_owned_process(tmp_path, monkeypatch):
    import json
    from probes.sanic_boundary import run
    child = SimpleNamespace()
    stopped = []
    monkeypatch.setattr(run.subprocess, 'Popen', lambda *a, **kw: child)
    def failed_read(process):
        raise RuntimeError('injected child startup failure')
    monkeypatch.setattr(run, 'read_json', failed_read)
    monkeypatch.setattr(run, 'stop_process', stopped.append)
    layout = {'server': [0, 1, 2], 'client': [3, 4, 5], 'controller': [7]}
    out = tmp_path / 'failed'
    with pytest.raises(RuntimeError, match='injected'):
        run.execute(out, 'native', 'small', 'none', budget={}, layout=layout,
                    seconds=.2, warmup=.1, concurrency=128, rate=0)
    assert stopped == [child]
    result = json.loads((out / 'row.json').read_text())
    assert not result['validated'] and 'injected' in result['failure']
    with pytest.raises(FileExistsError):
        run.execute(out, 'native', 'small', 'none', budget={}, layout=layout,
                    seconds=.2, warmup=.1, concurrency=128, rate=0)


def test_window_duration_and_offered_load_are_checked():
    from copy import deepcopy
    from probes.sanic_boundary.run import validate_row
    for field, value in [('seconds', 0.01), ('arrival_seconds', 1), ('rate', 500),
                         ('rps', float('nan')), ('p99_us', float('inf'))]:
        bad = valid_row()
        bad['client'][field] = value
        with pytest.raises(ValueError):
            validate_row(bad)
    bad = valid_row()
    bad['rate'] = bad['client']['rate'] = 1000
    with pytest.raises(ValueError):
        validate_row(bad)


def test_actual_module_paths_and_hashes_match_local_sources():
    from copy import deepcopy
    from probes.sanic_boundary.server import module_evidence
    from probes.sanic_boundary.run import verify_modules
    evidence = module_evidence()
    verify_modules(evidence)
    for name in evidence['modules']:
        for field, wrong in [('path', '/tmp/foreign-copy.py'), ('sha256', '0'*64)]:
            bad = deepcopy(evidence)
            bad['modules'][name][field] = wrong
            with pytest.raises(ValueError):
                verify_modules(bad)
    evidence['sanic_version'] = '25.12.1'
    with pytest.raises(ValueError):
        verify_modules(evidence)


def test_cpu_layout_keeps_server_and_client_disjoint():
    from probes.sanic_boundary.run import cpu_layout
    topology = [{"cpu": cpu, "package_id": "0", "core_id": str(cpu)} for cpu in range(11, 20)]
    layout = cpu_layout(tuple(range(11, 20)), topology)
    assert layout["server"] == [11, 12, 13]
    assert layout["client"] == [14, 15, 16, 17]
    assert layout["unused"] == 19
    assert layout["controller"] == 18
    with pytest.raises(ValueError):
        cpu_layout(tuple(range(8)), topology)


def test_cpu_layout_keeps_controller_off_server_smt_sibling():
    from probes.sanic_boundary.run import cpu_layout
    topology = [{"cpu": cpu, "package_id": "0", "core_id": str(cpu % 8)} for cpu in range(16)]
    layout = cpu_layout(range(16), topology)
    assert layout["server"] == [0, 1, 2]
    assert layout["client"] == [3, 4, 5, 6]
    assert layout["controller"] == 7
    assert layout["unused"] == 8
    # Interleaved logical numbering must not place sibling threads in different roles.
    topology = [{"cpu": cpu, "package_id": "0", "core_id": str(cpu // 2)} for cpu in range(16)]
    layout = cpu_layout(range(16), topology)
    assert layout["server"] == [0, 2, 4]
    assert layout["client"] == [6, 8, 10, 12]
    assert layout["controller"] == 14


def test_cpu_layout_rejects_insufficient_or_unknown_physical_cores():
    from probes.sanic_boundary.run import cpu_layout
    topology = [{"cpu": cpu, "package_id": "0", "core_id": str(cpu % 4)} for cpu in range(16)]
    with pytest.raises(ValueError, match="physical"):
        cpu_layout(range(16), topology)
    with pytest.raises(ValueError, match="topology"):
        cpu_layout(range(16), topology[:-1])


def test_response_parser_reads_chunked_body():
    import io
    from probes.sanic_boundary.application import EXPECTED
    from probes.sanic_boundary.run import read_response
    body = EXPECTED["small"]
    response = (b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                b"Transfer-Encoding: chunked\r\nX-Probe-Pid: 901\r\n\r\n" +
                b"%x\r\n" % len(body) + body + b"\r\n0\r\n\r\n")
    status, headers, read = read_response(io.BytesIO(response))
    assert (status, read, headers["x-probe-pid"]) == (200, body, "901")
    with pytest.raises(ValueError, match="truncated"):
        read_response(io.BytesIO(response[:-12]))
