"""Disposable Sanic ASGI feasibility probe, NOT a general ASGI server.

Only admitted fixture paths and HTTP/1.1 GET/POST with buffered bodies. Run each
case in a fresh process; there is no supported product interface in this file.
"""
import asyncio
from contextlib import asynccontextmanager, suppress
import contextvars
import importlib.metadata
import json
import os
import socket
import sys
import threading
from urllib.parse import unquote

import aiohttp
from aiohttp import web
from oldman_server import OldmanServer
from oldman_server.response import json as sanic_json, raw, text
import uvloop

from oldman_server.native.engine import ApplicationRequest, Config, Engine, ServerConfig


@asynccontextmanager
async def lifespan(app):
    incoming, outgoing = asyncio.Queue(), asyncio.Queue()
    task = asyncio.create_task(app({'type': 'lifespan', 'asgi': {'version': '3.0', 'spec_version': '2.0'}},
                                  incoming.get, outgoing.put))
    started = False
    try:
        await incoming.put({'type': 'lifespan.startup'})
        message = await asyncio.wait_for(outgoing.get(), 5)
        assert message['type'] == 'lifespan.startup.complete', message
        started = True
        yield
    finally:
        try:
            if started:
                await incoming.put({'type': 'lifespan.shutdown'})
                message = await asyncio.wait_for(outgoing.get(), 5)
                assert message['type'] == 'lifespan.shutdown.complete', message
                await asyncio.wait_for(task, 5)
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task


def buffered_handler(app, port, max_body):
    async def handle(request):
        path, _, query = request.target.partition(b'?')
        scope = {'type': 'http', 'asgi': {'version': '3.0', 'spec_version': '2.0'},
                 'http_version': '1.1', 'method': request.method, 'scheme': 'http',
                 'path': unquote(path.decode('ascii')), 'raw_path': path,
                 'query_string': query, 'root_path': '', 'server': ('127.0.0.1', port),
                 'headers': [(k.lower(), v) for k, v in request.headers]}
        # ASGI 2.0 predates the send-after-close exception requirement. This probe
        # independently lacks peer-disconnect events. HTTP/1.1 is a fixture fact;
        # native request metadata does not expose its version.
        received = False
        response = None
        body = None
        failure = None

        async def receive():
            nonlocal received
            if not received:
                received = True
                return {'type': 'http.request', 'body': request.body, 'more_body': False}
            # No fabricated disconnect after the buffered body. Engine timeout or
            # shutdown cancels this ordinary Task; peer disconnect is unimplemented.
            await asyncio.Future()

        async def send(message):
            nonlocal response, body, failure
            try:
                if failure is not None:
                    raise ValueError('response already failed')
                if message['type'] == 'http.response.start':
                    if response is not None or message.get('trailers'):
                        raise ValueError('duplicate start or unsupported trailers')
                    response = message
                elif message['type'] == 'http.response.body':
                    if response is None or body is not None or message.get('more_body', False):
                        raise ValueError('streaming or invalid response order')
                    body = message.get('body', b'')
                    if type(body) is not bytes or len(body) > max_body:
                        raise ValueError('response body exceeds the bounded probe contract')
                else:
                    raise ValueError('unsupported response event')
            except Exception as exc:
                failure = exc
                raise

        await app(scope, receive, send)
        if failure is not None:
            raise failure
        if response is None or body is None:
            raise ValueError('incomplete response')
        headers = []
        lengths = []
        for name, value in response.get('headers', []):
            if name.lower() == b'content-length':
                lengths.append(value)
            else:
                headers.append((name.decode('ascii'), value.decode('latin-1')))
        if lengths and (len(lengths) != 1 or not lengths[0].isdigit() or int(lengths[0]) != len(body)):
            raise ValueError('inconsistent content-length')
        # Native writer computes framing; only a verified equivalent length is removed.
        return response['status'], body, tuple(headers)
    return handle


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


async def quiescent(engine):
    async with asyncio.timeout(5):
        while True:
            stats = engine.server_stats()
            if all(stats[key] == 0 for key in ('active_requests', 'reserved_bytes',
                                             'pending_application_nodes', 'python_application_active',
                                             'pending_decisions', 'queued_decisions')):
                return stats
            await asyncio.sleep(.001)


async def protocol_checks():
    request = ApplicationRequest('GET', b'/fixture', ((b'X-Token', b'fixture'),), b'')
    start = {'type': 'http.response.start', 'status': 200, 'headers': [(b'content-length', b'3')]}
    end = {'type': 'http.response.body', 'body': b'abc'}
    cases = {
        'length mismatch': [start | {'headers': [(b'content-length', b'4')]}, end],
        'duplicate length': [start | {'headers': [(b'content-length', b'3')] * 2}, end],
        'duplicate start': [start, start, end],
        'stream': [start, end | {'more_body': True}],
        'oversize': [start, end | {'body': b'x' * 17}],
        'missing end': [start],
        'body before start': [end],
        'trailers': [start | {'trailers': True}, end],
    }
    for name, messages in {'valid': [start, end], **cases}.items():
        async def fake_app(scope, receive, send):
            event = await receive()
            assert event == {'type': 'http.request', 'body': b'', 'more_body': False}
            for message in messages:
                await send(message)
        try:
            result = await buffered_handler(fake_app, 8000, 16)(request)
        except ValueError:
            assert name != 'valid', name
        else:
            assert name == 'valid' and result == (200, b'abc', ()), (name, result)
    return {'case': 'protocol', 'passed': True, 'accepted': ['verified equivalent content-length'],
            'rejected': list(cases)}


async def run(case):
    assert isinstance(asyncio.get_running_loop(), uvloop.Loop)
    assert sys.version_info[:3] == (3, 14, 7) and sys._is_gil_enabled()
    main_tid = threading.get_native_id()
    app = OldmanServer('seastar_spike_' + case, configure_logging=False)
    app.config.ACCESS_LOG = False
    events, calls, cancelled = [], [], []
    trace = contextvars.ContextVar('trace')
    for event in ('before_server_start', 'after_server_start', 'before_server_stop', 'after_server_stop'):
        async def listener(app, label=event):
            events.append(label)
        app.register_listener(listener, event)

    @app.middleware('request')
    async def before(request):
        request.ctx.token = request.headers.get('x-token', 'default')
        trace.set(request.ctx.token)

    @app.middleware('response')
    async def after(request, response):
        assert trace.get() == request.ctx.token
        response.headers['x-token'] = request.ctx.token

    @app.get('/items/<number:int>')
    async def item(request, number):
        assert isinstance(asyncio.current_task(), asyncio.Task)
        assert threading.get_native_id() == main_tid
        await asyncio.sleep(.001)
        assert trace.get() == request.ctx.token
        calls.append(number)
        return sanic_json({'number': number, 'query': request.args.get('q'), 'call': len(calls)})

    @app.post('/echo')
    async def echo(request):
        await asyncio.sleep(0)
        return raw(request.body, status=201, headers={'x-size': str(len(request.body))})

    @app.get('/error')
    async def error(request):
        raise ValueError('fixture')

    @app.exception(ValueError)
    async def handle_error(request, exception):
        return text('handled', status=409)

    entered = asyncio.Event()
    @app.get('/wait')
    async def wait(request):
        entered.set()
        try:
            await asyncio.Future()
        finally:
            cancelled.append(True)

    upstream = web.Application()
    upstream_calls = []
    async def answer(request):
        upstream_calls.append(request.query['value'])
        await asyncio.sleep(.005)
        return web.json_response({'upstream': request.query['value']})
    upstream.router.add_get('/reply', answer)
    runner = web.AppRunner(upstream)
    try:
        await runner.setup()
        upstream_port = free_port()
        await web.TCPSite(runner, '127.0.0.1', upstream_port).start()
        @app.get('/proxy')
        async def proxy(request):
            async with aiohttp.ClientSession() as session:
                async with session.get(f'http://127.0.0.1:{upstream_port}/reply', params={'value': request.args['value']}) as reply:
                    return sanic_json(await reply.json())

        # Concrete fixture allowlist, NOT translation of Sanic's dynamic router.
        routes = (('GET', '/items/7'), ('GET', '/items/19'), ('POST', '/items/7'),
                  ('POST', '/echo'), ('GET', '/error'), ('GET', '/missing'),
                  ('GET', '/wait'), ('GET', '/proxy'))
        cpus = tuple(sorted(os.sched_getaffinity(0)))
        port = free_port()
        cfg = Config(shard_cpus=cpus[-4:-2], worker_cpus=cpus[-2:], batch_decisions=True,
                     retire_http_contexts=True,
                     server=ServerConfig(http_port=port, application_routes=routes,
                                         decision_timeout_ms=150 if case == 'timeout' else 3000))
        engine = None
        results = {}
        async with lifespan(app):
            try:
                assert events == ['before_server_start', 'after_server_start']
                engine = await Engine.open(cfg, application_handler=buffered_handler(app, port, cfg.server.max_response_bytes))
                async with aiohttp.ClientSession() as client:
                    async def exchange(method, path, **kwargs):
                        async with client.request(method, f'http://127.0.0.1:{port}{path}', **kwargs) as response:
                            return response.status, {k.lower(): v for k, v in response.headers.items()}, await response.read()
                    if case == 'semantics':
                        replies = await asyncio.gather(*(exchange('GET', f'/items/{n}?q=a%2Bb', headers={'x-token': str(n)}) for n in (7, 19)))
                        for n, (status, headers, body) in zip((7, 19), replies):
                            value = json.loads(body)
                            assert status == 200 and value['number'] == n and value['query'] == 'a+b'
                            assert headers['x-token'] == str(n)
                        assert sorted(calls) == [7, 19]
                        repeat = await exchange('GET', '/items/7')
                        assert json.loads(repeat[2])['call'] == 3
                        payload = b'raw\x00\xff'
                        response = await exchange('POST', '/echo', data=payload)
                        assert response[0] == 201 and response[2] == payload and response[1]['x-size'] == '5'
                        response = await exchange('GET', '/error')
                        assert response[0] == 409 and response[2] == b'handled'
                        assert (await exchange('GET', '/missing'))[0] == 404
                        assert (await exchange('POST', '/items/7'))[0] == 405
                        assert (await exchange('GET', '/items/99'))[0] == 404
                        assert len(calls) == 3 and sorted(calls[:2]) == [7, 19] and calls[-1] == 7, calls
                        response = await exchange('GET', '/proxy?value=real-await')
                        assert response[0] == 200 and json.loads(response[2]) == {'upstream': 'real-await'}
                        assert upstream_calls == ['real-await']
                        results['checks'] = ['dynamic parameters on admitted paths', 'query', 'fresh handlers',
                            'middleware/context isolation', 'binary POST', 'exception handler', 'Sanic 404/405',
                            'unadmitted path blocked by native', 'real aiohttp await']
                    elif case == 'timeout':
                        response = await exchange('GET', '/wait')
                        await quiescent(engine)
                        assert response[0] == 504 and cancelled == [True], (response, cancelled)
                        results['checks'] = ['deadline cancels suspended Sanic handler']
                    elif case == 'close':
                        pending = asyncio.create_task(exchange('GET', '/wait'))
                        try:
                            await asyncio.wait_for(entered.wait(), 3)
                            await asyncio.wait_for(engine.aclose(), 5)
                            try:
                                response = await pending
                                assert response[0] == 503, response
                            except aiohttp.ClientConnectionError:
                                pass
                            assert cancelled == [True]
                        finally:
                            pending.cancel()
                            with suppress(asyncio.CancelledError):
                                await pending
                        results['checks'] = ['shutdown cancels suspended Sanic handler']
                    else:
                        raise ValueError(case)
                results['quiescent'] = await quiescent(engine)
                await engine.aclose()
            finally:
                if engine is not None:
                    await engine.aclose()
        assert events == ['before_server_start', 'after_server_start', 'before_server_stop', 'after_server_stop']
        results['closed'] = await quiescent(engine)
        results.update(case=case, passed=True, lifespan=events, pid=os.getpid(), main_tid=main_tid,
                       shards=engine.shards(), oldman_server=importlib.metadata.version('oldman-server'), python=sys.version,
                       gil=sys._is_gil_enabled(), jit=sys._jit.is_enabled(), uvloop=uvloop.__version__)
        return results
    finally:
        await runner.cleanup()


if __name__ == '__main__':
    if not __debug__:
        raise RuntimeError('probe assertions require normal Python mode')
    result = uvloop.run(protocol_checks() if sys.argv[1] == 'protocol' else run(sys.argv[1]))
    print(json.dumps(result, sort_keys=True), flush=True)
