"""Native signal optimization; socket lifecycles run via the isolated runner."""
import asyncio
import contextvars

import aiohttp
import pytest
from oldman_server import OldmanServer
from oldman_server.request import Request
from oldman_server.response import text
import uvloop

from oldman_server.native.host import NativeApplication, serve
from test_http import config
from test_sanic_native import exchange
from test_sanic_request import ObservedRequest
from probes.sanic_spike.run import quiescent


def incoming():
    return ObservedRequest(headers=((b'Host', b'127.0.0.1'),))


@pytest.mark.parametrize('touchup', [True, False])
@pytest.mark.parametrize('signal', [None, 'http.lifecycle.request', 'http.lifecycle.<event>'])
def test_native_touchup_preserves_registered_events_and_skips_only_empty(touchup, signal):
    async def scenario():
        calls, events = [], []
        marker = contextvars.ContextVar('touchup_marker', default='unset')
        class ObservedServer(OldmanServer):
            def dispatch(self, event, **kwargs):
                if event == 'http.lifecycle.request': calls.append(event)
                return super().dispatch(event, **kwargs)
        kind = 'empty' if signal is None else 'dynamic' if '<' in signal else 'exact'
        app = ObservedServer(f'touchup_{touchup}_{kind}', configure_logging=False)
        app.config.TOUCHUP = touchup
        app.state.native_loop = asyncio.get_running_loop()
        if signal:
            async def listener(request=None, event='request', **kwargs):
                if event == 'request':
                    assert Request.get_current() is request
                    assert isinstance(asyncio.current_task(), asyncio.Task)
                    await asyncio.sleep(0)
                    marker.set('signal'); events.append('signal')
            app.signal(signal)(listener)
        @app.get('/')
        async def handler(request):
            assert Request.get_current() is request
            assert marker.get() == ('signal' if signal else 'unset')
            events.append('handler')
            return text('ok')
        await app._startup()
        adapter = NativeApplication(app, 64)
        response = await adapter(incoming())
        assert response[:2] == (200, b'ok')
        assert len(calls) == (0 if touchup and signal is None else 1)
        assert events == (['signal', 'handler'] if signal else ['handler'])
        with pytest.raises(Exception, match='No current request'): Request.get_current()
    uvloop.run(scenario())


def test_native_touchup_binding_survives_other_app_and_repeated_preparation():
    async def scenario():
        calls = []
        class ObservedServer(OldmanServer):
            def dispatch(self, event, **kwargs):
                if event == 'http.lifecycle.request': calls.append(self.name)
                return super().dispatch(event, **kwargs)
        def app(name, touchup):
            instance = ObservedServer(name, configure_logging=False)
            instance.config.TOUCHUP = touchup
            instance.state.native_loop = asyncio.get_running_loop()
            @instance.get('/')
            async def handler(request): return text(name)
            return instance
        first = app('first', True)
        await first._startup()
        a = NativeApplication(first, 64)
        second = app('second', False)
        await second._startup()
        b = NativeApplication(second, 64)
        assert (await a(incoming()))[:2] == (200, b'first')
        assert (await b(incoming()))[:2] == (200, b'second')
        again = NativeApplication(first, 64)
        assert (await again(incoming()))[:2] == (200, b'first')
        assert calls == ['second']
    uvloop.run(scenario())


def test_native_touchup_signal_exception_uses_real_error_chain_and_releases():
    async def scenario():
        app = OldmanServer('touchup_error', configure_logging=False)
        events = []
        @app.signal('http.lifecycle.request')
        async def listener(request):
            assert Request.get_current() is request
            await asyncio.sleep(0)
            events.append('signal')
            raise ValueError('signal failure')
        @app.exception(ValueError)
        async def error(request, exc):
            assert Request.get_current() is request
            events.append('error')
            return text('handled', status=409)
        @app.get('/')
        async def handler(request):
            raise AssertionError('failed signal must bypass the handler')
        cfg = config(application_dispatch_all=True)
        async with serve(app, cfg) as engine:
            async with aiohttp.ClientSession() as client:
                assert (await exchange(client, cfg.server.http_port, '/'))[0::2] == (409, b'handled')
            await quiescent(engine)
        await quiescent(engine)
        assert events == ['signal', 'error']
    uvloop.run(scenario())
