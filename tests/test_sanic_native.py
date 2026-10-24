"""Direct Sanic processing over real native sockets, isolated per test."""
import asyncio
import contextvars
import threading

import aiohttp
from aiohttp import web
import pytest
from oldman_server.request import Request
from oldman_server.response import raw, text
import uvloop

from oldman_server.native.engine import ApplicationRequest
from probes.sanic_spike.run import free_port, quiescent
from test_http import config, wait_stat


def api():
    from oldman_server.native.host import OldmanServer, NativeApplication, serve
    return OldmanServer, NativeApplication, serve


async def exchange(client, port, path, method="GET", **kwargs):
    async with client.request(method, f"http://127.0.0.1:{port}{path}", **kwargs) as reply:
        return reply.status, dict(reply.headers), await reply.read()


def test_native_sanic_retained_request_materializes_after_close():
    OldmanServer, _, serve = api()
    async def scenario():
        app = OldmanServer("native_retained", configure_logging=False)
        app.config.ACCESS_LOG = False
        retained = []
        @app.get("/")
        async def handler(req):
            assert type(req) is Request
            await asyncio.sleep(0)
            retained.append(req)
            return text("retained")
        cfg = config(application_dispatch_all=True)
        async with serve(app, cfg) as engine:
            async with aiohttp.ClientSession() as client:
                for query in ("q=1&q=2&blank=", "q=other"):
                    reply = await exchange(client, cfg.server.http_port, f"/?{query}", headers={"X-Keep": "later"})
                    assert reply[0::2] == (200, b"retained")
            await quiescent(engine)
        await quiescent(engine)
        req, other = retained
        assert req.headers["x-keep"] == "later"
        assert req.ip == "127.0.0.1" and req.port > 0
        assert req.conn_info.server_port == cfg.server.http_port
        assert req.transport.get_extra_info("sockname") == ("127.0.0.1", cfg.server.http_port)
        assert req.args == {"q": ["1", "2"]}
        assert req.query_args == [("q", "1"), ("q", "2")]
        assert req.get_args(keep_blank_values=True) == {"q": ["1", "2"], "blank": [""]}
        assert other.args == {"q": ["other"]} and other.query_args == [("q", "other")]
        assert req.parsed_args is not other.parsed_args
        assert req.parsed_not_grouped_args is not other.parsed_not_grouped_args
    uvloop.run(scenario())


def test_native_sanic_send_snapshot_waits_for_chain_and_preserves_terminal_priority():
    OldmanServer, _, serve = api()
    async def scenario():
        app = OldmanServer("native_send_snapshot", configure_logging=False)
        app.config.ACCESS_LOG = False
        sent, release = asyncio.Queue(), asyncio.Event()
        @app.get("/<kind>")
        async def handler(req, kind):
            response = await req.respond(status=201, headers={"X-Value": "café"})
            await response.send(b"frozen", end_stream=True)
            await sent.put(kind)
            if kind == "timeout":
                await asyncio.Future()
            else:
                await release.wait()
            response.status = 202
            response.body = b"mutated"
            response.headers["X-Value"] = "mutated"
            if kind == "error":
                raise ValueError("after send")
            if kind == "cancel":
                raise asyncio.CancelledError()
            return response
        cfg = config(application_dispatch_all=True, decision_timeout_ms=500)
        async with serve(app, cfg) as engine:
            async with aiohttp.ClientSession() as client:
                jobs = [asyncio.create_task(exchange(client, cfg.server.http_port, "/"+kind))
                        for kind in ("ok", "error", "cancel", "timeout")]
                try:
                    for _ in jobs:
                        await asyncio.wait_for(sent.get(), 2)
                    assert engine.server_stats()["responses"] == 0
                    assert not any(job.done() for job in jobs)
                finally:
                    release.set()
                replies = await asyncio.gather(*jobs)
                assert replies[0][0::2] == (201, b"frozen")
                assert replies[0][1]["X-Value"] == "café"
                # Sanic logs ordinary exceptions after send and keeps the first
                # response; cancellation/deadline still terminate via Engine.
                assert replies[1][0::2] == (201, b"frozen")
                assert [reply[0] for reply in replies[2:]] == [503, 504]
            await quiescent(engine)
        await quiescent(engine)
    uvloop.run(scenario())


def test_native_sanic_final_responses_isolate_invalid_siblings():
    OldmanServer, _, serve = api()
    async def scenario():
        app = OldmanServer("native_batch", configure_logging=False)
        entered, release, held = asyncio.Queue(), asyncio.Event(), asyncio.Event()
        retained = []
        @app.get("/<value:int>")
        async def handler(req, value):
            await entered.put(value)
            await release.wait()
            return text("replaced by middleware")
        @app.on_response
        async def response(req, original):
            value = int(req.path[1:])
            final = raw(f"final-{value}".encode(), status=201,
                        headers={"X-Value": str(value), "X-Latin": "café"})
            if value == 3:
                # Rejected by the native validator, after Sanic finishes (a
                # control character; a Connection field is framed by the host).
                final.headers["X-Bad"] = "a\x01b"
            retained.append(final)
            return final
        cfg = config(application_dispatch_all=True, decision_timeout_ms=3000)
        async with serve(app, cfg) as engine:
            flush = engine._flush_application
            engine._flush_application = held.set
            async with aiohttp.ClientSession() as client:
                pending = [asyncio.create_task(exchange(client, cfg.server.http_port, f"/{i}"))
                           for i in range(8)]
                try:
                    for _ in pending:
                        await asyncio.wait_for(entered.get(), 2)
                    release.set()
                    await asyncio.wait_for(held.wait(), 2)
                    await wait_stat(engine, "python_application_finished", lambda n: n == 8)
                    assert engine.server_stats()["active_requests"] == 8
                    assert engine.server_stats()["reserved_bytes"] > 0
                    for final in retained:
                        assert final.stream is None
                        final.body = b"mutated after completion"
                        final.headers["X-Value"] = "mutated"
                finally:
                    release.set()
                    engine._flush_application = flush
                    flush()
                    replies = await asyncio.gather(*pending, return_exceptions=True)
                for i, reply in enumerate(replies):
                    assert not isinstance(reply, BaseException), reply
                    if i == 3:
                        assert reply[0::2] == (500, b"")
                    else:
                        assert reply[0::2] == (201, f"final-{i}".encode())
                        assert reply[1]["X-Value"] == str(i)
                        assert reply[1]["X-Latin"] == "café"
            await quiescent(engine)
            stats = engine.server_stats()
            assert stats["python_application_errors"] == 1
        await quiescent(engine)
    uvloop.run(scenario())


def test_native_sanic_drops_reply_after_native_deadline_and_recovers():
    OldmanServer, _, serve = api()
    async def scenario():
        app = OldmanServer("native_batch_late", configure_logging=False)
        @app.get("/")
        async def handler(req):
            return text("ok")
        cfg = config(application_dispatch_all=True, decision_timeout_ms=100)
        async with serve(app, cfg) as engine:
            held = asyncio.Event()
            flush = engine._flush_application
            engine._flush_application = held.set
            async with aiohttp.ClientSession() as client:
                try:
                    pending = asyncio.create_task(exchange(client, cfg.server.http_port, "/"))
                    await asyncio.wait_for(held.wait(), 2)
                    # The task already answered, so the deadline notice finds no
                    # handler to cancel; the host answers 504 after its grace period.
                    assert (await asyncio.wait_for(pending, 5))[0] == 504
                finally:
                    engine._flush_application = flush
                    flush()
                await quiescent(engine)
                assert (await exchange(client, cfg.server.http_port, "/"))[0::2] == (200, b"ok")
            await quiescent(engine)
            stats = engine.server_stats()
            assert stats["python_application_errors"] == 0
            assert engine._application_terminal == set() and not engine._application_tasks
        await quiescent(engine)
    uvloop.run(scenario())


def test_native_sanic_routes_middleware_context_metadata_and_real_await():
    OldmanServer, _, serve = api()
    class CustomRequest(Request):
        def __init__(self, url, headers, *args, **kwargs):
            assert headers.getone("host").startswith("127.0.0.1:")
            super().__init__(url, headers, *args, **kwargs)
    class App(OldmanServer):
        async def __call__(self, *args):
            raise AssertionError("ASGI entry must not execute")
    async def scenario():
        app = App("native_semantics", request_class=CustomRequest, configure_logging=False)
        app.config.ACCESS_LOG = False
        trace = contextvars.ContextVar("native_trace")
        thread = threading.get_native_id()
        events, calls, signals = [], [], []
        for name in ("before_server_start", "after_server_start", "before_server_stop", "after_server_stop"):
            async def listener(app, label=name):
                assert app.loop is asyncio.get_running_loop()
                events.append(label)
            app.register_listener(listener, name)
        @app.signal("http.lifecycle.request")
        async def on_request(request):
            signals.append(request.path)
        @app.middleware("request")
        async def before(req):
            trace.set(req.headers.get("x-token", "default"))
            if req.path == "/short":
                return text("short", status=202)
        @app.middleware("response")
        async def after(req, response):
            response.headers["x-token"] = trace.get()
            if req.path == "/replace":
                return text("replacement", status=203)
        @app.get("/items/<value:int>")
        async def item(req, value):
            assert type(req) is CustomRequest and CustomRequest.get_current() is req
            assert isinstance(asyncio.current_task(), asyncio.Task)
            assert threading.get_native_id() == thread
            await asyncio.sleep(.001)
            assert CustomRequest.get_current() is req
            calls.append(value)
            assert req.version == "1.1" and req.ip == "127.0.0.1" and req.port > 0
            assert req.conn_info.server_port == cfg.server.http_port
            assert req.scheme == "http"
            return text(f"{value}:{req.args.get('q')}:{trace.get()}")
        @app.get("/short")
        async def short(req):
            raise AssertionError("short circuit must skip handler")
        @app.get("/replace")
        async def replaced(req):
            return text("original")
        @app.post("/echo")
        async def echo(req):
            return raw(req.body, status=201)
        @app.get("/error")
        async def error(req):
            raise ValueError("expected")
        @app.exception(ValueError)
        async def handled(req, exc):
            return text("handled", status=409)
        upstream = web.Application()
        upstream_calls = []
        async def upstream_reply(req):
            await asyncio.sleep(.001)
            upstream_calls.append(req.query["q"])
            return web.Response(text="upstream:" + req.query["q"])
        upstream.router.add_get("/", upstream_reply)
        runner = web.AppRunner(upstream)
        await runner.setup()
        upstream_port = free_port()
        cfg = config(application_dispatch_all=True, decision_timeout_ms=3000)
        try:
            await web.TCPSite(runner, "127.0.0.1", upstream_port).start()
            @app.get("/proxy")
            async def proxy(req):
                async with aiohttp.ClientSession() as session:
                    async with session.get(f"http://127.0.0.1:{upstream_port}/?q=real") as reply:
                        return text(await reply.text())
            async with serve(app, cfg) as engine:
                assert events == ["before_server_start", "after_server_start"]
                async with aiohttp.ClientSession() as client:
                    values = await asyncio.gather(*(exchange(client, cfg.server.http_port,
                        f"/items/{i}?q=a%2Bb", headers={"x-token": str(i)}) for i in (13, 97)))
                    assert [v[2] for v in values] == [b"13:a+b:13", b"97:a+b:97"]
                    assert sorted(calls) == [13, 97]
                    for path, expected in (("/short", (202, b"short")), ("/replace", (203, b"replacement")),
                                           ("/error", (409, b"handled")), ("/proxy", (200, b"upstream:real"))):
                        response = await exchange(client, cfg.server.http_port, path)
                        assert response[0::2] == expected
                    payload = b"raw\0\xff"
                    assert (await exchange(client, cfg.server.http_port, "/echo", "POST", data=payload))[0::2] == (201, payload)
                    assert (await exchange(client, cfg.server.http_port, "/missing"))[0] == 404
                    assert (await exchange(client, cfg.server.http_port, "/items/13", "POST"))[0] == 405
                assert upstream_calls == ["real"] and "/items/97" in signals
                await quiescent(engine)
            assert events == ["before_server_start", "after_server_start", "before_server_stop", "after_server_stop"]
            await quiescent(engine)
            with pytest.raises(Exception, match="No current request"):
                CustomRequest.get_current()
        finally:
            await runner.cleanup()
    uvloop.run(scenario())


def test_native_sanic_timeout_and_shutdown_cancel_real_tasks():
    OldmanServer, _, serve = api()
    async def scenario():
        app = OldmanServer("native_cancel", configure_logging=False)
        app.config.ACCESS_LOG = False
        entered, cancelled = asyncio.Queue(), []
        @app.get("/wait/<value>")
        async def wait(req, value):
            await entered.put(value)
            try:
                await asyncio.Future()
            finally:
                cancelled.append(value)
        cfg = config(application_dispatch_all=True, decision_timeout_ms=200)
        async with aiohttp.ClientSession() as client:
            async with serve(app, cfg) as engine:
                # Sanic's RESPONSE_TIMEOUT: the handler is cancelled and the error
                # handler answers 503 Response Timeout.
                status, _, body = await exchange(client, cfg.server.http_port, "/wait/timeout")
                assert status == 503 and b"Response Timeout" in body
                await quiescent(engine)
                assert cancelled == ["timeout"]
                assert await entered.get() == "timeout"
                pending = asyncio.create_task(exchange(client, cfg.server.http_port, "/wait/close"))
                assert await asyncio.wait_for(entered.get(), 2) == "close"
            result = await asyncio.gather(pending, return_exceptions=True)
            assert isinstance(result[0], aiohttp.ClientConnectionError) or result[0][0] == 503
        assert cancelled == ["timeout", "close"]
        await quiescent(engine)
    uvloop.run(scenario())


def test_native_sanic_invalid_and_streaming_responses_do_not_escape_limits():
    OldmanServer, _, serve = api()
    async def scenario():
        app = OldmanServer("native_limits", configure_logging=False)
        app.config.ACCESS_LOG = False
        @app.get("/<kind>")
        async def handler(req, kind):
            if kind == "oversize":
                return raw(b"x" * 65)
            if kind == "length":
                return raw(b"ok", headers={"content-length": "99"})
            if kind == "duplicate":
                response = text("cookies")
                response.headers.add("set-cookie", "a=1")
                response.headers.add("set-cookie", "b=2")
                return response
            if kind == "framing":
                return raw(b"ok", headers={"connection": "close"})
            if kind == "stream":
                response = await req.respond()
                try:
                    await response.send(b"part", end_stream=False)
                except ValueError:
                    pass  # Application cannot rescue a failed native submission.
                return response
            if kind == "double":
                response = await req.respond()
                await response.send(b"first", end_stream=True)
                try:
                    await response.send(b"second", end_stream=True)
                except ValueError:
                    pass
                return response
            if kind == "explicit":
                response = await req.respond(content_type="text/plain")
                await response.send(b"explicit", end_stream=True)
                return None
            return text("ok")
        cfg = config(application_dispatch_all=True, max_response_bytes=64)
        async with serve(app, cfg) as engine:
            async with aiohttp.ClientSession() as client:
                assert (await exchange(client, cfg.server.http_port, "/oversize"))[0] == 500
                # As in Sanic, the server's Connection replaces the application's.
                status, headers, body = await exchange(client, cfg.server.http_port, "/framing")
                assert (status, headers["Connection"], body) == (200, "keep-alive", b"ok")
                # Data after a complete response raises ServerError in the handler
                # (Http.send is None); as in Sanic, the first response stands.
                assert (await exchange(client, cfg.server.http_port, "/double"))[0::2] == (200, b"first")
                # Like Sanic, a complete response's Content-Length is its body size.
                assert (await exchange(client, cfg.server.http_port, "/length"))[0::2] == (200, b"ok")
                # Streamed responses are supported: Sanic's own final send ends the stream.
                assert (await exchange(client, cfg.server.http_port, "/stream"))[0::2] == (200, b"part")
                # Repeated names are written as separate field lines (see test_sanic_http1).
                assert (await exchange(client, cfg.server.http_port, "/duplicate"))[0::2] == (200, b"cookies")
                assert (await exchange(client, cfg.server.http_port, "/explicit"))[0::2] == (200, b"explicit")
                assert (await exchange(client, cfg.server.http_port, "/ok"))[0::2] == (200, b"ok")
            await quiescent(engine)
        await quiescent(engine)
    uvloop.run(scenario())


def test_native_sanic_startup_failure_does_not_consume_native_runtime():
    OldmanServer, _, serve = api()
    async def scenario():
        app = OldmanServer("native_start_failure", configure_logging=False)
        @app.get("/")
        async def handler(req):
            return text("ok")
        @app.before_server_start
        async def fail(app):
            assert app.loop is asyncio.get_running_loop()
            raise ValueError("startup failed")
        with pytest.raises(ValueError, match="startup failed"):
            async with serve(app, config(application_dispatch_all=True)):
                pytest.fail("failed startup yielded")
        assert app.state.native_loop is None and not app.state.server_info
        good = OldmanServer("native_after_failure", configure_logging=False)
        good.get("/")(handler)
        cfg = config(application_dispatch_all=True)
        async with serve(good, cfg) as engine:
            async with aiohttp.ClientSession() as client:
                assert (await exchange(client, cfg.server.http_port, "/"))[0::2] == (200, b"ok")
        await quiescent(engine)
    uvloop.run(scenario())


def test_native_sanic_shutdown_listener_failure_still_closes_runtime_and_tasks():
    OldmanServer, _, serve = api()
    async def scenario():
        app = OldmanServer("native_stop_failure", configure_logging=False)
        background_started, background_cancelled = asyncio.Event(), asyncio.Event()
        events = []
        async def background():
            background_started.set()
            try:
                await asyncio.Future()
            finally:
                background_cancelled.set()
        @app.before_server_start
        async def start(app):
            app.add_task(background(), name="background")
        @app.before_server_stop
        async def stop(app):
            events.append("before")
            raise ValueError("shutdown failed")
        @app.after_server_stop
        async def stopped(app):
            assert background_cancelled.is_set()
            events.append("after")
        @app.get("/")
        async def handler(req):
            return text("ok")
        with pytest.raises(ValueError, match="shutdown failed"):
            async with serve(app, config(application_dispatch_all=True)) as engine:
                await asyncio.wait_for(background_started.wait(), 2)
        assert events == ["before", "after"]
        assert list(app.tasks) == [] and app.state.native_loop is None
        await quiescent(engine)
    uvloop.run(scenario())


def test_native_sanic_after_start_failure_closes_listening_runtime():
    OldmanServer, _, serve = api()
    async def scenario():
        app = OldmanServer("native_after_start_failure", configure_logging=False)
        @app.get("/")
        async def handler(req):
            return text("ok")
        @app.after_server_start
        async def fail(app):
            raise ValueError("after start failed")
        cfg = config(application_dispatch_all=True)
        with pytest.raises(ValueError, match="after start failed"):
            async with serve(app, cfg):
                pytest.fail("failed startup yielded")
        assert app.state.native_loop is None and not app.state.is_running
        with pytest.raises(OSError):
            await asyncio.open_connection("127.0.0.1", cfg.server.http_port)
    uvloop.run(scenario())


def test_native_sanic_eager_factory_keeps_request_context_and_completes():
    OldmanServer, _, serve = api()
    async def scenario():
        app = OldmanServer("native_eager", configure_logging=False)
        calls = []
        @app.get("/")
        async def handler(req):
            assert Request.get_current() is req
            calls.append(asyncio.current_task())
            return text("eager")
        cfg = config(application_dispatch_all=True)
        loop = asyncio.get_running_loop()
        def factory(loop, coro, **kwargs):
            return asyncio.Task(coro, loop=loop, eager_start=True, **kwargs)
        async with serve(app, cfg) as engine:
            loop.set_task_factory(factory)
            try:
                async with aiohttp.ClientSession() as client:
                    assert (await exchange(client, cfg.server.http_port, "/"))[0::2] == (200, b"eager")
                await quiescent(engine)
                assert len(calls) == 1 and isinstance(calls[0], asyncio.Task)
                with pytest.raises(Exception, match="No current request"):
                    Request.get_current()
            finally:
                loop.set_task_factory(None)
    uvloop.run(scenario())


def test_native_sanic_rejects_unknown_metadata_without_faking_a_connection():
    OldmanServer, NativeApplication, _ = api()
    async def scenario():
        app = OldmanServer("metadata_missing", configure_logging=False)
        handler = NativeApplication(app, 64)
        with pytest.raises(ValueError, match="metadata"):
            await handler(ApplicationRequest("GET", b"/", (), b""))
    uvloop.run(scenario())


def test_native_response_stops_header_conversion_at_native_limit():
    from oldman_server.native.host import _BufferedExchange
    from oldman_server.response import HTTPResponse
    seen = []
    class ManyHeaders(HTTPResponse):
        @property
        def processed_headers(self):
            for i in range(10000):
                seen.append(i)
                yield f"x-{i}".encode(), b"value"
    async def scenario():
        exchange = _BufferedExchange(b"", 64)
        exchange.respond(ManyHeaders(b"ok"))
        with pytest.raises(ValueError, match="header"):
            await exchange.send(b"", end_stream=True)
        assert len(seen) == 129
        with pytest.raises(ValueError):
            exchange.finish()
    uvloop.run(scenario())


def test_native_sanic_host_cancel_during_close_waits_for_handler_cleanup():
    OldmanServer, _, serve = api()
    async def scenario():
        app = OldmanServer("native_close_cancel", configure_logging=False)
        entered, cleaning, release, stop, ready, after = [asyncio.Event() for _ in range(6)]
        engines = []
        @app.get("/")
        async def handler(req):
            entered.set()
            try:
                await asyncio.Future()
            finally:
                cleaning.set()
                await release.wait()
        @app.after_server_stop
        async def stopped(app):
            after.set()
        cfg = config(application_dispatch_all=True, decision_timeout_ms=3000)
        async def host():
            async with serve(app, cfg) as engine:
                engines.append(engine)
                ready.set()
                await stop.wait()
        host_task = asyncio.create_task(host())
        async with aiohttp.ClientSession() as client:
            await asyncio.wait_for(ready.wait(), 2)
            pending = asyncio.create_task(exchange(client, cfg.server.http_port, "/"))
            try:
                await asyncio.wait_for(entered.wait(), 2)
                stop.set()
                await asyncio.wait_for(cleaning.wait(), 2)
                host_task.cancel()
                await asyncio.sleep(.01)
                assert not after.is_set() and not host_task.done()
                assert app.state.native_loop is asyncio.get_running_loop()
                host_task.cancel()  # Repeated cancellation must not stop cleanup.
            finally:
                release.set()
                await asyncio.gather(host_task, return_exceptions=True)
                await engines[0].aclose()
                await asyncio.gather(pending, return_exceptions=True)
        assert after.is_set() and host_task.cancelled()
        assert app.state.native_loop is None
        await quiescent(engines[0])
    uvloop.run(scenario())
