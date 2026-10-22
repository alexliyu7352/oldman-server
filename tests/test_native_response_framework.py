"""Native response integration; run each test in a fresh Seastar process."""
import asyncio
import contextvars
from functools import partial
import json
import socket

import pytest

from oldman_server.native import engine as api
from test_http import config, request, wait_stat


@pytest.mark.asyncio
async def test_native_response_preserves_task_and_context_across_aiohttp_await():
    import aiohttp
    from aiohttp import web
    from seastar_http import App

    marker = contextvars.ContextVar("native_response_marker", default="unset")
    token = marker.set("registered")
    upstream = web.Application()
    seen, tasks = [], []

    async def answer(req):
        await asyncio.sleep(.005)
        return web.json_response({"value": req.query["value"]})

    upstream.router.add_get("/reply", answer)
    runner = web.AppRunner(upstream)
    await runner.setup()
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        upstream_port = sock.getsockname()[1]
    await web.TCPSite(runner, "127.0.0.1", upstream_port).start()
    app = App(native_responses=True)

    @app.get("/proxy")
    async def proxy(req):
        task = asyncio.current_task()
        assert isinstance(task, asyncio.Task)
        assert task in [entry[0] for entry in engine._application_tasks.values()]
        assert marker.get() == "registered"
        marker.set(req.query_string)
        tasks.append(task)
        async with aiohttp.ClientSession() as session:
            async with session.get(
                    f"http://127.0.0.1:{upstream_port}/reply?value={req.query_string}") as response:
                result = await response.json()
        assert asyncio.current_task() is task
        assert marker.get() == req.query_string
        seen.append(req.query_string)
        return result

    cfg = config()
    engine = await app.start(cfg)
    try:
        replies = await asyncio.gather(*(request(cfg.server.http_port, f"/proxy?{value}")
                                         for value in ("alpha", "beta")))
        assert [json.loads(body) for _, _, body in replies] == [{"value": "alpha"}, {"value": "beta"}]
        assert all(status == 200 for status, _, _ in replies)
        assert sorted(seen) == ["alpha", "beta"]
        assert len(set(tasks)) == 2
        assert marker.get() == "registered"
        await wait_stat(engine, "python_application_active", lambda n: n == 0)
    finally:
        await engine.aclose()
        await runner.cleanup()
        marker.reset(token)
    assert engine.server_stats()["reserved_bytes"] == 0


@pytest.mark.asyncio
async def test_native_response_fresh_dict_is_encoded_on_every_request(monkeypatch):
    from seastar_http import App
    app = App(native_responses=True)
    values = []
    serializer_calls = []
    serializer = json.dumps

    def counted(value, **kwargs):
        serializer_calls.append(value)
        return serializer(value, **kwargs)

    monkeypatch.setattr(json, "dumps", counted)

    @app.get("/json")
    async def fresh(req):
        result = {"message": "Hello, World!", "invocation": len(values) + 1}
        values.append(result)
        return result

    cfg = config()
    engine = await app.start(cfg)
    try:
        replies = [await request(cfg.server.http_port, "/json") for _ in range(4)]
        assert [json.loads(body)["invocation"] for _, _, body in replies] == [1, 2, 3, 4]
        assert len({id(value) for value in values}) == 4
        assert all(status == 200 and headers["content-type"] == "application/json"
                   for status, headers, _ in replies)
        assert serializer_calls == []
        stats = engine.server_stats()
        assert stats["native_response_json_encoded"] == 4
        assert stats["native_response_json_fallback"] == 0
        assert stats["native_response_queued"] == stats["native_response_flushed"] == 4
    finally:
        await engine.aclose()


@pytest.mark.asyncio
async def test_native_response_types_and_invalid_siblings_are_isolated():
    from seastar_http import App, Response

    class TimeoutDict(dict):
        def items(self):
            raise TimeoutError("intentional serializer timeout")

    class CancelledDict(dict):
        def items(self):
            raise asyncio.CancelledError("intentional serializer cancellation")

    serializer_effects = []

    class SideEffectDict(dict):
        def items(self):
            serializer_effects.append("items")
            return super().items()

    class EncodedText(str):
        def encode(self, encoding="utf-8", errors="strict"):
            assert (encoding, errors) == ("utf-8", "strict")
            return b"overridden encoding"

    class RawBytes(bytes):
        def __bytes__(self):
            return b"must not invoke conversion"

    app = App(native_responses=True)
    cycle = {}
    cycle["self"] = cycle
    fixtures = {
        "/json": {"text": "café ☃", "nested": [True, None, 1.5]},
        "/text": "café ☃",
        "/bytes": b"raw\x00\xff",
        "/response": Response("café ☃", status=201, headers=(("X-Test", "café"),)),
        "/object": object(),
        "/tuple": (200, b"user tuple", ()),
        "/unsupported": {"bad": {1, 2}},
        "/cycle": cycle,
        "/header": Response(b"bad", headers=(("Content-Length", "3"),)),
        "/large": "x" * 129,
        "/serializer-timeout": TimeoutDict(value=1),
        "/serializer-cancelled": CancelledDict(value=1),
        "/oversize-before-timeout": {"large": "x" * 256, "later": TimeoutDict(x=1)},
        "/oversize-before-cancelled": {"large": "x" * 256, "later": CancelledDict(x=1)},
        "/oversize-before-effect": {"large": "x" * 256, "later": SideEffectDict(x=1)},
        "/text-subclass": EncodedText("original text"),
        "/bytes-subclass": RawBytes(b"original bytes"),
        "/ok": "ok",
    }
    for path, value in fixtures.items():
        app.get(path)(lambda req, value=value: value)
    cfg = config(max_response_bytes=128)
    engine = await app.start(cfg)
    try:
        replies = dict(zip(fixtures, await asyncio.gather(*(request(cfg.server.http_port, path)
                                                          for path in fixtures))))
        assert replies["/json"][0::2] == (
            200, b'{"text":"caf\\u00e9 \\u2603","nested":[true,null,1.5]}')
        assert replies["/json"][1]["content-type"] == "application/json"
        assert replies["/text"][0::2] == (200, "café ☃".encode())
        assert replies["/text"][1]["content-type"] == "text/plain; charset=utf-8"
        assert replies["/bytes"][0::2] == (200, b"raw\x00\xff")
        assert replies["/response"][0::2] == (201, "café ☃".encode())
        assert replies["/response"][1]["x-test"] == "café"
        assert replies["/response"][1]["content-type"] == "text/plain; charset=utf-8"
        for path in ("/object", "/tuple", "/unsupported", "/cycle", "/header", "/large"):
            assert replies[path][0::2] == (500, b""), path
        assert replies["/serializer-timeout"][0::2] == (504, b"")
        assert replies["/serializer-cancelled"][0::2] == (503, b"")
        # Stdlib serializes the complete value before the response boundary
        # enforces its size limit. Later hooks must retain their effects/errors.
        assert replies["/oversize-before-timeout"][0::2] == (504, b"")
        assert replies["/oversize-before-cancelled"][0::2] == (503, b"")
        assert replies["/oversize-before-effect"][0::2] == (500, b"")
        assert serializer_effects == ["items"]
        assert replies["/text-subclass"][0::2] == (200, b"overridden encoding")
        assert replies["/text-subclass"][1]["content-type"] == "text/plain; charset=utf-8"
        assert replies["/bytes-subclass"][0::2] == (500, b"")
        assert replies["/ok"][0::2] == (200, b"ok")
        assert (await request(cfg.server.http_port, "/missing"))[0] == 404
        await wait_stat(engine, "python_application_active", lambda n: n == 0)
        assert engine.server_stats()["python_application_errors"] == 8
        assert engine.server_stats()["native_response_queued"] == 18
        assert engine.server_stats()["native_response_flushed"] == 18
    finally:
        await engine.aclose()
    assert engine.server_stats()["active_requests"] == engine.server_stats()["reserved_bytes"] == 0


@pytest.mark.asyncio
async def test_native_response_serializer_baseexception_flushes_500_and_cleans_task():
    from seastar_http import App

    class EncoderAbort(BaseException):
        pass

    class AbortDict(dict):
        def items(self):
            raise EncoderAbort("intentional serialization abort")

    app = App(native_responses=True)
    app.get("/abort")(lambda req: AbortDict(value=1))
    app.get("/ok")(lambda req: {"ok": True})
    cfg = config()
    engine = await app.start(cfg)
    try:
        assert (await request(cfg.server.http_port, "/abort"))[0::2] == (500, b"")
        assert (await request(cfg.server.http_port, "/ok"))[0::2] == (200, b'{"ok":true}')
        await wait_stat(engine, "python_application_active", lambda n: n == 0)
        stats = engine.server_stats()
        assert stats["python_application_started"] == stats["python_application_finished"] == 2
        assert stats["python_application_errors"] == 1
        assert stats["native_response_json_fallback"] == 1
        assert stats["native_response_queued"] == stats["native_response_flushed"] == 2
        assert engine._application_terminal == set() and not engine._application_tasks
    finally:
        await engine.aclose()


@pytest.mark.asyncio
async def test_native_response_eager_failed_and_prestart_cancelled_task_factories():
    from seastar_http import App
    app = App(native_responses=True)
    app.get("/instant")(lambda req: {"instant": True})
    cfg = config()
    engine = await app.start(cfg)
    loop = asyncio.get_running_loop()
    original = loop.get_task_factory()
    observed = []

    def is_application(coroutine):
        return getattr(coroutine, "cr_code", None) and coroutine.cr_code.co_name == "_serve_application"

    try:
        def eager_factory(loop, coroutine, **kwargs):
            task = asyncio.eager_task_factory(loop, coroutine, **kwargs)
            if is_application(coroutine):
                task.add_done_callback(lambda completed: observed.append((
                    engine.server_stats()["native_response_queued"],
                    engine.server_stats()["python_application_finished"])))
            return task
        loop.set_task_factory(eager_factory)
        assert (await request(cfg.server.http_port, "/instant"))[0::2] == (200, b'{"instant":true}')
        await wait_stat(engine, "python_application_active", lambda n: n == 0)
        # This callback is registered before Engine's done callback: normal
        # completion must already be queued by the coroutine wrapper's finally.
        assert observed == [(1, 1)]

        def failing_factory(loop, coroutine, **kwargs):
            if is_application(coroutine):
                raise RuntimeError("intentional task creation failure")
            return asyncio.Task(coroutine, loop=loop, **kwargs)
        loop.set_task_factory(failing_factory)
        assert (await request(cfg.server.http_port, "/instant"))[0::2] == (500, b"")

        def cancelled_factory(loop, coroutine, **kwargs):
            task = asyncio.Task(coroutine, loop=loop, **kwargs)
            if is_application(coroutine):
                task.cancel()
            return task
        loop.set_task_factory(cancelled_factory)
        assert (await request(cfg.server.http_port, "/instant"))[0::2] == (503, b"")
        await wait_stat(engine, "python_application_active", lambda n: n == 0)
        stats = engine.server_stats()
        assert stats["python_application_started"] == stats["python_application_finished"] == 2
        assert stats["python_application_errors"] == 2
        assert stats["native_response_queued"] == stats["native_response_flushed"] == 3
        assert engine._application_terminal == set() and not engine._application_tasks
    finally:
        loop.set_task_factory(original)
        await engine.aclose()


@pytest.mark.asyncio
async def test_native_response_timeout_preserves_cancellation_and_late_reply_cleanup(monkeypatch):
    import seastar_http
    from test_framework import test_framework_native_timeout_cancels_handler_without_late_reply
    monkeypatch.setattr(seastar_http, "App", partial(seastar_http.App, native_responses=True))
    await test_framework_native_timeout_cancels_handler_without_late_reply()


@pytest.mark.asyncio
async def test_native_response_close_cancels_pending_and_drains(monkeypatch):
    import seastar_http
    from test_framework import test_framework_close_cancels_pending_and_drains
    monkeypatch.setattr(seastar_http, "App", partial(seastar_http.App, native_responses=True))
    await test_framework_close_cancels_pending_and_drains()


@pytest.mark.asyncio
async def test_native_response_option_preserves_legacy_engine_tuple_handler():
    cfg = config(application_routes=(("GET", "/legacy"),))

    async def handler(req):
        return 202, b"legacy", (("X-Legacy", "yes"),)

    engine = await api.Engine.open(cfg, application_handler=handler)
    try:
        status, headers, body = await request(cfg.server.http_port, "/legacy")
        assert (status, headers["x-legacy"], body) == (202, "yes", b"legacy")
        assert engine.server_stats()["python_application_errors"] == 0
    finally:
        await engine.aclose()
