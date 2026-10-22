"""Generic HTTP application tests; run each test in its own Seastar process."""
import asyncio
from dataclasses import replace
import http.client
import json
import socket

import pytest

from oldman_server.native import engine as api
from test_http import config, request, wait_stat


async def exchange(port, method, target, body=None, headers=None):
    def send():
        client = http.client.HTTPConnection("127.0.0.1", port, timeout=4)
        try:
            client.request(method, target, body=body, headers=headers or {})
            response = client.getresponse()
            return response.status, dict((k.lower(), v) for k, v in response.getheaders()), response.read()
        finally:
            client.close()
    return await asyncio.to_thread(send)


@pytest.mark.asyncio
async def test_framework_dynamic_post_and_get_head_routes():
    from seastar_http import App, Response
    app = App()
    calls = []

    @app.post("/echo")
    async def echo(req):
        calls.append((req.method, req.path, req.query_string, req.header("X-Token"), req.body))
        return Response(req.body + b"/" + req.query_string.encode("latin-1"),
                        status=201, headers=(("X-Echo", "caf\xe9"),))

    @app.get("/dynamic")
    async def dynamic(req):
        return {"message": "Hello, World!", "n": len(calls)}

    @app.route("/dynamic", methods=("HEAD",))
    async def explicit_head(req):
        return Response("ignored", headers=(("X-Head", "explicit"),))

    @app.get("/no-content")
    async def no_content(req):
        return Response("suppressed", status=204)

    @app.get("/unchanged")
    async def unchanged(req):
        return Response("suppressed", status=304)

    @app.get("/x")
    async def exact(req):
        return "plain"

    @app.get("/x/")
    async def trailing(req):
        return "trailing"

    cfg = config()
    delayed_registration = app.get("/late")
    engine = await app.start(cfg)
    try:
        with pytest.raises(RuntimeError, match="before startup"):
            delayed_registration(dynamic)
        status, headers, body = await exchange(cfg.server.http_port, "POST", "/echo?q=%FF",
                                               b"raw\x00\xff", {"X-Token": "123"})
        assert (status, headers["x-echo"], body) == (201, "caf\xe9", b"raw\x00\xff/q=%FF")
        assert calls == [("POST", "/echo", "q=%FF", "123", b"raw\x00\xff")]
        status, headers, body = await request(cfg.server.http_port, "/dynamic")
        assert status == 200 and headers["content-type"] == "application/json"
        assert body == b'{"message":"Hello, World!","n":1}'
        head_status, head_headers, head_body = await exchange(cfg.server.http_port, "HEAD", "/dynamic")
        assert (head_status, head_headers["x-head"], head_body) == (200, "explicit", b"")
        assert (await exchange(cfg.server.http_port, "HEAD", "/x"))[0::2] == (200, b"")
        assert [(await request(cfg.server.http_port, path))[2] for path in ("/x", "/x/")] == [b"plain", b"trailing"]
        assert (await request(cfg.server.http_port, "/no-content"))[0] == 500
        assert (await request(cfg.server.http_port, "/unchanged"))[0] == 500
        assert (await request(cfg.server.http_port, "/missing"))[0] == 404
    finally:
        await engine.aclose()
    assert engine.server_stats()["python_application_active"] == 0


@pytest.mark.asyncio
async def test_framework_per_request_handler_and_serializer(monkeypatch):
    from seastar_http import App
    import seastar_http
    calls = []
    serializer = seastar_http.json.dumps

    def counted(value, **kwargs):
        calls.append(value)
        return serializer(value, **kwargs)

    monkeypatch.setattr(seastar_http.json, "dumps", counted)
    app = App()
    invocation = 0

    @app.get("/json")
    async def handler(req):
        nonlocal invocation
        invocation += 1
        return {"message": "Hello, World!", "invocation": invocation}

    cfg = config()
    engine = await app.start(cfg)
    try:
        replies = [await request(cfg.server.http_port, "/json") for _ in range(4)]
        assert [json.loads(body)["invocation"] for _, _, body in replies] == [1, 2, 3, 4]
        assert all(status == 200 for status, _, _ in replies)
        assert len(calls) == invocation == 4
    finally:
        await engine.aclose()


@pytest.mark.asyncio
async def test_framework_handler_awaits_real_aiohttp_upstream():
    import aiohttp
    from aiohttp import web
    from seastar_http import App
    upstream = web.Application()
    seen = []

    async def answer(req):
        seen.append(req.query["value"])
        await asyncio.sleep(.005)
        return web.json_response({"value": req.query["value"]})

    upstream.router.add_get("/reply", answer)
    runner = web.AppRunner(upstream)
    await runner.setup()
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        upstream_port = sock.getsockname()[1]
    await web.TCPSite(runner, "127.0.0.1", upstream_port).start()
    app = App()

    @app.get("/proxy")
    async def proxy(req):
        async with aiohttp.ClientSession() as session:
            async with session.get(f"http://127.0.0.1:{upstream_port}/reply?value={req.query_string}") as response:
                return await response.json()

    cfg = config()
    engine = await app.start(cfg)
    try:
        assert json.loads((await request(cfg.server.http_port, "/proxy?alpha"))[2]) == {"value": "alpha"}
        assert seen == ["alpha"]
    finally:
        await engine.aclose()
        await runner.cleanup()


@pytest.mark.asyncio
async def test_framework_invalid_output_and_headers_isolated():
    from seastar_http import App, Response
    app = App()

    @app.get("/bad")
    async def bad(req):
        return object()

    @app.get("/injection")
    async def injection(req):
        return Response("unsafe", headers=(("X-Injected", "a\r\nb"),))

    @app.get("/framing")
    async def framing(req):
        return Response("unsafe", headers=(("Content-Length", "6"),))

    @app.get("/huge-header")
    async def huge_header(req):
        return Response("unsafe", headers=(("X-Big", "a" * 8192),))

    @app.get("/many-headers")
    async def many_headers(req):
        return Response("unsafe", headers=tuple((f"X-{i}", "x") for i in range(129)))

    @app.get("/ok")
    async def ok(req):
        return "ok"

    cfg = config()
    engine = await app.start(cfg)
    try:
        assert [(await request(cfg.server.http_port, path))[0]
                for path in ("/bad", "/injection", "/framing", "/huge-header",
                             "/many-headers", "/ok")] == [500, 500, 500, 500, 500, 200]
        await wait_stat(engine, "active_requests", lambda n: n == 0)
        assert engine.server_stats()["python_application_errors"] >= 5
    finally:
        await engine.aclose()


@pytest.mark.asyncio
async def test_framework_native_timeout_cancels_handler_without_late_reply():
    from seastar_http import App
    app = App()
    started, cancelled = asyncio.Event(), asyncio.Event()

    @app.get("/slow")
    async def slow(req):
        started.set()
        try:
            await asyncio.sleep(10)
        finally:
            cancelled.set()

    cfg = config(decision_timeout_ms=40)
    engine = await app.start(cfg)
    try:
        assert (await request(cfg.server.http_port, "/slow"))[0] == 504
        await asyncio.wait_for(started.wait(), 2)
        await asyncio.wait_for(cancelled.wait(), 2)
        await wait_stat(engine, "python_application_active", lambda n: n == 0)
        assert (await request(cfg.server.http_port, "/slow"))[0] == 504
    finally:
        await engine.aclose()
    assert engine.server_stats()["active_requests"] == engine.server_stats()["reserved_bytes"] == 0


@pytest.mark.asyncio
async def test_framework_close_cancels_pending_and_drains():
    from seastar_http import App
    app = App()
    started, cancelled = asyncio.Event(), asyncio.Event()

    @app.get("/blocked")
    async def blocked(req):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    cfg = config(decision_timeout_ms=2000)
    engine = await app.start(cfg)
    client = asyncio.create_task(request(cfg.server.http_port, "/blocked"))
    try:
        await asyncio.wait_for(started.wait(), 2)
        await asyncio.wait_for(engine.aclose(), 5)
        await asyncio.wait_for(cancelled.wait(), 2)
        # Native shutdown aborts socket I/O while stopping the HTTP listener;
        # the selected 503 can be delivered, or the peer can observe that abort.
        try:
            assert (await client)[0] == 503
        except (http.client.RemoteDisconnected, ConnectionResetError):
            pass
    finally:
        if not client.done():
            client.cancel()
        await engine.aclose()
    stats = engine.server_stats()
    assert stats["python_application_active"] == stats["active_requests"] == stats["reserved_bytes"] == 0


@pytest.mark.asyncio
async def test_framework_eager_and_failing_task_factory():
    from seastar_http import App
    app = App()

    @app.get("/instant")
    async def instant(req):
        return {"instant": True}

    cfg = config()
    engine = await app.start(cfg)
    loop = asyncio.get_running_loop()
    original = loop.get_task_factory()
    try:
        loop.set_task_factory(asyncio.eager_task_factory)
        assert json.loads((await request(cfg.server.http_port, "/instant"))[2]) == {"instant": True}

        def failing_factory(loop, coroutine, **kwargs):
            if getattr(coroutine, "cr_code", None) and coroutine.cr_code.co_name == "_serve_application":
                coroutine.close()
                raise RuntimeError("task creation failed")
            return asyncio.Task(coroutine, loop=loop, **kwargs)
        loop.set_task_factory(failing_factory)
        assert (await request(cfg.server.http_port, "/instant"))[0] == 500
    finally:
        loop.set_task_factory(original)
        await engine.aclose()
    assert engine.server_stats()["python_application_active"] == 0


@pytest.mark.asyncio
async def test_framework_bounds_and_truncated_upload():
    from seastar_http import App
    app = App()
    calls = []

    @app.post("/input")
    async def input_handler(req):
        calls.append(req.body)
        return b"x" * 9

    cfg = config(max_body_bytes=8, max_response_bytes=8)
    engine = await app.start(cfg)
    try:
        assert (await request(cfg.server.http_port, "/input", b"x" * 9))[0] == 413
        assert (await request(cfg.server.http_port, "/input", b"x" * 8))[0] == 500
        reader, writer = await asyncio.open_connection("127.0.0.1", cfg.server.http_port)
        writer.write(b"POST /input HTTP/1.1\r\nHost: local\r\nContent-Length: 8\r\n\r\nx")
        await writer.drain()
        writer.write_eof()
        assert int((await asyncio.wait_for(reader.readline(), 2)).split()[1]) == 400
        writer.close()
        await writer.wait_closed()
        assert calls == [b"x" * 8]
        await wait_stat(engine, "active_requests", lambda n: n == 0)
    finally:
        await engine.aclose()


def test_framework_route_registration_validation():
    from seastar_http import App
    app = App()

    @app.get("/unique")
    async def handler(req):
        return "ok"

    with pytest.raises(ValueError):
        app.get("/unique")(handler)
    with pytest.raises(ValueError):
        app.get("/native/fixed")(handler)
    with pytest.raises(ValueError):
        app.get("/policy")(handler)
    with pytest.raises(ValueError):
        app.get("/bad path")(handler)
    with pytest.raises(ValueError):
        app.get("/caf\xe9")(handler)
    with pytest.raises(ValueError):
        app.route("/bad", methods=("TRACE",))(handler)
