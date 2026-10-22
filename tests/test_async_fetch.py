"""Real upstream I/O; run each case in a fresh Seastar process."""
import asyncio
from dataclasses import replace
import http.client

import pytest
from aiohttp import web
from oldman_server.native import engine as api
from test_http import config, free_port, request, wait_stat


async def upstream(handler):
    app = web.Application()
    app.router.add_get("/native/fixed", handler)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    port = free_port()
    await web.TCPSite(runner, "127.0.0.1", port).start()
    return runner, port


@pytest.mark.asyncio
async def test_fetch_uses_selected_upstream_and_python_controls_result():
    # Catches skipped I/O, wrong origin selection, lost status/bytes, or skipped
    # Python continuation. Upstream connections must actually be reused.
    assert hasattr(api.HttpRequest, "fetch"), "native async upstream operation is missing"
    calls, transports, continued = [], set(), []
    async def first(req):
        calls.append(0); transports.add(req.transport)
        return web.Response(body=b"A" * 8192)
    async def second(req):
        calls.append(1)
        return web.Response(status=404, body=b"missing")
    a, pa = await upstream(first)
    b, pb = await upstream(second)
    cfg = replace(config(upstream_ports=(pa, pb)), worker_cpus=(), batch_decisions=True)
    async def business(req):
        if req.parameter == "deny":
            return api.HttpResponse(status=403)
        result = await req.fetch(upstream=int(req.parameter))
        continued.append((result.status, result.size))
        if result.status != 200:
            return api.HttpResponse(status=422)
        with pytest.raises(RuntimeError, match="once"):
            await req.fetch()
        return api.HttpResponse(body=result, tag=f"{result.status}:{result.size}")
    engine = await api.Engine.open(cfg, http_handler=business)
    conn = http.client.HTTPConnection("127.0.0.1", cfg.server.http_port, timeout=5)
    try:
        for _ in range(4):
            status, headers, body = await request(cfg.server.http_port, "/python/fetch?upstream=0", connection=conn)
            assert (status, body, headers["x-app-result"]) == (200, b"A" * 8192, "200:8192")
        assert (await request(cfg.server.http_port, "/python/fetch?upstream=1", connection=conn))[0] == 422
        assert (await request(cfg.server.http_port, "/python/fetch?upstream=deny", connection=conn))[0] == 403
        assert calls == [0, 0, 0, 0, 1]
        assert len(transports) == 1
        assert continued == [(200, 8192)] * 4 + [(404, 7)]
        await wait_stat(engine, "active_requests", lambda n: n == 0)
        stats = engine.server_stats()
        assert stats["python_http_started"] == stats["python_http_finished"] == 6
        assert stats["python_http_resumed"] == stats["upstream_requests"] == 5
        assert stats["body_bytes_to_python"] == 0
        assert not engine.workers()
    finally:
        conn.close()
        await engine.aclose()
        await a.cleanup(); await b.cleanup()
    assert engine.server_stats()["reserved_bytes"] == 0


@pytest.mark.asyncio
async def test_fetch_failure_timeout_and_oversize_release_capacity():
    assert hasattr(api.HttpRequest, "fetch"), "native async upstream operation is missing"
    async def large(req):
        return web.Response(body=b"x" * 16384)
    async def slow(req):
        await asyncio.sleep(.15)
        return web.Response(body=b"ok")
    a, pa = await upstream(large)
    b, pb = await upstream(slow)
    cfg = replace(config(upstream_ports=(pa, pb, free_port()), max_body_bytes=8192,
                         decision_timeout_ms=60), worker_cpus=())
    async def business(req):
        result = await req.fetch(upstream=int(req.parameter))
        return api.HttpResponse(body=result)
    engine = await api.Engine.open(cfg, http_handler=business)
    try:
        for target, status in [(0, 502), (1, 504), (2, 502)]:
            reply = await request(cfg.server.http_port, f"/python/fetch?upstream={target}")
            assert reply[0] == status
        await wait_stat(engine, "active_requests", lambda n: n == 0)
        assert not engine._http_waiting
        assert not engine._http_tasks
    finally:
        await engine.aclose()
        await a.cleanup(); await b.cleanup()
    assert engine.server_stats()["reserved_bytes"] == 0


@pytest.mark.asyncio
async def test_fetch_shutdown_aborts_waiting_upstream():
    assert hasattr(api.HttpRequest, "fetch"), "native async upstream operation is missing"
    entered, release = asyncio.Event(), asyncio.Event()
    async def blocked(req):
        entered.set()
        await release.wait()
        return web.Response(body=b"ok")
    runner, port = await upstream(blocked)
    cfg = replace(config(upstream_ports=(port,), decision_timeout_ms=5000), worker_cpus=())
    async def business(req):
        result = await req.fetch()
        return api.HttpResponse(body=result)
    engine = await api.Engine.open(cfg, http_handler=business)
    client = asyncio.create_task(request(cfg.server.http_port, "/python/fetch"))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        await asyncio.wait_for(engine.aclose(), 1)
        try:
            assert (await client)[0] == 503
        except (http.client.RemoteDisconnected, ConnectionResetError):
            pass
        assert not engine._http_waiting
        assert engine.server_stats()["reserved_bytes"] == 0
    finally:
        release.set()
        await engine.aclose()
        await runner.cleanup()
