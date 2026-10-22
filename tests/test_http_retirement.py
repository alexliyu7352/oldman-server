"""Final Python replies may retire correlation; incomplete awaits may not."""
import asyncio
from dataclasses import replace
import http.client

import pytest
from aiohttp import web
from oldman_server.native import engine as api
from test_http import config, request, wait_stat
from test_async_fetch import upstream


def retiring_config(**kwargs):
    assert hasattr(api.Config, "retire_http_contexts"), "terminal retirement protocol is missing"
    return replace(config(**kwargs), worker_cpus=(), batch_decisions=True, retire_http_contexts=True)


@pytest.mark.asyncio
async def test_final_reply_retires_context_without_terminal_notification():
    cfg = retiring_config()
    async def origin(req):
        return web.Response(body=b"x" * 8192)
    runner, port = await upstream(origin)
    cfg = replace(cfg, server=replace(cfg.server, upstream_ports=(port,)))
    resumed = []
    async def business(req):
        if req.parameter == "deny":
            return api.HttpResponse(status=403)
        body = await req.fetch()
        resumed.append(body.size)
        if req.parameter == "reject":
            return api.HttpResponse(status=422)
        return api.HttpResponse(body=body, tag="checked")
    engine = await api.Engine.open(cfg, http_handler=business)
    try:
        for name, expected in [("ok", 200), ("reject", 422), ("deny", 403)] * 3:
            status, headers, body = await request(cfg.server.http_port, "/python/fetch?upstream=" + name)
            assert status == expected
            if status == 200:
                assert body == b"x" * 8192 and headers["x-app-result"] == "checked"
        await wait_stat(engine, "active_requests", lambda n: n == 0)
        assert resumed == [8192] * 6
        assert not engine._http_waiting
        assert engine.server_stats()["python_terminal_events"] == 0
        assert engine.server_stats()["python_http_started"] == engine.server_stats()["python_http_finished"] == 9
    finally:
        await engine.aclose(); await runner.cleanup()
    assert engine.server_stats()["reserved_bytes"] == 0


@pytest.mark.asyncio
async def test_retirement_keeps_terminal_event_for_shielded_pending_operation():
    cfg = retiring_config(decision_timeout_ms=1000)
    async def delayed(req):
        await asyncio.sleep(.05)
        return web.Response(body=b"ok")
    runner, port = await upstream(delayed)
    cfg = replace(cfg, server=replace(cfg.server, upstream_ports=(port,)))
    pending = []
    async def business(req):
        task = asyncio.create_task(req.fetch()); pending.append(task)
        async with asyncio.timeout(.01):
            body = await asyncio.shield(task)
        return api.HttpResponse(body=body)
    engine = await api.Engine.open(cfg, http_handler=business)
    try:
        assert (await request(cfg.server.http_port, "/python/fetch"))[0] == 504
        with pytest.raises(api.NativeOperationError) as failure:
            await asyncio.wait_for(pending[0], .5)
        assert failure.value.status == 504
        await wait_stat(engine, "active_requests", lambda n: n == 0)
        assert engine.server_stats()["python_terminal_events"] == 1
        assert not engine._http_waiting
    finally:
        await engine.aclose(); await runner.cleanup()
    assert engine.server_stats()["reserved_bytes"] == 0


@pytest.mark.asyncio
async def test_retirement_preserves_native_failure_and_shutdown_cleanup():
    cfg = retiring_config(decision_timeout_ms=1000)
    entered, release = asyncio.Event(), asyncio.Event()
    async def blocked(req):
        entered.set(); await release.wait()
        return web.Response(body=b"ok")
    runner, port = await upstream(blocked)
    cfg = replace(cfg, server=replace(cfg.server, upstream_ports=(port,)))
    async def business(req):
        body = await req.fetch()
        return api.HttpResponse(body=body)
    engine = await api.Engine.open(cfg, http_handler=business)
    client = asyncio.create_task(request(cfg.server.http_port, "/python/fetch"))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        await asyncio.wait_for(engine.aclose(), 1)
        try:
            assert (await client)[0] == 503
        except (http.client.RemoteDisconnected, ConnectionResetError):
            pass
        assert not engine._http_waiting and not engine._http_tasks
        assert engine.server_stats()["python_terminal_events"] == 1
    finally:
        release.set(); await engine.aclose(); await runner.cleanup()
    assert engine.server_stats()["active_requests"] == engine.server_stats()["reserved_bytes"] == 0
