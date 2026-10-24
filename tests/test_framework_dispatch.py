"""Framework-owned HTTP routing; every test gets a fresh native lifecycle."""
import asyncio
import http.client
from dataclasses import replace

from oldman_server.native import _native as native
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


def assert_released(engine):
    stats = engine.server_stats()
    for key in ("active_requests", "reserved_bytes", "pending_application_nodes",
                "pending_decisions", "queued_decisions", "python_application_active"):
        assert stats[key] == 0, (key, stats[key])


@pytest.mark.asyncio
async def test_dispatch_all_rejects_ambiguous_configs_before_starting_runtime():
    cfg = config(application_dispatch_all=True)
    handler = lambda req: (200, b"ok", ())
    for changes in ({"http_port": 0}, {"application_dispatch_all": "yes"},
                    {"application_dispatch_all": False}):
        with pytest.raises(ValueError):
            await api.Engine.open(replace(cfg, server=replace(cfg.server, **changes)),
                                  application_handler=handler)
    with pytest.raises(ValueError):
        await api.Engine.open(cfg)
    with pytest.raises(TypeError):
        await api.Engine.open(cfg, application_handler=object())
    for changes in ({"http_port": 0}, {"application_dispatch_all": False}):
        server = native.ServerConfig()
        server.http_port = cfg.server.http_port
        server.application_dispatch_all = True
        for key, value in changes.items():
            setattr(server, key, value)
        with pytest.raises(ValueError):
            native.Runtime(list(cfg.shard_cpus), server)
    # Invalid combinations must not consume the process's one native lifecycle.
    engine = await api.Engine.open(cfg, application_handler=handler)
    try:
        assert (await request(cfg.server.http_port, "/unlisted"))[0::2] == (200, b"ok")
    finally:
        await engine.aclose()
    assert_released(engine)


@pytest.mark.asyncio
async def test_dispatch_all_owns_paths_and_methods_without_native_route_interception():
    cfg = config(application_dispatch_all=True)
    seen = []
    async def handler(req):
        await asyncio.sleep(0)
        seen.append((req.method, req.target, req.body))
        return 200, b"framework", (("X-Dispatch", "python"),)
    engine = await api.Engine.open(cfg, application_handler=handler)
    paths = ("/never-registered/123", "/native/fixed", "/hybrid/fixed", "/copy/fixed",
             "/policy/fixed", "/python/fetch", "/reference/fetch", "/json")
    try:
        for path in paths:
            status, headers, body = await request(cfg.server.http_port, path)
            assert (status, headers["x-dispatch"], body) == (200, "python", b"framework")
        for method in ("GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"):
            status, headers, body = await exchange(cfg.server.http_port, method, "/method/456?raw=%FF")
            assert status == 200 and headers["x-dispatch"] == "python"
            assert body == (b"" if method == "HEAD" else b"framework")
        assert [m for m, _, _ in seen[len(paths):]] == ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"]
        assert all(t == b"/method/456?raw=%FF" for _, t, _ in seen[len(paths):])
    finally:
        await engine.aclose()
    assert_released(engine)


@pytest.mark.asyncio
async def test_dispatch_all_preserves_limits_deadlines_and_response_validation():
    cfg = config(application_dispatch_all=True, max_requests=1, max_body_bytes=8,
                 max_response_bytes=8, decision_timeout_ms=500)
    cfg = replace(cfg, shard_cpus=cfg.shard_cpus[:1])
    started, release, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()
    seen = []
    async def handler(req):
        seen.append(req.target)
        if req.path == "/hold/1":
            started.set()
            await release.wait()
        if req.path == "/slow/2":
            try:
                await asyncio.Future()
            finally:
                cancelled.set()
        if req.path == "/bad-size/3":
            return 200, b"123456789", ()
        if req.path == "/bad-header/4":
            return 200, b"ok", (("Bad Header", "x"),)
        if req.path == "/framing/5":
            # The host writes its own framing in place of the application's.
            return 200, b"ok", (("Content-Length", "99"), ("Connection", "close"))
        return 200, req.body or b"ok", ()
    engine = await api.Engine.open(cfg, application_handler=handler)
    pending = None
    try:
        assert (await request(cfg.server.http_port, "/body/0", b"123456789"))[0] == 413
        assert seen == []
        assert (await request(cfg.server.http_port, "/body/0", b"12345678"))[0::2] == (200, b"12345678")
        await wait_stat(engine, "active_requests", lambda n: n == 0)
        pending = asyncio.create_task(request(cfg.server.http_port, "/hold/1"))
        await asyncio.wait_for(started.wait(), 2)
        assert (await request(cfg.server.http_port, "/capacity/rejected"))[0] == 503
        assert b"/capacity/rejected" not in seen
        release.set()
        assert (await pending)[0] == 200
        await wait_stat(engine, "active_requests", lambda n: n == 0)
        assert (await request(cfg.server.http_port, "/slow/2"))[0] == 504
        await asyncio.wait_for(cancelled.wait(), 2)
        await wait_stat(engine, "active_requests", lambda n: n == 0)
        assert [(await request(cfg.server.http_port, path))[0]
                for path in ("/bad-size/3", "/bad-header/4", "/ok/6")] == [500, 500, 200]
        status, headers, body = await request(cfg.server.http_port, "/framing/5")
        assert (status, headers["content-length"], headers["connection"], body) == (200, "2", "keep-alive", b"ok")
    finally:
        release.set()
        await engine.aclose()
        if pending is not None:
            await asyncio.gather(pending, return_exceptions=True)
    assert_released(engine)


@pytest.mark.asyncio
async def test_dispatch_all_hands_complete_bodies_that_arrive_after_the_head():
    # The generic framework host has no body streaming: a body arriving in later
    # segments (Content-Length or chunked) is read completely before dispatch.
    cfg = config(application_dispatch_all=True)
    async def handler(req):
        return 200, req.body, ()
    engine = await api.Engine.open(cfg, application_handler=handler)
    try:
        for head, rest in (
                (b"POST /a HTTP/1.1\r\nHost: x\r\nContent-Length: 10\r\n\r\nabc", b"defghij"),
                (b"POST /b HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n",
                 b"7\r\ndefghij\r\n0\r\n\r\n")):
            reader, writer = await asyncio.open_connection("127.0.0.1", cfg.server.http_port)
            writer.write(head)
            await asyncio.sleep(0.05)
            writer.write(rest)
            response = await asyncio.wait_for(reader.readuntil(b"abcdefghij"), 2)
            assert response.startswith(b"HTTP/1.1 200")
            writer.close()
    finally:
        await engine.aclose()
    assert_released(engine)
