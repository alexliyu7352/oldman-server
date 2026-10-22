"""Each case runs in a fresh process: one Seastar runtime per interpreter."""
import asyncio
import http.client
from dataclasses import replace
import random
import zlib

import pytest
from oldman_server.native import engine as api
from test_http import config, request, wait_stat


@pytest.mark.asyncio
async def test_python_coroutine_controls_native_operation_and_response():
    cfg = replace(config(decision_timeout_ms=2000), batch_decisions=True)
    before, after = [], []
    async def business(req):
        level = int(req.parameter)
        before.append((req.body_size, level))
        await asyncio.sleep(.001)  # Ordinary Python awaits remain usable.
        result = await req.compress(level=level)
        after.append(result.size)
        if level == 1:
            return api.HttpResponse(status=422)
        return api.HttpResponse(body=result, tag=str(result.size))
    engine = await api.Engine.open(cfg, http_handler=business)
    payload = random.Random(38).randbytes(65536)
    try:
        replies = await asyncio.gather(*(request(cfg.server.http_port,
            f"/python/compress?level={level}", payload) for level in (1, 6, 9)))
        assert [r[0] for r in replies] == [422, 200, 200]
        for level, (status, headers, body) in zip((1, 6, 9), replies):
            if status == 200:
                assert body == zlib.compress(payload, level)
                assert headers["x-app-result"] == str(len(body))
        assert sorted(before) == [(len(payload), n) for n in (1, 6, 9)]
        assert len(after) == 3
        await wait_stat(engine, "active_requests", lambda n: n == 0)
        stats = engine.server_stats()
        assert stats["python_http_started"] == stats["python_http_finished"] == 3
        assert stats["python_http_resumed"] == 3
        assert stats["decision_return_items"] == 6
        assert stats["body_bytes_to_python"] == 0
        assert not engine._http_tasks
    finally:
        await engine.aclose()
    assert engine.server_stats()["reserved_bytes"] == 0


@pytest.mark.asyncio
async def test_async_business_error_timeout_and_shutdown_release_capacity():
    cfg = config(decision_timeout_ms=150)
    entered = asyncio.Event()
    async def business(req):
        if req.parameter == "error":
            await req.compress(level=1)
            raise ValueError("post-work failure")
        entered.set()
        await asyncio.Event().wait()
    engine = await api.Engine.open(cfg, http_handler=business)
    try:
        reply = await request(cfg.server.http_port, "/python/compress?level=error", b"abc"*1000)
        assert reply[0] == 500
        reply = await request(cfg.server.http_port, "/python/compress?level=wait", b"abc")
        assert reply[0] == 504
        await asyncio.sleep(.02)
        assert not engine._http_tasks
        entered.clear()
        client = asyncio.create_task(request(cfg.server.http_port, "/python/compress?level=wait", b"abc"))
        await entered.wait()
        await engine.aclose()
        # Existing server shutdown aborts live sockets; a pending request either
        # receives the cancellation status or observes that connection close.
        try:
            assert (await client)[0] == 503
        except (http.client.RemoteDisconnected, ConnectionResetError):
            pass
        assert not engine._http_waiting
    finally:
        await engine.aclose()
    assert engine.server_stats()["active_requests"] == 0
    assert engine.server_stats()["reserved_bytes"] == 0


@pytest.mark.asyncio
async def test_shared_cpu_pool_and_duplicate_operation_are_explicit():
    cfg = config()
    cfg = replace(cfg, worker_cpus=cfg.shard_cpus, share_worker_cpus=True)
    async def business(req):
        result = await req.compress(level=6)
        with pytest.raises(RuntimeError, match="once"):
            await req.compress(level=6)
        return api.HttpResponse(body=result, tag=str(result.size))
    engine = await api.Engine.open(cfg, http_handler=business)
    try:
        reply = await request(cfg.server.http_port, "/python/compress?level=6", b"xy"*4096)
        assert reply[0] == 200
        assert zlib.decompress(reply[2]) == b"xy"*4096
        assert {w["affinity"][0] for w in engine.workers()} == set(cfg.shard_cpus)
    finally:
        await engine.aclose()


@pytest.mark.asyncio
async def test_cooperative_http_operation_runs_on_owner_reactor_without_pool():
    await _operation_on_reactor(cooperative=True)


@pytest.mark.asyncio
async def test_inline_http_operation_runs_on_owner_reactor_without_pool():
    await _operation_on_reactor(cooperative=False)


async def _operation_on_reactor(cooperative):
    cfg = config(**{"cooperative_compression" if cooperative else "inline_compression": True})
    cfg = replace(cfg, worker_cpus=())
    async def business(req):
        result = await req.compress(level=6)
        return api.HttpResponse(body=result, tag=str(result.size))
    engine = await api.Engine.open(cfg, http_handler=business)
    payload = random.Random(67).randbytes(262144)
    try:
        replies = await asyncio.gather(*(request(cfg.server.http_port, "/python/compress?level=6", payload)
            for _ in range(6)))
        tids = {s["shard"]: s["tid"] for s in engine.shards()}
        for status, headers, body in replies:
            assert status == 200
            assert int(headers["x-probe-worker"]) == tids[int(headers["x-probe-shard"])]
            assert body == zlib.compress(payload, 6)
        assert not engine.workers()
    finally:
        await engine.aclose()
    assert engine.server_stats()["reserved_bytes"] == 0


@pytest.mark.asyncio
async def test_timeout_during_native_work_keeps_504_and_correlation_until_done():
    cfg = config(decision_timeout_ms=30)
    cfg = replace(cfg, shard_cpus=cfg.shard_cpus[:1], worker_cpus=cfg.worker_cpus[:1])
    async def business(req):
        body = await req.compress()
        return api.HttpResponse(body=body)
    engine = await api.Engine.open(cfg, http_handler=business)
    try:
        busy = asyncio.create_task(engine.blocking_wait(150))
        while not engine.stats()["pool_active"]:
            await asyncio.sleep(.001)
        reply = await request(cfg.server.http_port, "/python/compress", b"test")
        assert reply[0] == 504
        await busy
        await wait_stat(engine, "active_requests", lambda n: n == 0)
        assert not engine._http_waiting
    finally:
        await engine.aclose()


@pytest.mark.asyncio
async def test_native_queue_failure_completes_python_await_before_deadline():
    cfg = config(decision_timeout_ms=2000)
    cfg = replace(cfg, max_pending=1, shard_cpus=cfg.shard_cpus[:1], worker_cpus=cfg.worker_cpus[:1])
    failures = []
    async def business(req):
        try:
            body = await req.compress()
            return api.HttpResponse(body=body)
        except api.NativeOperationError as error:
            failures.append(error.status)
            raise
    engine = await api.Engine.open(cfg, http_handler=business)
    try:
        busy = asyncio.create_task(engine.blocking_wait(200))
        while not engine.stats()["pool_active"]:
            await asyncio.sleep(.001)
        replies = await asyncio.gather(*(request(cfg.server.http_port, "/python/compress", b"test") for _ in range(2)))
        assert sorted(r[0] for r in replies) == [200, 503]
        await busy
        await asyncio.sleep(.02)
        assert failures == [503]
        assert not engine._http_tasks
        assert not engine._http_waiting
    finally:
        await engine.aclose()


@pytest.mark.asyncio
async def test_shielded_operation_gets_terminal_error_after_handler_timeout():
    cfg = config(decision_timeout_ms=2000)
    cfg = replace(cfg, shard_cpus=cfg.shard_cpus[:1], worker_cpus=cfg.worker_cpus[:1])
    jobs = []
    async def business(req):
        job = asyncio.create_task(req.compress())
        jobs.append(job)
        await asyncio.wait_for(asyncio.shield(job), .01)
    engine = await api.Engine.open(cfg, http_handler=business)
    try:
        busy = asyncio.create_task(engine.blocking_wait(100))
        while not engine.stats()["pool_active"]:
            await asyncio.sleep(.001)
        assert (await request(cfg.server.http_port, "/python/compress", b"test"))[0] == 504
        with pytest.raises(api.NativeOperationError) as caught:
            await asyncio.wait_for(jobs[0], .1)
        assert caught.value.status == 504
        await busy
        assert not engine._http_waiting
    finally:
        await engine.aclose()


@pytest.mark.asyncio
async def test_python_selects_operation_before_native_body_read():
    cfg = config()
    entered = asyncio.Event()
    async def business(req):
        assert req.body_size == 4
        entered.set()
        body = await req.compress()
        return api.HttpResponse(body=body, tag=str(body.size))
    engine = await api.Engine.open(cfg, http_handler=business)
    reader, writer = await asyncio.open_connection("127.0.0.1", cfg.server.http_port)
    try:
        writer.write(b"POST /python/compress HTTP/1.1\r\nHost: local\r\nContent-Length: 4\r\n\r\n")
        await writer.drain()
        await asyncio.wait_for(entered.wait(), .5)
        writer.write(b"data")
        await writer.drain()
        assert b"200" in await reader.readline()
        while await reader.readline() != b"\r\n":
            pass
        parts = []
        while n := int((await reader.readline()).strip(), 16):
            parts.append(await reader.readexactly(n))
            assert await reader.readexactly(2) == b"\r\n"
        assert await reader.readexactly(2) == b"\r\n"
        assert b"".join(parts) == zlib.compress(b"data", 6)
    finally:
        writer.close()
        await writer.wait_closed()
        await engine.aclose()
