"""Run each integration case in a fresh process; the native runtime is one-shot."""
import asyncio
from dataclasses import replace
import zlib

import pytest
from oldman_server.native import engine as api
from test_http import config, request, wait_stat


@pytest.mark.asyncio
async def test_batch_return_preserves_per_request_results_and_owned_body():
    assert "batch_decisions" in api.Config.__dataclass_fields__, "batch return option is missing"
    cfg = replace(config(), batch_decisions=True)
    def decide(req):
        if req.route == "compress": return True
        if req.body_size % 3 == 0: return True
        if req.body_size % 3 == 1: return False
        raise RuntimeError("intentional decision failure")
    engine = await api.Engine.open(cfg, decision_handler=decide)
    loop = asyncio.get_running_loop()
    try:
        # Stay below this host's default HTTP client executor capacity (13).
        # Accumulate one known batch containing requests from both reactors.
        loop.remove_reader(engine.notification_fd)
        jobs = [asyncio.create_task(request(cfg.server.http_port, f"/hybrid/fixed?size={n}"))
                for n in range(1, 13)]
        await wait_stat(engine, "queued_decisions", lambda n: n == 12)
        engine._drain()
        loop.add_reader(engine.notification_fd, engine._drain)
        replies = await asyncio.gather(*jobs)
        for n, (status, _, body) in enumerate(replies, 1):
            assert status == (200 if n % 3 == 0 else 403 if n % 3 == 1 else 500)
            if status == 200: assert body == b"x" * n
        stats = engine.server_stats()
        assert stats["python_decisions"] == 12
        assert stats["decision_return_batches"] == 1
        assert stats["decision_return_items"] == 12
        assert stats["decision_return_posts"] == 2
        # Binary copied bodies must survive conversion and asynchronous submission.
        payloads = [bytes(range(256))*16, b"\x00\xff\x00body"*1000]
        replies = await asyncio.gather(*(request(cfg.server.http_port, "/copy/compress", p) for p in payloads))
        assert [r[0] for r in replies] == [200, 200]
        assert [r[2] for r in replies] == [zlib.compress(p, 6) for p in payloads]
        await wait_stat(engine, "active_requests", lambda n: n == 0)
    finally:
        loop.add_reader(engine.notification_fd, engine._drain)
        await engine.aclose()
    assert engine.server_stats()["reserved_bytes"] == 0


@pytest.mark.asyncio
async def test_batch_late_duplicate_decisions_and_shutdown_release_waiters():
    assert "batch_decisions" in api.Config.__dataclass_fields__, "batch return option is missing"
    cfg = replace(config(decision_timeout_ms=100), batch_decisions=True)
    engine = await api.Engine.open(cfg)
    loop = asyncio.get_running_loop()
    stale = []
    try:
        loop.remove_reader(engine.notification_fd)
        jobs = [asyncio.create_task(request(cfg.server.http_port, "/hybrid/fixed?size=8")) for _ in range(4)]
        await wait_stat(engine, "queued_decisions", lambda n: n == 4)
        events = engine._native.drain_requests(64)
        stale = [(r[0], r[1], 1, b"") for r in events]
        assert len(stale) == 4
        assert [r[0] for r in await asyncio.gather(*jobs)] == [504]*4
        engine._native.resolve_requests(stale + stale + [(1234, 999, 1, b"")])
        await wait_stat(engine, "active_requests", lambda n: n == 0)
        assert engine.server_stats()["pending_decisions"] == 0
        assert engine.server_stats()["queued_decisions"] == 0
    finally:
        loop.add_reader(engine.notification_fd, engine._drain)
        await engine.aclose()
    engine._native.resolve_requests(stale)
    assert engine.server_stats()["reserved_bytes"] == 0
