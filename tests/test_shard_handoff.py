"""Full admission and terminal ownership; one runtime per test process."""
import asyncio
from dataclasses import replace

import aiohttp
import pytest
from oldman_server.native import engine as api
from test_http import config, wait_stat
from test_native_boundary import async_request


@pytest.mark.asyncio
async def test_full_inbox_timeout_refill_late_reply_and_shutdown():
    # One reactor makes full admission deterministic, independent of reuseport.
    # Broken cancel-before-DONE would overflow the fixed inbox on the first wave;
    # stale ticket cancellation would lose refill events or their responses.
    cfg = config(application_routes=(("POST", "/slots"),), max_requests=4,
                 decision_timeout_ms=400, max_response_bytes=64)
    cfg = replace(cfg, shard_cpus=cfg.shard_cpus[:1])
    engine = await api.Engine.open(cfg)
    loop = asyncio.get_running_loop()
    jobs, retained = [], []

    def launch(tag):
        task = asyncio.create_task(async_request(
            cfg.server.http_port, f"/slots?{tag}", b"request\x00\xff"))
        jobs.append(task)
        return task

    async def empty():
        await wait_stat(engine, "active_requests", lambda n: n == 0)
        stats = engine.server_stats()
        assert stats["reserved_bytes"] == 0
        assert stats["pending_decisions"] == 0
        assert stats["queued_decisions"] == 0
        assert stats["pending_application_nodes"] == 0

    try:
        loop.remove_reader(engine.notification_fd)
        expired = [launch(f"timeout-{i}") for i in range(4)]
        await wait_stat(engine, "queued_decisions", lambda n: n == 4)
        assert engine.server_stats()["active_requests"] == 4
        # Pop only one event; remaining events must be canceled before DONE.
        stale_id, stale_shard, owned = engine._native.drain_requests_owned(1)[0]
        retained.append(owned)
        assert engine.server_stats()["pending_application_nodes"] == 1
        assert [r[0] for r in await asyncio.gather(*expired)] == [504] * 4
        await wait_stat(engine, "pending_decisions", lambda n: n == 0)
        stats = engine.server_stats()
        assert stats["queued_decisions"] == stats["active_requests"] == 4
        assert stats["reserved_bytes"] > 0
        assert stats["peak_requests"] == 4
        assert stats["pending_application_nodes"] == 0
        # DONE holds the original lease. New HTTP admission remains rejected
        # until the terminal events are actually drained.
        assert (await launch("blocked-by-terminal"))[0] == 503
        terminal = engine._native.drain_requests(100)
        assert len(terminal) == 4
        assert all(e[3] == "application_done" and e[4] == 504 for e in terminal)
        assert stale_id in {e[7] for e in terminal}
        await empty()

        refill = [launch(f"refill-{i}") for i in range(4)]
        await wait_stat(engine, "queued_decisions", lambda n: n == 4)
        # With one shard the first new ID follows the largest DONE ID. Exercise
        # the low-level response-before-drain contract without consuming it.
        early_id = max(e[0] for e in terminal) + (1 << 16)
        engine._native.resolve_request(early_id, 0, 200, b"early", True)
        await wait_stat(engine, "queued_decisions", lambda n: n == 3)
        engine._native.resolve_request(stale_id, stale_shard, 200, b"late", True)
        remaining = engine._native.drain_requests_owned(100)
        assert len(remaining) == 3
        assert [e[0] for e in remaining] == sorted(e[0] for e in remaining)
        assert early_id not in {e[0] for e in remaining}
        assert engine.server_stats()["pending_application_nodes"] == 3
        retained.extend(e[2] for e in remaining)
        engine._native.resolve_http_responses(
            [(rid, shard, 200, req.query_string.encode(), ())
             for rid, shard, req in remaining])
        replies = await asyncio.gather(*refill)
        assert all(status == 200 for status, _, _ in replies)
        assert sum(body == b"early" for _, _, body in replies) == 1
        assert all(body == b"early" or body == f"refill-{i}".encode()
                   for i, (_, _, body) in enumerate(replies))
        await empty()

        # Closing while every queue slot is occupied completes native waiters,
        # destroys Service objects and then drains Shared's terminal events.
        for i in range(4):
            launch(f"shutdown-{i}")
        await wait_stat(engine, "queued_decisions", lambda n: n == 4)
    finally:
        loop.add_reader(engine.notification_fd, engine._drain)
        await engine.aclose()
        await asyncio.gather(*jobs, return_exceptions=True)
    await empty()
    del engine
    assert retained
    assert all(req.method == "POST" and req.path == "/slots" and
               req.body == b"request\x00\xff" and isinstance(req.headers, tuple)
               for req in retained)


@pytest.mark.asyncio
async def test_terminal_events_release_after_reactors_have_joined():
    # Reclaiming every event through a future reply is invalid: DONE has no
    # reply, and the public drain API still works after all reactors have exited.
    cfg = config(application_routes=(("POST", "/after-stop"),), max_requests=4,
                 decision_timeout_ms=3000, max_response_bytes=64)
    cfg = replace(cfg, shard_cpus=cfg.shard_cpus[:1])
    engine = await api.Engine.open(cfg)
    loop = asyncio.get_running_loop()
    jobs, retained = [], []
    try:
        loop.remove_reader(engine.notification_fd)
        jobs = [asyncio.create_task(async_request(
            cfg.server.http_port, f"/after-stop?{i}", b"request\x00\xff")) for i in range(4)]
        await wait_stat(engine, "queued_decisions", lambda n: n == 4)
        request_id, shard, request = engine._native.drain_requests_owned(1)[0]
        retained.append(request)
        assert engine.server_stats()["queued_decisions"] == 3
        assert engine.server_stats()["pending_application_nodes"] == 1
        engine._native.stop()
        async with asyncio.timeout(5):
            while engine.stats()["state"] < 3:
                await asyncio.sleep(.001)
        await asyncio.to_thread(engine._native.join)
        assert engine.stats()["state"] == 3
        assert engine.stats()["live_workers"] == 0
        stopped = engine.server_stats()
        assert stopped["pending_decisions"] == stopped["pending_application_nodes"] == 0
        assert stopped["queued_decisions"] == stopped["active_requests"] == 4
        assert stopped["reserved_bytes"] > 0
        assert stopped["python_terminal_events"] == 4
        # Neither a reply nor an alien callback can be required for reclamation.
        engine._native.resolve_requests([(request_id, shard, 200, b"late", True)])
        assert engine.server_stats()["decision_return_posts"] == stopped["decision_return_posts"] == 0
        first = engine._native.drain_requests_owned(1)
        assert len(first) == 1 and first[0][3] == "application_done"
        partial = engine.server_stats()
        assert partial["queued_decisions"] == partial["active_requests"] == 3
        assert 0 < partial["reserved_bytes"] < stopped["reserved_bytes"]
        rest = engine._native.drain_requests(100)
        assert len(rest) == 3 and all(e[3] == "application_done" for e in rest)
        terminal = first + rest  # Retaining tuples cannot retain admission.
        assert request_id in {e[7] for e in terminal}
        assert all(e[4] == 503 for e in terminal)
        final = engine.server_stats()
        assert final["queued_decisions"] == final["active_requests"] == final["reserved_bytes"] == 0
        assert final["pending_application_nodes"] == 0
        replies = await asyncio.gather(*jobs, return_exceptions=True)
        assert all(isinstance(reply, aiohttp.ClientConnectionError) or
                   isinstance(reply, tuple) and reply[0] == 503 for reply in replies)
    finally:
        loop.add_reader(engine.notification_fd, engine._drain)
        # Deliver the stopped state even if an earlier assertion failed while
        # the reader was removed. aclose still owns normal fd/Task cleanup.
        engine._drain()
        await engine.aclose()
        await asyncio.gather(*jobs, return_exceptions=True)
    del engine
    assert retained and all(req.path == "/after-stop" and req.body == b"request\x00\xff"
                            and isinstance(req.headers, tuple) for req in retained)
