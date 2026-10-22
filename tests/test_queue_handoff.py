"""Real queue handoff contracts; run this lifecycle in a fresh process."""
import asyncio
import ctypes

import pytest
from oldman_server.native import engine as api
from test_http import config, wait_stat
from test_native_boundary import async_request


@pytest.mark.asyncio
async def test_queue_batches_preserve_requests_timeouts_and_owned_lifetime():
    # Catches dropped/duplicated partial batches, an unbounded count allocation,
    # lost arrivals during draining, and terminal reservations left undrained.
    cfg = config(application_routes=(("POST", "/handoff"),), max_requests=8,
                 decision_timeout_ms=1000, max_response_bytes=64)
    engine = await api.Engine.open(cfg)
    loop = asyncio.get_running_loop()
    huge_count = (1 << (8 * ctypes.sizeof(ctypes.c_size_t))) - 1
    jobs, retained, seen = [], [], set()

    def launch(tag):
        job = asyncio.create_task(async_request(
            cfg.server.http_port, f"/handoff?{tag}", b"request\x00\xff"))
        jobs.append(job)
        return job

    def reply_owned(events):
        ids = [event[0] for event in events]
        assert ids == sorted(ids)
        assert not seen.intersection(ids)
        seen.update(ids)
        retained.extend(req for _, _, req in events)
        engine._native.resolve_http_responses(
            [(rid, shard, 200, b"ok", ()) for rid, shard, _ in events])

    try:
        loop.remove_reader(engine.notification_fd)
        for i in range(6):
            launch(f"initial-{i}")
        await wait_stat(engine, "queued_decisions", lambda n: n == 6)
        assert engine._native.drain_requests(0) == []
        assert engine._native.drain_requests_owned(0) == []
        assert engine.server_stats()["queued_decisions"] == 6

        raw = engine._native.drain_requests(2)
        assert len(raw) == 2
        assert [e[0] for e in raw] == sorted(e[0] for e in raw)
        assert all(len(e) == 10 and e[3] == "application" and
                   e[5] == b"request\x00\xff" and e[8].startswith(b"/handoff?initial-")
                   and isinstance(e[9], tuple) for e in raw)
        assert engine.server_stats()["pending_application_nodes"] == 2
        seen.update(e[0] for e in raw)
        engine._native.resolve_http_responses(
            [(e[0], e[1], 200, b"ok", ()) for e in raw])
        await wait_stat(engine, "active_requests", lambda n: n == 4)
        assert engine.server_stats()["queued_decisions"] == 4

        # New arrivals join the still-pending batch before partial/whole drains.
        for i in range(4):
            launch(f"arrival-{i}")
        await wait_stat(engine, "queued_decisions", lambda n: n == 8)
        partial = engine._native.drain_requests_owned(3)
        assert len(partial) == 3
        assert engine.server_stats()["pending_application_nodes"] == 3
        reply_owned(partial)
        whole = engine._native.drain_requests_owned(huge_count)
        assert len(whole) == 5
        reply_owned(whole)
        assert engine._native.drain_requests_owned(huge_count) == []
        assert [(r[0], r[2]) for r in await asyncio.gather(*jobs)] == [(200, b"ok")] * 10
        assert len(seen) == 10
        await wait_stat(engine, "active_requests", lambda n: n == 0)

        # Keep accepting actual network requests while repeatedly taking batches.
        concurrent = []
        async def produce():
            for i in range(12):
                concurrent.append(launch(f"concurrent-{i}"))
                await asyncio.sleep(.001)
        producer = asyncio.create_task(produce())
        async with asyncio.timeout(5):
            while not producer.done() or any(not job.done() for job in concurrent):
                reply_owned(engine._native.drain_requests_owned(3))
                await asyncio.sleep(.001)
        await producer
        replies = await asyncio.gather(*concurrent)
        assert all(status in (200, 503) for status, _, _ in replies)
        accepted = sum(status == 200 for status, _, _ in replies)
        assert accepted > 0 and len(seen) == 10 + accepted
        assert all(body == b"ok" for status, _, body in replies if status == 200)
        await wait_stat(engine, "active_requests", lambda n: n == 0)

        # One request is retained in Python; the other expires while still queued.
        expired = [launch("retained-timeout"), launch("undrained-timeout")]
        await wait_stat(engine, "queued_decisions", lambda n: n == 2)
        stale_id, stale_shard, stale_request = engine._native.drain_requests_owned(1)[0]
        retained.append(stale_request)
        assert engine.server_stats()["pending_application_nodes"] == 1
        assert [r[0] for r in await asyncio.gather(*expired)] == [504, 504]
        await wait_stat(engine, "pending_decisions", lambda n: n == 0)
        assert engine.server_stats()["pending_application_nodes"] == 0
        assert engine._native.drain_requests(0) == []
        terminal = engine._native.drain_requests(huge_count)
        assert len(terminal) == 2
        assert all(len(e) == 8 and e[3] == "application_done" and e[4] == 504 for e in terminal)
        assert stale_id in {e[7] for e in terminal}
        engine._native.resolve_request(stale_id, stale_shard, 200, b"late", True)
        await wait_stat(engine, "active_requests", lambda n: n == 0)
        assert engine._native.drain_requests_owned(huge_count) == []

        launch("shutdown")
        await wait_stat(engine, "queued_decisions", lambda n: n == 1)
        retained.append(engine._native.drain_requests_owned(huge_count)[0][2])
    finally:
        loop.add_reader(engine.notification_fd, engine._drain)
        await engine.aclose()
        await asyncio.gather(*jobs, return_exceptions=True)
    assert engine.server_stats()["active_requests"] == 0
    assert engine.server_stats()["reserved_bytes"] == 0
    assert engine.server_stats()["pending_application_nodes"] == 0
    del engine
    assert all(req.method == "POST" and req.path == "/handoff" and
               req.target == b"/handoff?" + req.query_string.encode("latin-1") and
               req.body == b"request\x00\xff" and isinstance(req.headers, tuple)
               for req in retained)
