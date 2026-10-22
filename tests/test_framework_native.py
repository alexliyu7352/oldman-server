"""Native application API integration: run this lifecycle in its own process."""
import asyncio

import pytest

from oldman_server.native import engine as api
from test_http import config, request, wait_stat


@pytest.mark.asyncio
async def test_http_response_binding_validates_entire_batch_before_submission():
    cfg = config(application_routes=(("GET", "/raw"),), decision_timeout_ms=3000)
    engine = await api.Engine.open(cfg)
    loop = asyncio.get_running_loop()
    jobs = []
    try:
        loop.remove_reader(engine.notification_fd)
        jobs = [asyncio.create_task(request(cfg.server.http_port, f"/raw?n={i}")) for i in range(2)]
        await wait_stat(engine, "queued_decisions", lambda n: n == 2)
        events = engine._native.drain_requests(16)
        assert len(events) == 2
        assert all(len(event) == 10 and event[3] == "application" for event in events)
        assert {event[8] for event in events} == {b"/raw?n=0", b"/raw?n=1"}
        assert all(event[5] == b"" and isinstance(event[9], tuple) for event in events)
        assert all(isinstance(name, bytes) and isinstance(value, bytes)
                   for event in events for name, value in event[9])
        first, second = events
        valid = (first[0], first[1], 200, b"must-not-be-submitted", ())
        invalid = [
            (199, b"bad", ()),
            (600, b"bad", ()),
            (204, b"bad", ()),
            (304, b"bad", ()),
            (200, b"bad", (("Bad Name", "value"),)),
            (200, b"bad", (("X-Test", "a\r\nb"),)),
            (200, b"bad", (("X-Test", "bad\x00value"),)),
            (200, b"bad", (("X-Test", "x" * 8192),)),
            (200, b"bad", tuple((f"X-{i}", "v") for i in range(129))),
        ]
        invalid.extend((200, b"bad", ((name, "override"),)) for name in (
            "Content-Length", "Transfer-Encoding", "Connection", "Date", "Server", "X-Probe-Pid"))
        for status, body, headers in invalid:
            with pytest.raises(ValueError):
                engine._native.resolve_http_responses([
                    valid, (second[0], second[1], status, body, headers)])
            assert engine.server_stats()["decision_return_batches"] == 0
            assert engine.server_stats()["pending_decisions"] == 2
        with pytest.raises(TypeError):
            engine._native.resolve_http_responses([(second[0], second[1], 200, "text", ())])
        with pytest.raises((UnicodeError, ValueError)):
            engine._native.resolve_http_responses([(second[0], second[1], 200, b"bad", (("X-Test", "\u2603"),))])
        assert engine.server_stats()["decision_return_batches"] == 0

        engine._native.resolve_http_responses([
            (event[0], event[1], 201, b"owned\x00\xff", (("X-Latin", "caf\xe9"),)) for event in events])
        replies = await asyncio.gather(*jobs)
        assert [(status, headers["x-latin"], body) for status, headers, body in replies] == [
            (201, "caf\xe9", b"owned\x00\xff"), (201, "caf\xe9", b"owned\x00\xff")]
        await wait_stat(engine, "active_requests", lambda n: n == 0)
        assert engine._native.drain_requests(16) == []  # retire=True: no normal DONE

        # The existing shutdown decision remains meaningful to generic handlers.
        stopped = asyncio.create_task(request(cfg.server.http_port, "/raw"))
        jobs.append(stopped)
        await wait_stat(engine, "queued_decisions", lambda n: n == 1)
        pending = engine._native.drain_requests(16)[0]
        engine._native.resolve_request(pending[0], pending[1], 3, b"")
        assert (await stopped)[0] == 503
        terminal = engine._native.drain_requests(16)
        assert len(terminal) == 1 and len(terminal[0]) == 8
        assert terminal[0][3] == "application_done"
        assert terminal[0][4] == 503 and terminal[0][7] == pending[0]
        await wait_stat(engine, "active_requests", lambda n: n == 0)
    finally:
        loop.add_reader(engine.notification_fd, engine._drain)
        await engine.aclose()
        await asyncio.gather(*jobs, return_exceptions=True)
    assert engine.server_stats()["reserved_bytes"] == 0
