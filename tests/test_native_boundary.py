"""Regression tests for owned request metadata and per-response native validation.

Each runtime lifecycle test must run in a fresh process, as with the other
native runtime tests.
"""
import asyncio

import aiohttp
import pytest
from oldman_server.native import _native as native
from oldman_server.native import engine as api
from test_http import config, hold_requests, poll_shard, take_requests, wait_stat


async def async_request(port, path, body=None):
    # All requests must reach the server before its manually held batch is
    # released. The shared blocking HTTP helper's default thread pool is smaller
    # than this batch on the test host, so use real async clients here.
    async with aiohttp.ClientSession() as session:
        async with session.request("POST" if body is not None else "GET",
                                   f"http://127.0.0.1:{port}{path}", data=body) as reply:
            headers = {k.decode("ascii").lower(): v.decode("latin-1") for k, v in reply.raw_headers}
            return reply.status, headers, await reply.read()


def test_owned_request_public_fields_and_latin1():
    req = native.ApplicationRequest(method="POST", target=b"/caf\xe9?a=\xff?tail",
                                    headers=((b"X-Test", b"caf\xe9"),), body=b"binary\x00\xff")
    assert req.method == "POST"
    assert req.target == b"/caf\xe9?a=\xff?tail"
    assert req.path == "/caf\xe9"
    assert req.query_string == "a=\xff?tail"
    assert req.headers == ((b"X-Test", b"caf\xe9"),)
    assert req.body == b"binary\x00\xff"
    assert req.header("x-TEST") == "caf\xe9"
    sentinel = object()
    assert req.header("missing", sentinel) is sentinel
    with pytest.raises(UnicodeEncodeError):
        req.header("\u2603")
    for name in ("method", "target", "headers", "body", "path", "query_string"):
        with pytest.raises(AttributeError):
            setattr(req, name, None)


@pytest.mark.asyncio
async def test_native_checked_batch_isolates_bad_replies_and_owns_request():
    # Assert the new entrypoints before starting the one-shot runtime (red phase).
    assert hasattr(native.Runtime, "drain_requests_owned")
    assert hasattr(native.Runtime, "resolve_http_responses_checked")
    class Status(int):
        pass
    invalid = [
        (True, b"", ()), (Status(200), b"", ()), (1 << 100, b"", ()),
        (204, b"", ()), (200, "text", ()), (200, b"x" * 65, ()),
        (200, b"", []), (200, b"", (("X-Test", "\u2603"),)),
        (200, b"", (("X-Test", "line\r\ninjected"),)),
        (200, b"", (("Content-Length", "0"),)),
        (200, b"", (("X-Probe-Pid", "0"),)),
        (200, b"", (("X-Test", "x" * 8192),)),
        (200, b"", tuple((f"X-{i}", "v") for i in range(129))),
        (200, b"", (("one", "two", "three"),)),
    ]
    replies = [(201, b"valid-first", (("X-Latin", "caf\xe9"),)), *invalid,
               (202, b"valid-last", ())]
    cfg = config(application_dispatch_all=True, decision_timeout_ms=3000)
    # Queued, not called directly by the shard (the same-thread mode's default):
    # the batch API is what is tested.
    engine = await api.Engine.open(cfg, application_handler=lambda req: (200, b"", ()),
                                   application_inline=False)
    jobs = []
    retained = []
    try:
        hold_requests(engine)
        jobs = [asyncio.create_task(async_request(cfg.server.http_port, f"/owned?{i}", b"request\x00\xff"))
                for i in range(len(replies))]
        await wait_stat(engine, "queued_decisions", lambda n: n == len(replies))
        events = engine._native.drain_requests_owned(64)
        assert len(events) == len(replies)
        assert all(len(e) == 3 and isinstance(e[2], native.ApplicationRequest) for e in events)
        retained = [e[2] for e in events]
        assert all(r.path == "/owned" and r.method == "POST" for r in retained)
        batch = [(rid, shard, *replies[int(req.query_string)]) for rid, shard, req in events]
        with pytest.raises((TypeError, ValueError)):
            engine._native.resolve_http_responses_checked([batch[0], (1, 0, 200, b"")], 64)
        assert engine.server_stats()["decision_return_batches"] == 0
        assert engine.server_stats()["pending_decisions"] == len(replies)
        bad_count = engine._native.resolve_http_responses_checked(batch, 64)
        poll_shard(engine)
        assert bad_count == len(invalid)
        result = await asyncio.gather(*jobs)
        assert result[0][0] == 201 and result[0][2] == b"valid-first"
        assert result[0][1]["x-latin"] == "caf\xe9"
        assert result[-1][0] == 202 and result[-1][2] == b"valid-last"
        assert [(status, body) for status, _headers, body in result[1:-1]] == [(500, b"")] * len(invalid)
        await wait_stat(engine, "active_requests", lambda n: n == 0)
        # The connections' close events are consumed by the drain, not returned.
        await wait_stat(engine, "connections_open", lambda n: n == 0)
        assert engine._native.drain_requests_owned(64) == []
        # Terminal events keep their tuple shape on the owned drain API: an
        # invalid stream start is answered 500 and ends with application_done.
        pending_job = asyncio.create_task(async_request(cfg.server.http_port, "/owned?terminal", b"x"))
        jobs.append(pending_job)
        await wait_stat(engine, "queued_decisions", lambda n: n == 1)
        rid, shard, _req = engine._native.drain_requests_owned(64)[0]
        assert engine._native.resolve_http_responses_checked([(rid, shard, 99, b"", (), 1)], 64) == 1
        poll_shard(engine)
        assert (await pending_job)[0] == 500
        terminal = engine._native.drain_requests_owned(64)
        assert len(terminal) == 1 and len(terminal[0]) == 8
        assert terminal[0][3] == "application_done" and terminal[0][7] == rid
    finally:
        take_requests(engine)
        await engine.aclose()
        await asyncio.gather(*jobs, return_exceptions=True)
    assert engine.server_stats()["reserved_bytes"] == 0
    assert sorted(int(r.query_string) for r in retained) == list(range(len(replies)))
    assert all(r.target.startswith(b"/owned?") and isinstance(r.headers, tuple) for r in retained)
    assert all(r.body == b"request\x00\xff" for r in retained)


@pytest.mark.asyncio
async def test_bytes_checked_batch_keeps_owned_replies_and_rejects_invalid_fields():
    replies = [(201, b"first", ((b"X-Value", b"caf\xc3\xa9\xff"),)),
               (200, b"bad", (("X-Value", "wrong mode"),)),
               (200, b"bad", ((b"X-Bad", b"a\x01b"),)),
               (202, b"last", ())]
    async def handler(req):
        return replies[int(req.query_string)]
    cfg = config(application_dispatch_all=True, decision_timeout_ms=3000)
    engine = await api.Engine.open(cfg, application_handler=handler, application_bytes_headers=True)
    jobs = []
    flush = engine._flush_application
    engine._flush_application = lambda: None
    try:
        jobs = [asyncio.create_task(async_request(cfg.server.http_port, f"/bytes?{i}"))
                for i in range(len(replies))]
        await wait_stat(engine, "python_application_finished", lambda n: n == len(replies))
        batch = engine._application_returns
        assert len(batch) == 4 and engine.server_stats()["active_requests"] == 4
        with pytest.raises((TypeError, ValueError)):
            engine._native.resolve_http_responses_checked(
                [batch[0], (1, 0, 200, b"")], 64, bytes_headers=True)
        assert engine.server_stats()["decision_return_batches"] == 0
        # Replacing the caller's results cannot change the queued immutable data.
        replies[:] = [(500, b"mutated", ())] * 4
        engine._flush_application = flush
        flush()
        result = await asyncio.gather(*jobs)
        assert [(r[0], r[2]) for r in result] == [(201, b"first"), (500, b""), (500, b""), (202, b"last")]
        assert result[0][1]["x-value"].encode("latin-1") == b"caf\xc3\xa9\xff"
        assert engine.server_stats()["python_application_errors"] == 2
        await wait_stat(engine, "active_requests", lambda n: n == 0)
    finally:
        engine._flush_application = flush
        await engine.aclose()
        await asyncio.gather(*jobs, return_exceptions=True)
    assert engine.server_stats()["reserved_bytes"] == 0
