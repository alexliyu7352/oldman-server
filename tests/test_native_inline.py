"""The same-thread mode's direct call: the shard calls a plain function handler
itself for a complete request, without the queue (Engine.open's
application_inline). One native runtime per test process."""
import asyncio
import contextvars
import http.client
import os

import pytest
from oldman_server.native import engine as api
from test_http import free_port, request, wait_stat


def config(**kwargs):
    server = api.ServerConfig(http_port=free_port(), application_dispatch_all=True, **kwargs)
    return api.Config(shard_cpus=(sorted(os.sched_getaffinity(0))[-1],), server=server, same_thread=True)


async def serve(handler, scenario, **options):
    cfg = config()
    engine = await api.Engine.open(cfg, application_handler=handler, **options)
    try:
        return await scenario(engine, cfg.server.http_port)
    finally:
        await engine.aclose()


@pytest.mark.asyncio
async def test_a_plain_handler_answers_without_the_queue():
    seen = []

    def handler(req):
        seen.append((req.method, req.path, req.body))
        return 201, b"direct:" + req.body, (("X-Way", "inline"),)

    async def scenario(engine, port):
        status, headers, body = await request(port, "/x", b"payload")
        assert (status, headers["x-way"], body) == (201, "inline", b"direct:payload")
        stats = engine.server_stats()
        # Answered in the call: never queued for a drain, nothing pending.
        assert stats["queued_decisions"] == 0 and stats["pending_decisions"] == 0
        assert stats["python_application_finished"] == 1
        assert engine._application_tasks == {}
    await serve(handler, scenario)
    assert seen == [("POST", "/x", b"payload")]


VALUE = contextvars.ContextVar("VALUE", default="unset")


@pytest.mark.asyncio
async def test_each_call_runs_in_a_copy_of_the_context():
    seen = []

    def handler(req):
        # As a Task would: what one call sets stays in its own copy.
        seen.append(VALUE.get())
        VALUE.set(req.path)
        return 200, b"", ()

    async def scenario(engine, port):
        for path in ("/a", "/b"):
            assert (await request(port, path))[0] == 200
    await serve(handler, scenario)
    assert seen == ["unset", "unset"] and VALUE.get() == "unset"


@pytest.mark.asyncio
async def test_an_answer_follows_what_the_handler_streamed():
    engines = []

    def handler(req):
        engine = engines[0]
        # Its first piece queues without waiting: a plain function can start a
        # stream in the call, then write and end it.
        with pytest.raises(StopIteration):
            engine.stream_send(req.request_id, req.shard, api.STREAM_START, b"one ", 200,
                               (("Content-Type", "text/plain"),)).send(None)
        engine.stream_write(req.request_id, req.shard, b"two")
        engine.stream_end(req.request_id, req.shard)
        # An answer as well: it goes after the pieces, so the stream is the response.
        return 200, b"not this", ()

    async def scenario(engine, port):
        engines.append(engine)
        status, _, body = await request(port, "/s")
        assert (status, body) == (200, b"one two")
        assert (await request(port, "/s"))[2] == b"one two"
    await serve(handler, scenario)


@pytest.mark.asyncio
async def test_a_closing_engine_answers_503():
    called = []

    def handler(req):
        called.append(req.path)
        return 200, b"", ()

    async def scenario(engine, port):
        engine._closed = True  # as aclose sets it while the server still runs
        try:
            assert (await request(port, "/"))[0] == 503
        finally:
            engine._closed = False
    await serve(handler, scenario)
    assert called == []


@pytest.mark.asyncio
async def test_an_awaitable_answer_comes_later():
    async def later(req):
        await asyncio.sleep(0.01)
        return 200, b"later " + req.path.encode(), ()

    async def scenario(engine, port):
        status, _, body = await request(port, "/slow")
        assert (status, body) == (200, b"later /slow")
        await wait_stat(engine, "pending_decisions", lambda n: n == 0)
    await serve(lambda req: later(req), scenario)


@pytest.mark.asyncio
async def test_errors_and_invalid_answers_are_500():
    def handler(req):
        if req.path == "/raise":
            raise RuntimeError("boom")
        if req.path == "/timeout":
            raise TimeoutError
        if req.path == "/shape":
            return 200, b"x"
        return 200, b"", (("Bad Header", "x"),)

    async def scenario(engine, port):
        assert (await request(port, "/raise"))[0] == 500
        assert (await request(port, "/timeout"))[0] == 504
        assert (await request(port, "/shape"))[0] == 500
        assert (await request(port, "/header"))[0] == 500
        # The server goes on serving.
        assert (await request(port, "/raise"))[0] == 500
    await serve(handler, scenario)


@pytest.mark.asyncio
async def test_a_head_answer_keeps_its_content_length():
    def handler(req):
        return 200, b"", (("Content-Type", "text/plain"),), 0, 12

    async def scenario(engine, port):
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        try:
            await asyncio.to_thread(connection.request, "HEAD", "/")
            reply = await asyncio.to_thread(connection.getresponse)
            assert reply.status == 200 and reply.getheader("Content-Length") == "12"
        finally:
            connection.close()
    await serve(handler, scenario)


@pytest.mark.asyncio
async def test_connections_are_registered_and_closed():
    connections = {}
    lost = []

    class State:
        def connection_lost(self):
            lost.append(True)

    def handler(req):
        # As the drain does: the connection is known from its first request.
        assert connections.get(req.connection, "missing") is None
        connections[req.connection] = State()
        return 200, b"ok", ()

    async def scenario(engine, port):
        assert (await request(port, "/"))[0] == 200
        async with asyncio.timeout(5):
            while connections or not lost:
                await asyncio.sleep(0.01)
    await serve(handler, scenario, application_connections=connections)
    assert lost == [True]


@pytest.mark.asyncio
async def test_without_inline_the_queue_serves():
    async def scenario(engine, port):
        assert (await request(port, "/"))[0] == 200
        assert engine.server_stats()["python_application_finished"] == 1
    await serve(lambda req: (200, b"queued", ()), scenario, application_inline=False)


@pytest.mark.asyncio
async def test_a_coroutine_handler_goes_through_the_queue():
    async def handler(req):
        return 200, b"queued " + req.path.encode(), ()

    async def scenario(engine, port):
        status, _, body = await request(port, "/c")
        assert (status, body) == (200, b"queued /c")
        # Not called by the shard: its batches keep the Python work together.
        assert engine._inline is None
    await serve(handler, scenario)


@pytest.mark.asyncio
async def test_a_coroutine_handler_that_suspends_answers_through_its_task():
    started = []

    async def handler(req):
        started.append(asyncio.current_task())
        await asyncio.sleep(0.01)
        if req.path == "/raise":
            raise RuntimeError("late")
        return 200, b"after " + req.path.encode(), ()

    async def scenario(engine, port):
        assert (await request(port, "/wait"))[::2] == (200, b"after /wait")
        assert (await request(port, "/raise"))[0] == 500
        # Each ran in a Task of its own, as a queued request's handler does.
        assert len(started) == 2 and all(started) and started[0] is not started[1]
        await wait_stat(engine, "pending_decisions", lambda n: n == 0)
    await serve(handler, scenario)


@pytest.mark.asyncio
async def test_a_plain_handler_returning_a_coroutine_that_finishes_at_once():
    async def answer(req):
        return 200, b"wrapped " + req.path.encode(), ()

    async def scenario(engine, port):
        assert (await request(port, "/w"))[::2] == (200, b"wrapped /w")
        assert engine.server_stats()["pending_decisions"] == 0
    # As a wrapper around a coroutine function: the Task finishes in the call.
    await serve(lambda req: answer(req), scenario)
