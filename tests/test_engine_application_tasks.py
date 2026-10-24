"""Engine bookkeeping for application Tasks, including Tasks cancelled before
their first step (their coroutine body, and so its finally, never runs)."""
import asyncio
import os
from types import SimpleNamespace

import uvloop

from oldman_server.native import _native as _seastar_probe
from oldman_server.native import engine as probe_engine
from oldman_server.native.engine import ApplicationRequest, Config, Engine, ServerConfig


class FakeRuntime:
    """Delivers queued events on demand and records checked responses."""

    def __init__(self):
        self.fd_ = os.eventfd(0, os.EFD_NONBLOCK | os.EFD_CLOEXEC)
        self.state, self.events, self.responses = 1, [], []
        os.eventfd_write(self.fd_, 1)  # Engine startup waits for state 1.

    def fd(self): return self.fd_
    def clear_notification(self):
        try:
            os.eventfd_read(self.fd_)
        except BlockingIOError:
            pass
    def drain_requests_owned(self, count, connections=None):
        events, self.events = self.events, []
        return events
    def resolve_http_responses_checked(self, batch, limit, bytes_headers=False, no_body_statuses=False):
        self.responses.extend((rid, status) for rid, _, status, _, _ in batch)
        return 0
    def stats(self): return {"state": self.state}
    def server_stats(self): return {}
    def stop(self):
        self.state = 3
        os.eventfd_write(self.fd_, 1)
    def join(self): pass
    def close_fd(self): os.close(self.fd_)


def request():
    return ApplicationRequest("GET", b"/", (), b"", http_version="1.1",
                              client=("127.0.0.1", 1), server=("127.0.0.1", 2))


def run(scenario, handler=None):
    async def main():
        runtime = FakeRuntime()
        binding = {name: getattr(_seastar_probe, name) for name in dir(_seastar_probe)}
        binding["Runtime"] = lambda *unused: runtime
        original, probe_engine._native = probe_engine._native, SimpleNamespace(**binding)
        started = []

        async def default(req):
            started.append(req)
            await asyncio.sleep(0)
            return 200, b"ok", ()
        try:
            config = Config(shard_cpus=(sorted(os.sched_getaffinity(0))[0],),
                            server=ServerConfig(http_port=1, application_dispatch_all=True))
            engine = await Engine.open(config, application_handler=handler or default)
            try:
                return await scenario(engine, runtime, started)
            finally:
                await engine.aclose()
        finally:
            probe_engine._native = original
    return uvloop.run(main())


def done_event(event_id, shard, parent, status=504):
    return (event_id, shard, "GET", "application_done", status, None, "", parent)


def test_completed_tasks_clean_up_without_done_callbacks():
    async def scenario(engine, runtime, started):
        engine._application_event((1 << 16, 0, request()))
        assert engine._application_tasks
        for _ in range(5):
            await asyncio.sleep(0)
        assert not engine._application_tasks and engine._application_terminal == set()
        assert runtime.responses == [(1 << 16, 200)]
        stats = engine._decision_stats
        assert stats["python_application_started"] == stats["python_application_finished"] == 1
    run(scenario)


def test_terminal_event_before_first_step_reaps_without_a_response():
    async def scenario(engine, runtime, started):
        engine._application_event((1 << 16, 0, request()))
        engine._application_event(done_event(2 << 16, 0, 1 << 16))
        assert not engine._application_tasks and engine._application_terminal == set()
        for _ in range(5):
            await asyncio.sleep(0)
        assert started == [] and runtime.responses == []  # native already answered
        stats = engine._decision_stats
        assert stats["python_application_finished"] == 1 and stats["python_application_errors"] == 0
    run(scenario)


def test_terminal_event_while_running_cancels_through_finally():
    async def scenario(engine, runtime, started):
        engine._application_event((1 << 16, 0, request()))
        await asyncio.sleep(0)  # the handler started and is suspended
        assert len(started) == 1
        engine._application_event(done_event(2 << 16, 0, 1 << 16))
        for _ in range(5):
            await asyncio.sleep(0)
        assert not engine._application_tasks and engine._application_terminal == set()
        assert runtime.responses == []
        assert engine._decision_stats["python_application_finished"] == 1
    run(scenario)


def test_unstarted_task_cancelled_by_others_is_answered_at_close():
    async def scenario(engine, runtime, started):
        engine._application_event((1 << 16, 0, request()))
        task, _ = engine._application_tasks[1 << 16]
        task.cancel()  # not the Engine's own cancellation
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert task.cancelled() and started == []
        engine._cancel_applications()
        engine._reap_applications()
        engine._flush_application()
        assert runtime.responses == [(1 << 16, 503)] and not engine._application_tasks
        assert engine._decision_stats["python_application_errors"] == 1
    run(scenario)


def test_a_handler_that_ignores_its_cancellation_does_not_hold_up_close():
    # aclose waited for every cancelled handler: one that catches its
    # CancelledError and carries on kept the server from ever stopping. It is
    # left running, as Sanic leaves it, and its late answer is dropped.
    async def stubborn(req):
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            await asyncio.sleep(30)  # the event loop's own end cancels this
        return 200, b"late", ()

    async def scenario(engine, runtime, started):
        engine._application_event((1 << 16, 0, request()))
        await asyncio.sleep(0)
        loop = asyncio.get_running_loop()
        began = loop.time()
        await asyncio.wait_for(engine.aclose(), 10)
        assert loop.time() - began < probe_engine._CANCELLED_WAIT + 1
        assert not engine._application_tasks and runtime.responses == []
    run(scenario, handler=stubborn)


def test_a_request_answered_at_close_leaves_no_body_buffer():
    # A request whose body was still arriving got a 503 once the Engine was
    # closing, but its buffer stayed: the rest of its body piled up there.
    async def scenario(engine, runtime, started):
        engine._closed = True
        engine._application_event((1 << 16, 0, request(), "more"))
        assert engine._application_inbound == {}
        engine._flush_application()
        assert runtime.responses == [(1 << 16, 503)]
    run(scenario)


def test_a_failing_connection_lost_at_close_still_releases_the_others():
    class Failing:
        def __init__(self, calls):
            self.calls = calls

        def connection_lost(self):
            self.calls.append(self)
            raise RuntimeError("connection_lost failed")

    async def scenario(engine, runtime, started):
        calls = []
        engine.application_connections.update({1: Failing(calls), 2: Failing(calls), 3: None})
        engine._release_connections()
        assert len(calls) == 2 and engine.application_connections == {}
    run(scenario)
