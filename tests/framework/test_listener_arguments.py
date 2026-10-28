"""Listeners, tasks and main-process events get their arguments by their
signature: a TypeError raised inside one is its own error, not a reason to
call it again with other arguments."""
import pytest

from oldman_server import OldmanServer
from oldman_server.helpers import accepts
from oldman_server.response import text
from oldman_server.server.events import trigger_events


def test_accepts_reads_the_signature():
    def one(app): ...

    def two(app, loop): ...

    def anything(*args, **kwargs): ...

    assert accepts(one, 1) and not accepts(one, 1, 2)
    assert accepts(two, 1, 2) and not accepts(two, 1)
    assert accepts(anything) and accepts(anything, 1, 2, x=3)
    assert accepts(len, [])  # a builtin: the call decides


def test_a_listener_raising_type_error_runs_once(app: OldmanServer, recwarn):
    calls = []

    @app.after_server_start
    def broken(app):
        calls.append(app)
        raise TypeError("the listener's own")

    @app.get("/")
    async def index(request):
        return text("ok")

    with pytest.raises(TypeError, match="the listener's own"):
        app.run(single_process=True, port=0)
    assert len(calls) == 1
    assert not [w for w in recwarn if "loop argument" in str(w.message)]


async def test_an_old_style_listener_still_gets_the_loop(app: OldmanServer):
    received = []

    def old_style(app, loop):
        received.append(loop)

    with pytest.warns(DeprecationWarning, match="loop argument"):
        await OldmanServer._listener(app, "the loop", old_style)
    assert received == ["the loop"]


async def test_a_task_raising_type_error_runs_once(app: OldmanServer):
    calls = []

    def task(app):
        calls.append(app)
        raise TypeError("the task's own")

    with pytest.raises(TypeError, match="the task's own"):
        await OldmanServer._prep_task(task, app, None)
    assert calls == [app]

    def no_argument():
        calls.append("none")

    await OldmanServer._prep_task(no_argument, app, None)
    assert calls[-1] == "none"


def test_a_main_process_event_raising_type_error_runs_once(app: OldmanServer):
    import asyncio

    calls = []

    def event(app):
        calls.append(app)
        raise TypeError("the event's own")

    loop = asyncio.new_event_loop()
    try:
        with pytest.raises(TypeError, match="the event's own"):
            trigger_events([event], loop, app)
    finally:
        loop.close()
    assert calls == [app]
