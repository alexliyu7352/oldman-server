"""Server listeners come in pairs: after_server_stop releases what
before_server_start set up, before_server_stop what after_server_start
started. A failing listener does not skip its pair's release, nor a later
stop phase; the failure still ends the run."""
import socket

import pytest

from oldman_server import OldmanServer
from oldman_server.response import text


class Failure(Exception):
    pass


def lifecycle(app: OldmanServer, failing: str):
    calls = []

    def listener(event):
        async def run(app):
            calls.append(event)
            if event == failing:
                raise Failure(event)
            if event == "after_server_start":
                app.stop()

        return run

    for event in ("before_server_start", "after_server_start",
                  "before_server_stop", "after_server_stop"):
        app.register_listener(listener(event), event)

    @app.get("/")
    async def index(request):
        return text("ok")

    return calls


def serving(port):
    with socket.socket() as sock:
        return sock.connect_ex(("127.0.0.1", port)) == 0


def test_before_server_start_failure_still_runs_after_server_stop(app, port):
    calls = lifecycle(app, "before_server_start")
    with pytest.raises(Failure):
        app.run(single_process=True, port=port, access_log=False)
    assert calls == ["before_server_start", "after_server_stop"]


def test_after_server_start_failure_stops_the_server(app, port):
    calls = lifecycle(app, "after_server_start")
    with pytest.raises(Failure):
        app.run(single_process=True, port=port, access_log=False)
    assert calls == ["before_server_start", "after_server_start",
                     "before_server_stop", "after_server_stop"]
    assert not serving(port)


def test_before_server_stop_failure_still_runs_after_server_stop(app, port):
    calls = lifecycle(app, "before_server_stop")
    with pytest.raises(Failure):
        app.run(single_process=True, port=port, access_log=False)
    assert calls == ["before_server_start", "after_server_start",
                     "before_server_stop", "after_server_stop"]
    assert not serving(port)


def test_a_failing_shutdown_listener_runs_once(app, port):
    # A RuntimeError from a listener is its own failure, not the loop
    # stopping early: the stop phase is not started a second time.
    calls = []

    @app.after_server_start
    async def stop(app):
        app.stop()

    @app.before_server_stop
    async def fail(app):
        calls.append("before_server_stop")
        raise RuntimeError("listener failure")

    @app.get("/")
    async def index(request):
        return text("ok")

    with pytest.raises(RuntimeError, match="listener failure"):
        app.run(single_process=True, port=port, access_log=False)
    assert calls == ["before_server_stop"]
