import asyncio
import os
import signal

from queue import Queue
from types import SimpleNamespace
from typing import Optional

import pytest

from oldman_server.testing.testing import HOST, PORT

from oldman_server import OldmanServer
from oldman_server.compat import ctrlc_workaround_for_windows
from oldman_server.exceptions import BadRequest, ServerError
from oldman_server.response import HTTPResponse
from oldman_server.signals import Event


pytestmark = pytest.mark.xdist_group(name="process_spawning")


async def stop(app, loop):
    await asyncio.sleep(0.1)
    app.stop()


calledq = Queue()


def registered(app, loop):
    # A Python-level handler: it also sees a signal while the loop is not
    # running, which uvloop's own (add_signal_handler) would drop.
    handler = signal.getsignal(signal.SIGTERM)
    calledq.put(handler not in (signal.SIG_IGN, signal.SIG_DFL, None)
                and handler is signal.getsignal(signal.SIGINT))


@pytest.mark.skipif(os.name == "nt", reason="Windows: the Ctrl+C workaround, below")
def test_register_system_signals(app):
    """Test if sanic register system signals"""

    @app.route("/hello")
    async def hello_route(request):
        return HTTPResponse()

    app.listener("after_server_start")(registered)
    app.listener("after_server_start")(stop)

    app.run(HOST, PORT, single_process=True)
    assert calledq.get() is True


def test_no_register_system_signals_fails(app):
    """Test if sanic don't register system signals"""

    @app.route("/hello")
    async def hello_route(request):
        return HTTPResponse()

    app.listener("after_server_start")(registered)
    app.listener("after_server_start")(stop)

    message = (
        r"Cannot run OldmanServer\.serve with register_sys_signals=False\. Use "
        r"OldmanServer.serve_single\."
    )
    with pytest.raises(RuntimeError, match=message):
        app.prepare(HOST, PORT, register_sys_signals=False)
    assert calledq.empty()


def test_dont_register_system_signals(app):
    """Test if sanic don't register system signals"""

    @app.route("/hello")
    async def hello_route(request):
        return HTTPResponse()

    app.listener("after_server_start")(registered)
    app.listener("after_server_start")(stop)

    app.run(HOST, PORT, register_sys_signals=False, single_process=True)
    assert calledq.get() is False


@pytest.mark.skipif(os.name == "nt", reason="windows cannot SIGINT processes")
def test_windows_workaround():
    """Test Windows workaround (on any other OS)"""

    # At least some code coverage, even though this test doesn't work on
    # Windows...
    class MockApp:
        def __init__(self):
            self.state = SimpleNamespace()
            self.state.is_stopping = False

        def stop(self):
            assert not self.state.is_stopping
            self.state.is_stopping = True

        def add_task(self, func):
            loop = asyncio.get_event_loop()
            self.stay_active_task = loop.create_task(func(self))

    async def atest(stop_first):
        app = MockApp()
        ctrlc_workaround_for_windows(app)
        await asyncio.sleep(0.05)
        if stop_first:
            app.stop()
            await asyncio.sleep(0.2)
        assert app.state.is_stopping == stop_first
        # First Ctrl+C: should call app.stop() within 0.1 seconds
        os.kill(os.getpid(), signal.SIGINT)
        await asyncio.sleep(0.2)
        assert app.state.is_stopping
        assert app.stay_active_task.result() is None
        # Second Ctrl+C should raise
        with pytest.raises(KeyboardInterrupt):
            os.kill(os.getpid(), signal.SIGINT)
        return "OK"

    # Run in our private loop; the workaround's SIGINT handler goes after.
    handler = signal.getsignal(signal.SIGINT)
    loop = asyncio.new_event_loop()
    try:
        assert loop.run_until_complete(atest(False)) == "OK"
        assert loop.run_until_complete(atest(True)) == "OK"
    finally:
        loop.close()
        signal.signal(signal.SIGINT, handler)


def test_signals_with_invalid_invocation(app):
    """Test if sanic register fails with invalid invocation"""

    @app.route("/hello")
    async def hello_route(request):
        return HTTPResponse()

    with pytest.raises(
        BadRequest, match="Invalid event registration: Missing event name"
    ):
        app.listener(stop)


def test_signal_server_lifecycle_exception(app: OldmanServer):
    trigger: Optional[Exception] = None

    @app.route("/hello")
    async def hello_route(request):
        return HTTPResponse()

    @app.signal(Event.SERVER_EXCEPTION_REPORT)
    async def test_signal(exception: Exception):
        nonlocal trigger
        trigger = exception

    @app.before_server_start
    async def test_before_server_start(app):
        raise ServerError("test_before_server_start")

    with pytest.raises(ServerError, match="test_before_server_start"):
        app.run(single_process=True)

    assert isinstance(trigger, ServerError)
    assert str(trigger) == "test_before_server_start"


def _custom_handler(signum, frame):  # pragma: no cover - never signalled
    pass


@pytest.fixture
def custom_sigterm():
    """A SIGTERM handler of the program's own, in place before a run."""
    previous = signal.signal(signal.SIGTERM, _custom_handler)
    yield
    signal.signal(signal.SIGTERM, previous)


@pytest.mark.skipif(os.name == "nt", reason="Windows: the Ctrl+C workaround")
@pytest.mark.parametrize("register_sys_signals", (True, False))
def test_a_run_hands_the_signal_handlers_back(app, custom_sigterm, register_sys_signals):
    """A run installs its own SIGINT and SIGTERM handling (and ignores both
    while it stops); when app.run() returns, the handlers in place before it
    are back, those of the program's own included."""

    @app.route("/hello")
    async def hello_route(request):
        return HTTPResponse()

    app.listener("after_server_start")(stop)
    before = (signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM))

    app.run(HOST, PORT, single_process=True, register_sys_signals=register_sys_signals)

    assert (signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)) == before
    assert before[1] is _custom_handler


@pytest.mark.skipif(os.name == "nt", reason="Windows: the Ctrl+C workaround")
def test_a_test_client_call_hands_the_signal_handlers_back(app, custom_sigterm):
    @app.route("/hello")
    async def hello_route(request):
        return HTTPResponse()

    before = (signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM))
    _, response = app.test_client.get("/hello")
    assert response.status == 200
    assert (signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)) == before


AFTER_A_RUN = """
import os
import signal
import sys

from oldman_server import OldmanServer
from oldman_server.response import text

app = OldmanServer("AfterARun")


@app.get("/")
async def index(request):
    return text("ok")


@app.after_server_start
async def stop(app):
    app.stop()


if __name__ == "__main__":
    workers = sys.argv[1]
    options = {{"single_process": True}} if workers == "single" else {{"workers": int(workers)}}
    app.run(port={port}, access_log=False, **options)
    print("RUN ENDED", flush=True)
    # The handler in place before the run is the default: SIGTERM ends the
    # process (a run that kept its own ignored it, or sent it to a manager
    # that no longer ran).
    os.kill(os.getpid(), signal.SIGTERM)
    print("STILL RUNNING", flush=True)
"""


@pytest.mark.skipif(os.name == "nt", reason="POSIX signals")
@pytest.mark.parametrize("workers", ("single", "1", "2"))
def test_a_sigterm_after_a_run_ends_the_process(tmp_path, port, workers):
    import subprocess
    import sys

    script = tmp_path / "after_a_run.py"
    script.write_text(AFTER_A_RUN.format(port=port))
    completed = subprocess.run(
        [sys.executable, str(script), workers],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert "RUN ENDED" in completed.stdout, completed.stdout + completed.stderr
    assert "STILL RUNNING" not in completed.stdout
    assert completed.returncode == -signal.SIGTERM, completed.stdout + completed.stderr
