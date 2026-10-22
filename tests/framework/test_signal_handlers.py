import asyncio

from queue import Queue
from typing import Optional
from unittest.mock import MagicMock

import pytest

from oldman_server.testing.testing import HOST, PORT

from oldman_server import OldmanServer
from oldman_server.exceptions import BadRequest, ServerError
from oldman_server.response import HTTPResponse
from oldman_server.signals import Event


pytestmark = pytest.mark.xdist_group(name="process_spawning")


async def stop(app, loop):
    await asyncio.sleep(0.1)
    app.stop()


calledq = Queue()


def set_loop(app, loop):
    global mock
    mock = MagicMock()
    loop.add_signal_handler = mock


def after(app, loop):
    print("...")
    calledq.put(mock.called)


def test_register_system_signals(app):
    """Test if sanic register system signals"""

    @app.route("/hello")
    async def hello_route(request):
        return HTTPResponse()

    app.listener("after_server_start")(stop)
    app.listener("before_server_start")(set_loop)
    app.listener("after_server_stop")(after)

    app.run(HOST, PORT, single_process=True)
    assert calledq.get() is True


def test_no_register_system_signals_fails(app):
    """Test if sanic don't register system signals"""

    @app.route("/hello")
    async def hello_route(request):
        return HTTPResponse()

    app.listener("after_server_start")(stop)
    app.listener("before_server_start")(set_loop)
    app.listener("after_server_stop")(after)

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

    app.listener("after_server_start")(stop)
    app.listener("before_server_start")(set_loop)
    app.listener("after_server_stop")(after)

    app.run(HOST, PORT, register_sys_signals=False, single_process=True)
    assert calledq.get() is False


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
