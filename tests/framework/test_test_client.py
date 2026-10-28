"""The test clients: ports, redirects, warnings, and what a call leaves on
the app it ran."""
import subprocess
import sys
import warnings

import pytest

from oldman_server import OldmanServer
from oldman_server.application.state import Mode
from oldman_server.response import redirect, text
from oldman_server.testing.reusable import ReusableClient
from oldman_server.testing.testing import TestClient


def test_calls_after_the_server_closed_the_connection(app: OldmanServer):
    # The server closes first (no keep-alive): its end of the connection
    # stays in TIME_WAIT on the port. The next call binds a port of its own.
    app.config.KEEP_ALIVE = False

    @app.get("/")
    async def index(request):
        return text("ok")

    ports = set()
    for _ in range(3):
        _, response = app.test_client.get("/")
        assert response.text == "ok"
        assert response.headers["connection"] == "close"
        ports.add(app.test_client.port)
    assert all(port > 0 for port in ports)


def test_a_call_after_a_websocket_the_server_closed(app: OldmanServer):
    @app.websocket("/ws")
    async def greet(request, ws):
        await ws.send("hi")

    @app.get("/")
    async def index(request):
        return text("ok")

    async def client(ws):
        await ws.recv()
        await ws.recv()  # the server closes: ConnectionClosedOK

    _, proxy = app.test_client.websocket("/ws", mimic=client)
    assert proxy.client_received == ["hi"]
    _, response = app.test_client.get("/")
    assert response.text == "ok"


def test_a_configured_port_serves_every_call(app: OldmanServer, port):
    app.config.KEEP_ALIVE = False

    @app.get("/")
    async def index(request):
        return text(str(request.server_port))

    client = TestClient(app, port=port)
    for _ in range(2):
        _, response = client.get("/")
        assert response.text == str(port)
    assert client.port == port


def test_a_call_warns_about_nothing(app: OldmanServer):
    @app.get("/")
    async def index(request):
        return text("ok")

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        app.test_client.get("/")
    assert not [w for w in caught if "loop argument" in str(w.message)]


def test_debug_is_the_call_s_own(app: OldmanServer):
    @app.get("/")
    async def index(request):
        raise ValueError("boom")

    access_log = app.config.ACCESS_LOG
    _, response = app.test_client.get("/", debug=True)
    assert "Traceback" in response.text
    assert app.state.mode is Mode.DEBUG
    assert app.config.ACCESS_LOG == access_log

    _, response = app.test_client.get("/")
    assert response.status == 500
    assert "Traceback" not in response.text
    assert app.state.mode is Mode.PRODUCTION


@pytest.mark.parametrize("method", ("put", "patch", "delete", "post"))
def test_method_not_allowed_is_the_app_s_own(app: OldmanServer, method):
    @app.get("/")
    async def index(request):
        return text("ok")

    _, response = getattr(app.test_client, method)("/")
    assert response.status == 405
    assert "Method" in response.text  # the error page, not an empty body
    assert set(response.headers["allow"].split(", ")) >= {"GET"}


def test_the_app_keeps_its_own_method_not_allowed_handler(app: OldmanServer):
    from oldman_server.exceptions import MethodNotAllowed

    @app.get("/")
    async def index(request):
        return text("ok")

    app.test_client.get("/")

    @app.exception(MethodNotAllowed)
    async def not_allowed(request, exception):
        return text("not here", status=405)

    _, response = app.test_client.put("/")
    assert response.text == "not here"


def _redirecting(app):
    @app.get("/from")
    async def source(request):
        return redirect("/to")

    @app.get("/to")
    async def target(request):
        return text("arrived")


def test_test_client_redirect_options(app: OldmanServer):
    _redirecting(app)
    assert app.test_client.get("/from")[1].text == "arrived"
    assert app.test_client.get("/from", allow_redirects=False)[1].status == 302
    assert app.test_client.get("/from", follow_redirects=False)[1].status == 302
    with pytest.raises(TypeError):
        app.test_client.get("/from", allow_redirects=True, follow_redirects=False)


def test_reusable_client_redirect_options_and_port(app: OldmanServer):
    _redirecting(app)
    with ReusableClient(app) as client:
        assert client.port > 0
        request, response = client.get("/from")
        assert response.text == "arrived"
        assert request.path == "/from"
        assert client.get("/from", allow_redirects=False)[1].status == 302
        assert client.get("/from", follow_redirects=False)[1].status == 302
        # Requests are collected per call; the middleware does not pile up.
        for _ in range(3):
            request, _ = client.get("/to")
            assert request.path == "/to"
        assert all(
            len(route.extra.request_middleware) == 0 for route in app.router.routes
        )


def test_the_websocket_client_uses_no_deprecated_api():
    result = subprocess.run(
        [sys.executable, "-W", "error::DeprecationWarning", "-c",
         "import oldman_server.testing.websocket"],
        capture_output=True, text=True, timeout=60,
    )
    assert result.returncode == 0, result.stderr


def test_websocket_additional_headers(app: OldmanServer):
    seen = []

    @app.websocket("/ws")
    async def handler(request, ws):
        seen.append(request.headers.get("x-token"))
        await ws.send("ok")

    async def client(ws):
        await ws.recv()
        await ws.recv()

    app.test_client.websocket("/ws", mimic=client, additional_headers={"X-Token": "t"})
    assert seen == ["t"]


def _closed_port():
    import socket

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_no_response_raises_instead_of_returning_none(app: OldmanServer):
    import httpx

    @app.get("/")
    async def index(request):
        return text("ok")

    url = f"http://127.0.0.1:{_closed_port()}/"
    with pytest.raises(ValueError, match="ConnectError"):
        app.test_client.get(url)
    with ReusableClient(app) as client:
        with pytest.raises(httpx.ConnectError):
            client.get(url)


def test_a_server_that_does_not_start_raises(app: OldmanServer):
    @app.before_server_start
    async def fail(app):
        raise RuntimeError("cannot start")

    @app.get("/")
    async def index(request):
        return text("ok")

    with pytest.raises(RuntimeError, match="cannot start"):
        app.test_client.get("/")


def test_test_client_and_reusable_client_on_one_app(app: OldmanServer):
    # A run leaves nothing a later run of the app takes for its own: the
    # test client's run, then a ReusableClient's server, then the test client.
    @app.get("/")
    async def index(request):
        return text("ok")

    assert app.test_client.get("/")[1].text == "ok"
    with ReusableClient(app) as client:
        assert client.get("/")[1].text == "ok"
    assert app.state.server_info == []
    assert app.test_client.get("/")[1].text == "ok"


def test_the_other_apps_starter_belongs_to_its_run(app: OldmanServer):
    @app.get("/")
    async def index(request):
        return text("ok")

    for _ in range(3):
        app.test_client.get("/")
    starter = app._start_run_servers
    assert sum(f.listener == starter for f in app._future_listeners) == 1
    assert app.state.run_apps is None
