"""AsyncTestClient: requests to the app's own server from inside a running
event loop (async tests), over a real local socket."""
import socket
import unittest

import httpx
import pytest

from oldman_server import OldmanServer
from oldman_server.native import native_server
from oldman_server.response import json, redirect, text
from oldman_server.testing import AsyncTestClient


def _app(app: OldmanServer, events: list) -> OldmanServer:
    @app.before_server_start
    async def started(app):
        events.append("start")

    @app.after_server_stop
    async def stopped(app):
        events.append("stop")

    @app.get("/")
    async def index(request):
        return json({
            "ip": request.ip,
            "native": type(request.headers).__name__ == "NativeHeaders",
        })

    @app.post("/echo")
    async def echo(request):
        return text(request.body.decode())

    @app.get("/from")
    async def source(request):
        return redirect("/")

    return app


async def test_each_call_outside_a_block_runs_a_server(app: OldmanServer):
    events: list = []
    _app(app, events)
    for _ in range(3):
        request, response = await app.async_test_client.get("/")
        assert response.status == 200
        assert request.path == "/"
    assert events == ["start", "stop"] * 3


async def test_a_block_serves_every_call(app: OldmanServer):
    events: list = []
    _app(app, events)
    async with AsyncTestClient(app) as client:
        port = client.port
        request, response = await client.post("/echo", content=b"hello")
        assert response.text == "hello"
        assert request.body == b"hello"
        request, response = await client.get("/")
        assert request.path == "/"
        assert client.port == port
    assert events == ["start", "stop"]


async def test_it_is_this_package_s_server_over_a_socket(app: OldmanServer):
    _app(app, [])
    assert not hasattr(app, "asgi_client")
    _, response = await app.async_test_client.get("/")
    # A real TCP peer, served by the server NATIVE_SERVER chooses.
    assert response.json["ip"] == "127.0.0.1"
    assert response.json["native"] is bool(native_server(app.config))


async def test_redirect_options_and_gathering(app: OldmanServer):
    _app(app, [])
    async with AsyncTestClient(app) as client:
        request, response = await client.get("/from")
        assert response.status == 200
        assert request.path == "/from"
        _, response = await client.get("/from", follow_redirects=False)
        assert response.status == 302
        _, response = await client.get("/from", allow_redirects=False)
        assert response.status == 302
        request, response = await client.get("/", gather_request=False)
        assert request is None and response.status == 200


async def test_websocket(app: OldmanServer):
    @app.websocket("/ws")
    async def echo(request, ws):
        await ws.send(await ws.recv())

    async def client(ws):
        await ws.send("ping")
        await ws.recv()
        await ws.recv()

    _, proxy = await app.async_test_client.websocket("/ws", mimic=client)
    assert proxy.client_received == ["ping"]


class AsyncCase(unittest.IsolatedAsyncioTestCase):
    """The use it is for: an IsolatedAsyncioTestCase, whose loop is running."""

    async def asyncSetUp(self):
        self.app = OldmanServer("async_case")

        @self.app.get("/")
        async def index(request):
            return text("case")

        self.client = AsyncTestClient(self.app)
        await self.client.__aenter__()

    async def asyncTearDown(self):
        await self.client.__aexit__(None, None, None)
        OldmanServer.unregister_app(self.app)

    async def test_get(self):
        _, response = await self.client.get("/")
        self.assertEqual(response.text, "case")

    async def test_one_shot_from_the_property(self):
        await self.client.__aexit__(None, None, None)
        _, response = await self.app.async_test_client.get("/")
        self.assertEqual(response.text, "case")
        await self.client.__aenter__()


async def test_no_response_raises(app: OldmanServer):
    _app(app, [])
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        closed = sock.getsockname()[1]
    with pytest.raises(httpx.ConnectError):
        await app.async_test_client.get(f"http://127.0.0.1:{closed}/")


async def test_a_server_that_does_not_start_raises(app: OldmanServer):
    @app.before_server_start
    async def fail(app):
        raise RuntimeError("cannot start")

    @app.get("/")
    async def index(request):
        return text("ok")

    with pytest.raises(RuntimeError, match="cannot start"):
        await app.async_test_client.get("/")


async def test_listeners_pair_up_when_start_fails(app: OldmanServer):
    events = []

    @app.before_server_start
    async def fail(app):
        events.append("before_start")
        raise RuntimeError("cannot start")

    @app.after_server_stop
    async def after_stop(app):
        events.append("after_stop")

    @app.before_server_stop
    async def before_stop(app):
        events.append("before_stop")

    with pytest.raises(RuntimeError):
        await app.async_test_client.get("/")
    assert events == ["before_start", "after_stop"]
