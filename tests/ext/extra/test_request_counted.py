import socket

from unittest.mock import Mock

import pytest

from oldman_server import OldmanServer
from oldman_server.compat import Header
from oldman_server.response import json
from oldman_server.testing.reusable import ReusableClient

from oldman_server.ext import CountedRequest


def free_port():
    # ReusableClient's default is a random port in 5000-65000, which an
    # outgoing connection may hold (the ephemeral range overlaps it).
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(autouse=True)
def reset_counter():
    yield
    CountedRequest.reset_count()


def test_counter_increments(app: OldmanServer):
    app.request_class = CountedRequest

    @app.get("/")
    async def handler(request: CountedRequest):
        return json({"count": request.count})

    @app.get("/info")
    async def info(request: CountedRequest):
        return json({"state": request.app.m.state})

    with ReusableClient(app, port=free_port()) as client:
        for i in range(1, 10):
            _, response = client.get("/")
            assert response.json["count"] == i


def test_counter_increment_on_state(app: OldmanServer):
    mock = Mock()
    mock.state = {}
    app.multiplexer = mock

    for i in range(1, 10):
        CountedRequest(b"/", Header({}), "", "", Mock(), app)
        assert CountedRequest.count == i
