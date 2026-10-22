import uuid

from unittest.mock import Mock
from uuid import UUID, uuid4

import pytest

from oldman_server import OldmanServer, response
from oldman_server.exceptions import BadURL, OldmanServerException
from oldman_server.http.constants import HTTP
from oldman_server.native import native_server
from oldman_server.request import Request
from oldman_server.server import HttpProtocol


def test_no_request_id_not_called(monkeypatch):
    monkeypatch.setattr(uuid, "uuid4", Mock())
    request = Request(b"/", {}, None, "GET", None, None)

    assert request._id is None
    uuid.uuid4.assert_not_called()


def test_request_id_generates_from_request(monkeypatch):
    monkeypatch.setattr(Request, "generate_id", Mock())
    Request.generate_id.return_value = 1
    request = Request(b"/", {}, None, "GET", None, Mock())
    request.app.config.REQUEST_ID_HEADER = "foo"

    for _ in range(10):
        request.id
    Request.generate_id.assert_called_once_with(request)


def test_request_id_defaults_uuid():
    request = Request(b"/", {}, None, "GET", None, Mock())
    request.app.config.REQUEST_ID_HEADER = "foo"

    assert isinstance(request.id, UUID)

    # Makes sure that it has been cached and not called multiple times
    assert request.id == request.id == request._id


def test_name_none():
    request = Request(b"/", {}, None, "GET", None, None)

    assert request.name is None


def test_name_from_route():
    request = Request(b"/", {}, None, "GET", None, None)
    route = Mock()
    request.route = route

    assert request.name == route.name


def test_name_from_set():
    request = Request(b"/", {}, None, "GET", None, None)
    request._name = "foo"

    assert request.name == "foo"


@pytest.mark.parametrize(
    "request_id,expected_type",
    (
        (99, int),
        (uuid4(), UUID),
        ("foo", str),
    ),
)
def test_request_id(request_id, expected_type):
    app = OldmanServer("req-generator")

    @app.get("/")
    async def get(request):
        return response.empty()

    request, _ = app.test_client.get(
        "/", headers={"X-REQUEST-ID": f"{request_id}"}
    )
    assert request.id == request_id
    assert type(request.id) is expected_type


def test_custom_generator():
    REQUEST_ID = 99

    class FooRequest(Request):
        @classmethod
        def generate_id(cls, request):
            return int(request.headers["some-other-request-id"]) * 2

    app = OldmanServer("req-generator", request_class=FooRequest)

    @app.get("/")
    async def get(request):
        return response.empty()

    request, _ = app.test_client.get(
        "/", headers={"SOME-OTHER-REQUEST-ID": f"{REQUEST_ID}"}
    )
    assert request.id == REQUEST_ID * 2


def test_route_assigned_to_request(app):
    @app.get("/")
    async def get(request):
        return response.empty()

    request, _ = app.test_client.get("/")
    assert request.route is list(app.router.routes)[0]


def test_protocol_attribute(app):
    retrieved = None

    @app.get("/")
    async def get(request):
        nonlocal retrieved
        retrieved = request.protocol
        return response.empty()

    headers = {"Connection": "keep-alive"}
    _ = app.test_client.get("/", headers=headers)

    if native_server(app.config):
        # The native connection's stand-in for HttpProtocol: its state,
        # HTTP version and connection info.
        assert retrieved.state["requests_count"] == 1
        assert retrieved.version is HTTP.VERSION_1
        assert retrieved.conn_info.server_port == app.test_client.port
    else:
        assert isinstance(retrieved, HttpProtocol)


def test_ipv6_address_is_not_wrapped(app):
    @app.get("/")
    async def get(request):
        return response.json(
            {
                "client_ip": request.conn_info.client_ip,
                "client": request.conn_info.client,
            }
        )

    request, resp = app.test_client.get("/", host="::1")

    assert request.route is list(app.router.routes)[0]
    assert resp.json["client"] == "[::1]"
    assert resp.json["client_ip"] == "::1"
    assert request.ip == "::1"


def test_request_accept():
    app = OldmanServer("req-generator")

    @app.get("/")
    async def get(request):
        return response.empty()

    header_value = "text/plain;format=flowed, text/plain, text/*, */*"
    request, _ = app.test_client.get(
        "/",
        headers={"Accept": header_value},
    )
    assert str(request.accept) == header_value
    match = request.accept.match(
        "*/*;format=flowed",
        "text/plain;format=flowed",
        "text/plain",
        "text/*",
        "*/*",
    )
    assert match == "*/*;format=flowed"
    assert match.header.mime == "text/plain"
    assert match.header.params == {"format": "flowed"}

    header_value = (
        "text/plain; q=0.5,   text/html, text/x-dvi; q=0.8, text/x-c"
    )
    request, _ = app.test_client.get(
        "/",
        headers={"Accept": header_value},
    )
    assert [str(i) for i in request.accept] == [
        "text/html",
        "text/x-c",
        "text/x-dvi;q=0.8",
        "text/plain;q=0.5",
    ]
    match = request.accept.match(
        "application/json",
        "text/plain",  # Has lower q in accept header
        "text/html;format=flowed",  # Params mismatch
        "text/*",  # Matches
        "*/*",
    )
    assert match == "text/*"
    assert match.header.mime == "text/html"
    assert match.header.q == 1.0
    assert not match.header.params


def test_bad_url_parse():
    message = "Bad URL: my.redacted-domain.com:443"
    with pytest.raises(BadURL, match=message):
        Request(
            b"my.redacted-domain.com:443",
            Mock(),
            Mock(),
            Mock(),
            Mock(),
            Mock(),
            Mock(),
        )


def test_cannot_get_request_outside_of_cycle():
    with pytest.raises(OldmanServerException, match="No current request"):
        Request.get_current()


def test_get_current_request(app):
    @app.get("/")
    async def get(request):
        return response.json({"same": request is Request.get_current()})

    _, resp = app.test_client.get("/")
    assert resp.json["same"]


@pytest.mark.parametrize(
    "method,safe",
    (
        ("DELETE", False),
        ("GET", True),
        ("HEAD", True),
        ("OPTIONS", True),
        ("PATCH", False),
        ("POST", False),
        ("PUT", False),
    ),
)
def test_request_safe(method, safe):
    request = Request(b"/", {}, None, method, None, None)
    assert request.is_safe is safe


@pytest.mark.parametrize(
    "method,idempotent",
    (
        ("DELETE", True),
        ("GET", True),
        ("HEAD", True),
        ("OPTIONS", True),
        ("PATCH", False),
        ("POST", False),
        ("PUT", True),
    ),
)
def test_request_idempotent(method, idempotent):
    request = Request(b"/", {}, None, method, None, None)
    assert request.is_idempotent is idempotent


@pytest.mark.parametrize(
    "method,cacheable",
    (
        ("DELETE", False),
        ("GET", True),
        ("HEAD", True),
        ("OPTIONS", False),
        ("PATCH", False),
        ("POST", False),
        ("PUT", False),
    ),
)
def test_request_cacheable(method, cacheable):
    request = Request(b"/", {}, None, method, None, None)
    assert request.is_cacheable is cacheable


def test_custom_ctx():
    class CustomContext:
        FOO = "foo"

    class CustomRequest(Request[OldmanServer, CustomContext]):
        @staticmethod
        def make_context() -> CustomContext:
            return CustomContext()

    app = OldmanServer("Test", request_class=CustomRequest)

    @app.get("/")
    async def handler(request: CustomRequest):
        return response.json(
            [
                isinstance(request, CustomRequest),
                isinstance(request.ctx, CustomContext),
                request.ctx.FOO,
            ]
        )

    _, resp = app.test_client.get("/")

    assert resp.json == [True, True, "foo"]
