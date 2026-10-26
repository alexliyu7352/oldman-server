"""The flat path's native calls must leave the same state as the Python they replace."""
import pytest
from oldman_server.exceptions import URITooLong

from oldman_server.native.engine import ApplicationRequest
from oldman_server.native.host import _BufferedExchange
from oldman_server.request import Request
from oldman_server.request.types import _PARSED_URLS

_UNSET = object()


def native(method, target, body=b"", http_version="1.1"):
    headers = ((b"Host", b"127.0.0.1"), (b"Content-Length", str(len(body)).encode()))
    return ApplicationRequest(method, target, headers, body, http_version=http_version,
                              client=("127.0.0.1", 40000), server=("127.0.0.1", 8000))


def slots(value):
    """Every slot of value, _UNSET where it was never assigned."""
    out = {}
    for cls in type(value).__mro__:
        for name in getattr(cls, "__slots__", ()):
            if name != "__weakref__":
                try:
                    out[name] = cls.__dict__[name].__get__(value, type(value))
                except AttributeError:
                    out[name] = _UNSET
    return out


def python_prepare(request, app, max_body, request_max_size):
    target, version, method, body, more_body = request.sanic_request()
    made = Request._from_native_fields(target, version, method, app, request)
    exchange = _BufferedExchange(body, max_body, request, more_body, request_max_size)
    made.stream = exchange
    return made, exchange


@pytest.mark.parametrize("method, target, body", [
    ("GET", b"/", b""),
    ("GET", b"/user/0?x=1&y=%20", b""),
    ("POST", b"/user", b""),
    ("POST", b"/echo", b"a body"),
    ("DELETE", b"/a/b/c?", b""),
])
def test_sanic_prepare_assigns_what_the_python_path_assigns(method, target, body):
    app, max_body, request_max_size = object(), 1 << 20, float("inf")
    _PARSED_URLS.pop(target, None)
    first = native(method, target, body)
    # Not parsed yet: the Python path parses (and caches) it.
    assert first.sanic_prepare(app, max_body, request_max_size) is None
    expected, expected_exchange = python_prepare(first, app, max_body, request_max_size)
    second = native(method, target, body)
    made, exchange = second.sanic_prepare(app, max_body, request_max_size)
    assert type(made) is Request and type(exchange) is _BufferedExchange
    got, want = slots(made), slots(expected)
    assert got.keys() == want.keys()
    for name in want:
        if name == "stream":
            assert got[name] is exchange
        elif name == "_native_request":
            assert want[name] is first and got[name] is second
        elif name in ("app", "_parsed_url"):
            assert got[name] is want[name]  # the same app, the one cached parse
        elif name == "_match_info":
            assert got[name] == want[name] == {} and got[name] is not want[name]
        else:
            assert got[name] == want[name], name
            assert type(got[name]) is type(want[name]), name
    got, want = slots(exchange), slots(expected_exchange)
    assert got.keys() == want.keys()
    for name in want:
        if name == "native":
            assert want[name] is first and got[name] is second
        else:
            assert got[name] == want[name] and type(got[name]) is type(want[name]), name
    # The made request behaves as one: its lazy fields read the native request.
    assert (made.path, made.query_string, made.headers["host"]) == (
        expected.path, expected.query_string, expected.headers["host"])


def test_sanic_prepare_leaves_a_refused_request_to_the_python_path():
    app = object()
    long_target = b"/" + b"a" * 65535
    request = native("GET", long_target)
    assert request.sanic_prepare(app, 1, 1) is None
    with pytest.raises(URITooLong):
        python_prepare(request, app, 1, 1)
    # Metadata the adapter requires: the same error either way.
    request = native("GET", b"/", http_version="")
    with pytest.raises(ValueError, match="metadata"):
        request.sanic_prepare(app, 1, 1)
    with pytest.raises(ValueError, match="metadata"):
        request.sanic_request()
