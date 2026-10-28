"""The flat path's native calls must leave the same state as the Python they replace."""
import asyncio
import types

import pytest
from oldman_server import Blueprint, OldmanServer
from oldman_server.exceptions import URITooLong
from oldman_server.http.constants import Stage
from oldman_server.response import HTTPResponse, empty, html, json, raw, redirect, text
import uvloop

from oldman_server.native import _native
from oldman_server.native.engine import ApplicationRequest, Engine
from oldman_server.native.host import NativeApplication, _BufferedExchange, _sent_content_type
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
    made, exchange, token = second.sanic_prepare(app, max_body, request_max_size)
    assert type(made) is Request and type(exchange) is _BufferedExchange
    assert Request._current.get() is made  # set as the Python path sets it
    Request._current.reset(token)
    assert Request._current.get(None) is None
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


def made(response, max_body=1 << 20):
    """A prepared request and exchange (Python path), ready for sanic_finish."""
    request = native("GET", b"/finish")
    made_request, exchange = python_prepare(request, object(), max_body, float("inf"))
    return request, made_request, exchange, response


def python_finish(made_request, exchange, response):
    # _call_flat's Request.respond and its inlined send(end_stream=True).
    exchange.response = response
    response.stream = exchange
    made_request.responded = True
    response._headers, headers = _sent_content_type(response.content_type)
    exchange.result = response.status, response.body, headers
    exchange.stage = Stage.IDLE


def state(*values):
    return [{name: (id(value) if name in ("stream", "response", "native", "_native_request") else value)
             for name, value in slots(value).items()} for value in values]


@pytest.mark.parametrize("make", [
    lambda: text("plain"), lambda: json({"a": 1}), lambda: html("<b>x</b>"),
    lambda: raw(b"raw", content_type="application/octet-stream"), lambda: text("created", status=201),
])
def test_sanic_finish_leaves_what_respond_and_send_leave(make):
    left, right = made(make()), made(make())
    assert left[0].sanic_finish(*left[1:]) == 1
    python_finish(*right[1:])
    for got, want in zip(state(*left[1:]), state(*right[1:])):
        assert got.keys() == want.keys()
        for name in want:
            if name not in ("stream", "response", "native", "_native_request", "app"):
                assert got[name] == want[name], name
    request, exchange, response = left[1:]
    assert exchange.response is response and response.stream is exchange
    assert request.responded and exchange.stage is Stage.IDLE
    assert exchange.result == right[2].result
    assert response._headers is right[3]._headers  # the one cached _SentContentType


def headers_read():
    response = text("x")
    response.headers  # noqa: B018 - built: the Python path sends them
    return response


class TextResponse(HTTPResponse):
    pass


@pytest.mark.parametrize("case", [
    "responded", "headers read", "cookie", "redirect", "no content", "not modified", "informational",
    "too large", "str body", "no content type", "subclass", "streaming", "other response",
    "stage", "result", "failure", "not a response", "other stream",
])
def test_sanic_finish_changes_nothing_it_does_not_finish(case):
    response = {"headers read": headers_read, "redirect": lambda: redirect("/x"), "no content": empty,
                "not modified": lambda: text("", status=304),
                "informational": lambda: text("", status=199),
                "no content type": lambda: raw(b"x", content_type=None),
                "subclass": lambda: TextResponse(b"x")}.get(case, lambda: text("body"))()
    if case == "not a response":
        response = {"not": "a response"}
    request, made_request, exchange, _ = made(response, max_body=2 if case == "too large" else 1 << 20)
    if case == "responded":
        made_request.responded = True
    elif case == "cookie":
        response.add_cookie("k", "v")
    elif case == "str body":
        response.body = "text"
    elif case == "streaming":
        exchange.streaming = "open"
    elif case == "other response":
        exchange.response = text("earlier")
    elif case == "stage":
        exchange.stage = Stage.RESPONSE
    elif case == "result":
        exchange.result = (200, b"", ())
    elif case == "failure":
        exchange.failure = RuntimeError()
    elif case == "other stream":
        made_request.stream = None
    before = state(made_request, exchange, response) if case != "not a response" else state(made_request, exchange)
    assert request.sanic_finish(made_request, exchange, response) == 0
    after = state(made_request, exchange, response) if case != "not a response" else state(made_request, exchange)
    assert after == before


def finishing_app(name, receiver=False, middleware=None):
    app = OldmanServer(name, configure_logging=False)
    if receiver:
        @app.signal("http.lifecycle.response")
        async def observe(request, response):
            response.headers["x-seen"] = "1"
    if middleware == "app":
        @app.on_response
        async def leave(request, response):
            response.headers["x-left"] = request.path
    bp = Blueprint("bp", url_prefix="/bp")
    if middleware == "route":
        @bp.on_response
        def bp_leave(request, response):
            response.headers["x-bp"] = "1"

    @bp.get("/text")
    async def bp_text(request):
        return text("bp")
    app.blueprint(bp)

    @app.get("/text")
    async def text_route(request):
        return text("plain")

    @app.get("/json/<value:int>")
    def json_route(request, value):
        return json({"value": value})

    @app.get("/cookie")
    async def cookie(request):
        response = text("c")
        response.add_cookie("k", "v")
        return response

    @app.get("/large")
    async def large(request):
        return raw(b"x" * 4096)

    @app.get("/empty")
    async def nothing(request):
        return empty()

    @app.get("/streamed")
    async def streamed(request):
        await request.respond(text("early"))

    @app.get("/raise")
    async def raises(request):
        raise ValueError("boom")

    @app.post("/echo")
    async def echo(request):
        return raw(request.body)
    return app


FINISHING = [("GET", b"/bp/text", b""), ("GET", b"/text", b""), ("GET", b"/json/3", b""), ("GET", b"/cookie", b""),
             ("GET", b"/large", b""), ("GET", b"/empty", b""), ("GET", b"/streamed", b""),
             ("GET", b"/raise", b""), ("POST", b"/echo", b"body"), ("GET", b"/missing", b""),
             ("POST", b"/text", b""), ("HEAD", b"/text", b"")]


@pytest.mark.parametrize("receiver, middleware", [(False, None), (True, None), (False, "app"), (False, "route")])
def test_the_flat_path_answers_the_same_with_and_without_sanic_finish(receiver, middleware):
    async def scenario(natively):
        app = finishing_app(f"finish_{receiver}_{middleware}_{natively}", receiver, middleware)
        app.state.native_loop = asyncio.get_running_loop()
        await app._startup()
        adapter = NativeApplication(app, 2048)
        assert adapter._flat and adapter._finish_natively is not receiver
        adapter._finish_natively = natively and not receiver
        results = []
        for method, target, body in FINISHING * 2:  # the second round with each target cached
            try:
                results.append(await adapter._call_flat(native(method, target, body)))
            except Exception as error:  # Compare escaping failures too.
                results.append(("raised", type(error).__name__, str(error)))
        return results

    natively, in_python = uvloop.run(scenario(True)), uvloop.run(scenario(False))
    for request, left, right in zip(FINISHING * 2, natively, in_python, strict=True):
        assert left == right, request
    statuses = [result[0] if result else None for result in natively[:len(FINISHING)]]
    # /large is over the adapter's 2048-byte limit: the error handler answers.
    # HEAD /text is answered by its GET route (HTTP_AUTO_HEAD).
    assert statuses == [200, 200, 200, 200, 500, 204, 200, 500, 200, 404, 405, 200]
    if middleware:  # it ran, on the route it applies to
        name = b"x-bp" if middleware == "route" else b"x-left"
        assert dict(natively[0][2]).get(name) and (middleware == "route") != bool(dict(natively[1][2]).get(name))


class Settling:
    """The Engine's registries and answer queue, as _application_finished uses them."""

    def __init__(self, case, request_id):
        self._application_tasks = {request_id: "task"} if case == "registered" else {7: "other"}
        self._application_inbound = {request_id: "inbound"} if case == "inbound" else {}
        self._application_timeouts = {request_id} if case == "timed out" else set()
        self._application_lost = {request_id, 7} if case == "lost" else set()
        self._application_terminal = {request_id} if case == "answered" else set()
        self._application_returns = [("earlier",)]
        self._application_flush_scheduled = case != "first"
        self._decision_stats = {"python_application_finished": 3}
        self.scheduled = []
        self._loop = types.SimpleNamespace(call_soon=self.scheduled.append)

    def _flush_application(self):
        pass

    _application_finished = Engine._application_finished

    def state(self):
        return (self._application_tasks, self._application_inbound, self._application_timeouts,
                self._application_lost, self._application_terminal, self._application_returns,
                self._application_flush_scheduled, self._decision_stats, len(self.scheduled))


@pytest.mark.parametrize("case", ["first", "batched", "registered", "inbound", "timed out", "lost", "answered"])
def test_sanic_finish_settles_as_application_finished(case):
    request, made_request, exchange, response = made(text("settled"))
    engine = Settling(case, request.request_id)
    _native.set_application_state(
        engine, engine._application_tasks, engine._application_inbound, engine._application_timeouts,
        engine._application_lost, engine._application_terminal, engine._application_returns,
        engine._decision_stats, engine._loop.call_soon)
    token = Request._current.set(made_request)
    try:
        assert request.sanic_finish(made_request, exchange, response, engine, token) == 2
    finally:
        _native.clear_application_state(engine)
    assert Request._current.get(None) is None  # reset with the request settled
    expected = Settling(case, request.request_id)
    python_finish(*made(text("settled"))[1:])
    expected._application_finished(request.request_id, request.shard, exchange.result)
    assert engine.state() == expected.state()
    if case == "first":
        assert engine.scheduled == [engine._flush_application]


def test_sanic_finish_settles_only_for_the_engine_serving():
    request, made_request, exchange, response = made(text("x"))
    engine, other = Settling("batched", 1), Settling("batched", 1)
    _native.set_application_state(
        engine, engine._application_tasks, engine._application_inbound, engine._application_timeouts,
        engine._application_lost, engine._application_terminal, engine._application_returns,
        engine._decision_stats, engine._loop.call_soon)
    _native.clear_application_state(other)  # not the one set: nothing changes
    token = Request._current.set(made_request)
    try:
        assert request.sanic_finish(made_request, exchange, response, other, token) == 1
        assert engine._application_returns == [("earlier",)]
        assert Request._current.get() is made_request  # not settled: the caller resets it
    finally:
        Request._current.reset(token)
        _native.clear_application_state(engine)
    request, made_request, exchange, response = made(text("x"))
    assert request.sanic_finish(made_request, exchange, response, engine) == 1  # cleared
