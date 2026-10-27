"""The flat native path must match Sanic's generic handle_request path exactly."""
import asyncio
import logging

import pytest
from oldman_server import Blueprint, OldmanServer
from oldman_server.exceptions import NotFound, OldmanServerException
from oldman_server.response import empty, html, json, raw, redirect, text
import uvloop

from oldman_server.native.engine import ApplicationRequest
from oldman_server.native.host import NativeApplication


def native(method, target, body=b"", headers=()):
    headers = ((b"Host", b"127.0.0.1"), (b"Content-Length", str(len(body)).encode())) + headers
    return ApplicationRequest(method, target, headers, body, http_version="1.1",
                              client=("127.0.0.1", 40000), server=("127.0.0.1", 8000))


# The http.* events the native host dispatches (on either path).
SIGNALS = ("http.lifecycle.request", "http.lifecycle.handle", "http.routing.before",
           "http.routing.after", "http.handler.before", "http.handler.after",
           "http.lifecycle.response", "http.lifecycle.exception")


def build(name, log, kept, signals=False):
    app = OldmanServer(name, configure_logging=False)
    bp = Blueprint("bp", url_prefix="/bp")

    def record(event, attach_to=None):
        async def observe(**context):
            response = context.get("response")
            log.append(("signal", event, attach_to, context["request"].path, sorted(context),
                        getattr(response, "status", response)))
        return observe

    if signals:
        for event in SIGNALS:
            app.signal(event)(record(event))
        for event in ("http.middleware.before", "http.middleware.after"):
            for attach_to in ("request", "response"):
                app.signal(event, condition={"attach_to": attach_to})(record(event, attach_to))

    @app.on_request
    async def enter(request):
        log.append(("enter", request.path))
        if request.path == "/short":
            return text("short", status=202)
        if request.path == "/falsy":
            return 0
        if request.path == "/mw-raise":
            raise OldmanServerException("request middleware failed", status_code=418)

    @app.on_response
    async def leave(request, response):
        log.append(("leave", request.path, response.status))
        if not request.path.startswith("/kept"):
            response.headers["x-seen"] = "1"
        if request.path == "/replace":
            return text("replaced", status=201)
        if request.path == "/resp-raise":
            raise RuntimeError("response middleware failed")

    @bp.on_request
    def bp_enter(request):
        log.append(("bp", request.path))

    @bp.get("/item/<value:int>")
    async def item(request, value):
        return json({"value": value, "args": request.args})
    app.blueprint(bp)

    @app.get("/json")
    async def json_route(request):
        return json({"message": "Hello, World!"})

    @app.get("/kept")
    async def kept_route(request):
        # Headers never touched before the send; read them only afterwards.
        response = json({"kept": True})
        kept.append(response)
        return response

    @app.get("/kept-empty")
    async def kept_empty(request):
        response = empty()
        kept.append(response)
        return response

    @app.get("/text")
    def text_route(request):
        response = text("plain", headers={"X-A": "a\r\nb"})
        response.add_cookie("k", "v")
        return response

    @app.post("/body")
    async def body_route(request):
        return raw(request.headers["host"].encode() + b":" + request.body,
                   content_type="application/octet-stream")

    @app.get("/ignored")
    async def ignored(request):
        return raw(b"[" + request.body + b"]")

    def plain(path):
        async def handler(request):
            return text(path)
        return handler

    for path in ("/short", "/falsy", "/mw-raise", "/replace", "/resp-raise"):
        app.get(path, name=path.strip("/").replace("-", "_"))(plain(path))

    @app.get("/raise")
    async def raises(request):
        raise ValueError("boom")

    @app.get("/notfound")
    async def notfound(request):
        raise NotFound("nothing")

    @app.get("/respond")
    async def respond(request):
        response = await request.respond(text("early"))
        assert response is not None
        return None

    @app.get("/both")
    async def both(request):
        await request.respond(text("first"))
        return text("ignored")

    @app.get("/none")
    async def none(request):
        return None

    @app.get("/dict")
    async def wrong_type(request):
        return {"not": "a response"}

    @app.get("/nocontent")
    async def nocontent(request):
        return empty()

    @app.get("/html")
    async def html_route(request):
        return html("<b>x</b>")

    @app.get("/redirect")
    async def redirect_route(request):
        return redirect("/json")

    @app.get("/duplicate")
    async def duplicate(request):
        response = text("dup")
        response.headers.add("x-dup", "1")
        response.headers.add("x-dup", "2")
        return response

    @app.get("/length")
    async def length(request):
        return raw(b"abc", headers={"content-length": "4"})

    @app.exception(KeyError)
    async def key_error(request, exception):
        return text("key handled", status=409)

    @app.get("/keyerror")
    async def key_route(request):
        raise KeyError("x")

    return app


REQUESTS = [
    ("GET", b"/json", b""), ("GET", b"/kept", b""), ("GET", b"/kept-empty", b""), ("GET", b"/text", b""), ("POST", b"/body", b"payload"),
    ("GET", b"/ignored", b"unread"), ("GET", b"/bp/item/7?q=1&q=2", b""),
    ("GET", b"/bp/item/nope", b""), ("GET", b"/short", b""), ("GET", b"/falsy", b""),
    ("GET", b"/mw-raise", b""), ("GET", b"/replace", b""), ("GET", b"/resp-raise", b""),
    ("GET", b"/raise", b""), ("GET", b"/notfound", b""), ("GET", b"/respond", b""),
    ("GET", b"/both", b""), ("GET", b"/none", b""), ("GET", b"/dict", b""),
    ("GET", b"/nocontent", b""), ("GET", b"/html", b""), ("GET", b"/redirect", b""),
    ("GET", b"/duplicate", b""), ("GET", b"/length", b""), ("GET", b"/keyerror", b""),
    ("GET", b"/missing", b""), ("POST", b"/json", b""), ("HEAD", b"/json", b""),
]


def run_all(flat, signals=False):
    log, kept = [], []

    async def scenario():
        app = build(f"flat_equivalence_{flat}_{signals}", log, kept, signals)
        app.state.native_loop = asyncio.get_running_loop()
        await app._startup()
        adapter = NativeApplication(app, 65536)
        assert adapter._flat  # The fixture uses only features the flat path covers.
        adapter._flat = flat
        results = []
        for method, target, body in REQUESTS:
            try:
                results.append(await adapter(native(method, target, body)))
            except Exception as error:  # Compare escaping failures too.
                results.append(("raised", type(error).__name__, str(error)))
        return results

    results = uvloop.run(scenario())
    return results, log, [list(response.headers.items()) for response in kept]


@pytest.mark.parametrize("signals", [False, True])
def test_flat_path_matches_generic_handle_request(signals):
    flat, flat_log, flat_kept = run_all(True, signals)
    generic, generic_log, generic_kept = run_all(False, signals)
    assert flat_kept == generic_kept
    assert flat_kept == [[("content-type", "application/json")], []]
    for request, left, right in zip(REQUESTS, flat, generic, strict=True):
        assert left == right, request
    assert flat_log == generic_log
    # Spot-check that the fixture exercises what it claims.
    statuses = {request[:2]: result[0] for request, result in zip(REQUESTS, flat)}
    assert statuses["GET", b"/json"] == 200 and statuses["POST", b"/json"] == 405
    assert statuses["GET", b"/short"] == 202 and statuses["GET", b"/replace"] == 201
    assert statuses["GET", b"/keyerror"] == 409 and statuses["GET", b"/missing"] == 404
    assert statuses["GET", b"/raise"] == 500 and statuses["GET", b"/bp/item/7?q=1&q=2"] == 200
    events = {entry[1:3] for entry in flat_log if entry[0] == "signal"}
    assert events == ({(event, None) for event in SIGNALS} | {
        (event, attach_to) for event in ("http.middleware.before", "http.middleware.after")
        for attach_to in ("request", "response")} if signals else set())


@pytest.mark.parametrize("change, reason", [
    ("signal", "the signal http.lifecycle.* is a parameterized http event"),
    ("request_class", "Custom, not Request"),
    ("handle_request", "Overridden replaces handle_request"),
    ("touchup", "TOUCHUP is off"),
])
def test_flat_path_is_not_used_outside_its_contract(change, reason, caplog):
    async def scenario():
        from oldman_server.request import Request
        if change == "request_class":
            class Custom(Request):
                pass
            app = OldmanServer(f"flat_off_{change}", configure_logging=False, request_class=Custom)
        elif change == "handle_request":
            class Overridden(OldmanServer):
                async def handle_request(self, request):
                    return await super().handle_request(request)
            app = Overridden(f"flat_off_{change}", configure_logging=False)
        elif change == "touchup":
            app = OldmanServer(f"flat_off_{change}", configure_logging=False)
            app.config.TOUCHUP = False  # as debug mode sets it
        else:
            app = OldmanServer(f"flat_off_{change}", configure_logging=False)

            # One named by a parameter: TouchUp cannot tell which dispatches it receives.
            @app.signal("http.lifecycle.<stage>")
            async def observe(stage, **context):
                pass

        @app.get("/")
        async def handler(request):
            return text("ok")
        app.state.native_loop = asyncio.get_running_loop()
        await app._startup()
        with caplog.at_level(logging.INFO, logger="oldman_server.server"):
            adapter = NativeApplication(app, 65536)
        assert not adapter._flat
        # Said once, at startup, with the reason.
        notes = [record.getMessage() for record in caplog.records
                 if "without its fast path" in record.getMessage()]
        assert len(notes) == 1 and reason in notes[0], notes
        assert (await adapter(native("GET", b"/")))[:2] == (200, b"ok")
    uvloop.run(scenario())


def test_flat_path_is_used_without_a_note(caplog):
    async def scenario():
        app = OldmanServer("flat_on", configure_logging=False)

        @app.get("/")
        async def handler(request):
            return text("ok")
        app.state.native_loop = asyncio.get_running_loop()
        await app._startup()
        with caplog.at_level(logging.INFO, logger="oldman_server.server"):
            adapter = NativeApplication(app, 65536)
        assert adapter._flat
        assert not [record for record in caplog.records if "fast path" in record.getMessage()]
    uvloop.run(scenario())
