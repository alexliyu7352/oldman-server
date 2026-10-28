"""HEAD and OPTIONS answered by the router for every route (HTTP_AUTO_HEAD,
HTTP_AUTO_OPTIONS): HEAD by the path's GET route itself, OPTIONS by 204 and
Allow, without registering routes of their own."""
from pathlib import Path

import pytest

from oldman_server import CORS, Blueprint, OldmanServer, text
from oldman_server.exceptions import MethodNotAllowed
from oldman_server.views import HTTPMethodView


def test_head_runs_the_get_route():
    app = OldmanServer("auto_head")
    seen = []

    @app.get("/page", name="page")
    async def page(request):
        seen.append((request.method, request.route.name))
        return text("hello world")

    _, get = app.test_client.get("/page")
    request, head = app.test_client.head("/page")
    assert head.status == 200
    assert head.body == b""
    assert head.headers["content-length"] == get.headers["content-length"] == "11"
    assert head.headers["content-type"] == get.headers["content-type"]
    assert seen == [("GET", "auto_head.page"), ("HEAD", "auto_head.page")]
    assert request.method == "HEAD"
    assert request.route.methods == frozenset({"GET"})


def test_head_runs_the_blueprint_middleware():
    app = OldmanServer("auto_head_bp")
    bp = Blueprint("guarded", url_prefix="/guarded")
    called = []

    @bp.on_request
    async def deny(request):
        return text("sign in first", status=401)

    @bp.get("/data")
    async def data(request):
        called.append(request.method)
        return text("secret")

    app.blueprint(bp)

    _, get = app.test_client.get("/guarded/data")
    _, head = app.test_client.head("/guarded/data")
    assert get.status == head.status == 401
    assert head.body == b""
    assert called == []


def test_an_explicit_head_route_wins():
    app = OldmanServer("auto_head_explicit")

    @app.get("/x")
    async def get(request):
        return text("from get")

    @app.head("/x")
    async def head(request):
        return text("from head!", headers={"x-head": "1"})

    _, response = app.test_client.head("/x")
    assert response.status == 200
    assert response.headers["x-head"] == "1"
    assert response.headers["content-length"] == "10"


def test_head_and_options_with_several_hosts():
    app = OldmanServer("auto_hosts")

    @app.get("/site", host=["a.example", "b.example"])
    async def site(request):
        return text(request.host)

    for host in ("a.example", "b.example"):
        _, head = app.test_client.head("/site", headers={"host": host})
        assert head.status == 200
        _, options = app.test_client.options("/site", headers={"host": host})
        assert options.status == 204
        assert options.headers["allow"] == "GET, HEAD, OPTIONS"
    _, other = app.test_client.head("/site", headers={"host": "c.example"})
    assert other.status == 404


def test_class_view_head():
    app = OldmanServer("auto_view")

    class Item(HTTPMethodView):
        async def get(self, request):
            return text("item")

        async def put(self, request):
            return text("updated")

    app.add_route(Item.as_view(), "/item")

    _, head = app.test_client.head("/item")
    assert head.status == 200
    assert head.headers["content-length"] == "4"
    _, options = app.test_client.options("/item")
    assert options.status == 204
    assert options.headers["allow"] == "GET, HEAD, OPTIONS, PUT"


def test_options_is_answered_like_a_405_with_204():
    app = OldmanServer("auto_options")
    bp = Blueprint("bp", url_prefix="/bp")
    events = []

    @app.on_request
    async def app_request(request):
        events.append("app request")

    @app.on_response
    async def app_response(request, response):
        events.append("app response")
        response.headers["x-app"] = "1"

    @bp.on_request
    async def bp_request(request):
        events.append("bp request")
        return text("no", status=401)

    @bp.get("/x")
    async def bx(request):
        events.append("handler")
        return text("x")

    @app.post("/form")
    async def form(request):
        events.append("handler")
        return text("posted")

    app.blueprint(bp)

    _, response = app.test_client.options("/form")
    assert response.status == 204
    assert response.headers["allow"] == "OPTIONS, POST"
    assert response.headers["x-app"] == "1"
    assert response.body == b""
    assert events == ["app request", "app response"]

    events.clear()
    # As a 405 is: the application's middleware, not the blueprint's.
    _, response = app.test_client.options("/bp/x")
    assert response.status == 204
    assert response.headers["allow"] == "GET, HEAD, OPTIONS"
    assert events == ["app request", "app response"]

    _, response = app.test_client.options("/nothing-here")
    assert response.status == 404


def test_request_middleware_can_answer_an_options_request():
    app = OldmanServer("auto_options_middleware")

    @app.on_request
    async def gate(request):
        if request.method == "OPTIONS":
            return text("gated", status=403)

    @app.get("/x")
    async def x(request):
        return text("x")

    _, response = app.test_client.options("/x")
    assert response.status == 403
    assert response.text == "gated"


def test_an_options_route_is_used():
    app = OldmanServer("auto_options_route")

    @app.get("/x")
    async def get(request):
        return text("x")

    @app.options("/x")
    async def options(request):
        return text("custom", headers={"allow": "GET"})

    _, response = app.test_client.options("/x")
    assert response.status == 200
    assert response.text == "custom"
    assert response.headers["allow"] == "GET"


def test_a_405_lists_the_automatic_methods():
    app = OldmanServer("auto_405")

    @app.route("/x", methods=["GET", "POST"])
    async def x(request):
        return text("x")

    _, response = app.test_client.put("/x")
    assert response.status == 405
    assert response.headers["allow"] == "GET, HEAD, OPTIONS, POST"


def test_the_automatic_options_is_not_an_error():
    app = OldmanServer("auto_options_quiet")
    handled, reported = [], []

    @app.exception(MethodNotAllowed)
    async def not_allowed(request, exception):
        handled.append(request.method)
        return text("custom 405", status=405)

    @app.report_exception
    async def report(app, exception):
        reported.append(exception)

    @app.get("/x")
    async def x(request):
        return text("x")

    _, response = app.test_client.options("/x")
    assert response.status == 204
    _, response = app.test_client.delete("/x")
    assert response.status == 405
    assert response.text == "custom 405"
    assert handled == ["DELETE"]
    assert all(not isinstance(error, MethodNotAllowed) or error.method != "OPTIONS"
               for error in reported)


def test_settings_off_restore_only_the_registered_methods():
    app = OldmanServer("auto_off")
    app.config.HTTP_AUTO_HEAD = False
    app.config.HTTP_AUTO_OPTIONS = False

    @app.get("/x")
    async def x(request):
        return text("x")

    _, head = app.test_client.head("/x")
    assert head.status == 405
    assert head.headers["allow"] == "GET"
    _, options = app.test_client.options("/x")
    assert options.status == 405
    assert options.headers["allow"] == "GET"


def test_head_off_options_on():
    app = OldmanServer("auto_head_off")
    app.config.HTTP_AUTO_HEAD = False

    @app.get("/x")
    async def x(request):
        return text("x")

    _, head = app.test_client.head("/x")
    assert head.status == 405
    assert head.headers["allow"] == "GET, OPTIONS"
    _, options = app.test_client.options("/x")
    assert options.status == 204
    assert options.headers["allow"] == "GET, OPTIONS"


def test_websocket_routes_have_neither():
    app = OldmanServer("auto_websocket")

    @app.websocket("/ws")
    async def ws(request, sock):
        pass

    _, head = app.test_client.head("/ws")
    assert head.status == 405
    assert head.headers["allow"] == "GET"
    _, options = app.test_client.options("/ws")
    assert options.status == 405


def test_no_route_is_registered_for_them():
    app = OldmanServer("auto_routes")

    @app.get("/a", name="a")
    async def a(request):
        return text("a")

    @app.post("/b", name="b")
    async def b(request):
        return text("b")

    app.test_client.head("/a")
    app.test_client.options("/b")
    assert len(app.router.routes) == 2
    assert sorted(app.router.name_index) == ["auto_routes.a", "auto_routes.b"]
    assert app.url_for("a") == "/a"


def test_static_routes_keep_their_head(tmp_path: Path):
    app = OldmanServer("auto_static")
    (tmp_path / "file.txt").write_text("content")
    app.static("/files", tmp_path)

    # Registered with GET and HEAD itself: unchanged.
    _, head = app.test_client.head("/files/file.txt")
    assert head.status == 200
    assert head.body == b""
    _, options = app.test_client.options("/files/file.txt")
    assert options.status == 204
    assert options.headers["allow"] == "GET, HEAD, OPTIONS"


@pytest.mark.parametrize("route_policy", [False, True])
def test_cors_preflight_comes_first(route_policy):
    app = OldmanServer(f"auto_cors_{int(route_policy)}")
    app.config.CORS_ORIGINS = "https://a.example"

    @app.get("/api", cors=CORS(max_age=5) if route_policy else None)
    async def api(request):
        return text("api")

    preflight = {"origin": "https://a.example", "access-control-request-method": "GET"}
    _, response = app.test_client.options("/api", headers=preflight)
    assert response.status == 204
    assert response.headers["access-control-allow-origin"] == "https://a.example"
    assert "allow" not in response.headers

    other = {**preflight, "origin": "https://b.example"}
    _, response = app.test_client.options("/api", headers=other)
    assert response.status == 204
    assert response.headers["allow"] == "GET, HEAD, OPTIONS"
    assert "access-control-allow-origin" not in response.headers
