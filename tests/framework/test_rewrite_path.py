"""@app.rewrite_path: functions that may give the request a new path before
routing (an i18n prefix, say), in registration order; None keeps it."""
from oldman_server.exceptions import NotFound
from oldman_server.response import json


def strip_language(request):
    language, _, rest = request.path[1:].partition("/")
    if language in ("en", "zh"):
        request.ctx.language = language
        return "/" + rest
    return None


def test_the_rewritten_path_is_routed(app):
    app.rewrite_path(strip_language)

    @app.get("/hello/<name>")
    async def hello(request, name):
        return json({
            "name": name, "path": request.path, "query": request.query_string,
            "raw_url": request.raw_url.decode(),
            "language": getattr(request.ctx, "language", None),
        })

    _, response = app.test_client.get("/zh/hello/world?x=1")
    assert response.status == 200
    assert response.json == {"name": "world", "path": "/hello/world", "query": "x=1",
                             "raw_url": "/zh/hello/world?x=1", "language": "zh"}

    _, response = app.test_client.get("/hello/world")
    assert response.json["path"] == "/hello/world"
    assert response.json["language"] is None


def test_rewriters_run_in_order_on_the_previous_result(app):
    calls = []

    @app.rewrite_path
    def first(request):
        calls.append(("first", request.path))
        return "/b"

    @app.rewrite_path
    def second(request):
        calls.append(("second", request.path))
        return None

    @app.get("/b")
    async def b(request):
        return json(request.path)

    _, response = app.test_client.get("/a")
    assert response.json == "/b"
    assert calls == [("first", "/a"), ("second", "/b")]


def test_no_route_for_the_rewritten_path_is_not_found(app):
    app.rewrite_path(strip_language)

    @app.get("/zh/page")
    async def page(request):
        return json("unreachable")

    _, response = app.test_client.get("/zh/page")
    assert response.status == 404


def test_a_rewriter_exception_goes_to_the_error_handler(app):
    @app.rewrite_path
    def reject(request):
        raise NotFound("no such language")

    @app.get("/")
    async def index(request):
        return json("unreachable")

    _, response = app.test_client.get("/")
    assert response.status == 404
    assert "no such language" in response.text


def test_a_path_without_a_leading_slash_is_a_server_error(app):
    app.rewrite_path(lambda request: "relative")

    @app.get("/")
    async def index(request):
        return json("unreachable")

    _, response = app.test_client.get("/")
    assert response.status == 500
