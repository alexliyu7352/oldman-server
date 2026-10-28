"""The error handler contract, the same for plain and coroutine handlers and
for a plain or coroutine `default()`, on both servers and (with a request
subclass) on the native host's general path as well as its fast path."""
import logging

import pytest

from oldman_server import OldmanServer
from oldman_server.exceptions import NotFound
from oldman_server.handlers import ErrorHandler
from oldman_server.request import Request
from oldman_server.response import text


class Custom(Request):
    pass


@pytest.fixture(params=["flat", "full"])
def make_app(request):
    def make(name, **kwargs):
        if request.param == "full":
            kwargs["request_class"] = Custom
        return OldmanServer(name, **kwargs)
    return make


@pytest.fixture(params=["sync", "async"])
def kind(request):
    return request.param


def handler_of(kind, result):
    """An error handler of `kind` whose body is `result(request, exception)`."""
    if kind == "sync":
        def handler(request, exception):
            return result(request, exception)
    else:
        async def handler(request, exception):
            return result(request, exception)
    return handler


def raises(exception):
    def result(request, error):
        raise exception
    return result


def test_a_handler_returning_none_gets_the_default_page(make_app, kind):
    app = make_app("none_default")
    app.error_handler.add(KeyError, handler_of(kind, lambda request, exception: None))
    app.error_handler.add(NotFound, handler_of(kind, lambda request, exception: None))

    @app.get("/key")
    async def key(request):
        raise KeyError("missing")

    _, response = app.test_client.get("/key")
    assert response.status == 500
    assert "Internal Server Error" in response.text
    _, response = app.test_client.get("/nowhere")
    assert response.status == 404
    assert "Not Found" in response.text


def test_a_handler_raising_a_framework_exception_gets_its_page(make_app, kind):
    app = make_app("raise_framework")
    app.error_handler.add(ValueError, handler_of(kind, raises(NotFound("converted"))))

    @app.get("/")
    async def index(request):
        raise ValueError("original")

    _, response = app.test_client.get("/")
    assert response.status == 404
    assert "converted" in response.text


def test_a_handler_raising_another_exception_gets_a_500(make_app, kind, caplog):
    app = make_app("raise_other")
    app.error_handler.add(KeyError, handler_of(kind, raises(ValueError("inner"))))

    @app.get("/")
    async def index(request):
        raise KeyError("original")

    with caplog.at_level(logging.ERROR):
        _, response = app.test_client.get("/")
    assert response.status == 500
    assert response.text == "An error occurred while handling an error"
    assert any("Exception raised in exception handler" in record.message
               for record in caplog.records)


def test_a_coroutine_default_is_awaited(make_app):
    # An ErrorHandler whose default() is a coroutine (as oldman's error
    # page handler), whatever kind of handler falls back to it.
    class AsyncDefault(ErrorHandler):
        async def default(self, request, exception):
            return text(f"page {getattr(exception, 'status_code', 500)}",
                        status=getattr(exception, "status_code", 500))

    app = make_app("async_default", error_handler=AsyncDefault())

    @app.exception(KeyError)
    def none_sync(request, exception):
        return None

    @app.exception(LookupError)
    async def raise_async(request, exception):
        raise NotFound("converted")

    @app.get("/plain")
    async def plain(request):
        raise RuntimeError("no handler")

    @app.get("/none")
    async def none(request):
        raise KeyError("x")

    @app.get("/raise")
    async def raised(request):
        raise IndexError("x")

    for path, expected in (("/plain", (500, "page 500")), ("/none", (500, "page 500")),
                           ("/raise", (404, "page 404"))):
        _, response = app.test_client.get(path)
        assert (response.status, response.text) == expected, path


def test_a_handler_returning_no_response_gets_the_error_page(make_app):
    # Handling the error failed (not a response): that failure gets the
    # error page, on the fast path as on the others.
    app = make_app("not_a_response")

    @app.exception(KeyError)
    async def handler(request, exception):
        return {"not": "a response"}

    @app.get("/")
    async def index(request):
        raise KeyError("x")

    _, response = app.test_client.get("/")
    assert response.status == 500
    assert "Invalid response type" in response.text
