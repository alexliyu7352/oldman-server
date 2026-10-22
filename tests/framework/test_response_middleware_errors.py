"""An exception from response middleware is answered by the error handler,
like one from the handler; the middleware does not run again on the error
response. While an error response is being sent, a failing response
middleware is only logged (no second error response)."""
import logging

from oldman_server.exceptions import NotFound, Unauthorized
from oldman_server.response import text


def test_response_middleware_error_goes_to_the_error_handler(app):
    calls = []

    @app.on_response
    async def fail(request, response):
        calls.append(response.status)
        raise Unauthorized("session expired")

    @app.get("/")
    async def handler(request):
        return text("handler")

    _, response = app.test_client.get("/")
    assert response.status == 401
    assert "session expired" in response.text
    assert calls == [200]  # not again on the error response


def test_custom_error_handler_sees_the_middleware_exception(app):
    class SaveFailed(Exception):
        pass

    @app.on_response
    async def save(request, response):
        raise SaveFailed("could not save the session")

    @app.exception(SaveFailed)
    async def handle(request, exception):
        return text(f"handled: {exception}", status=503)

    @app.get("/")
    async def handler(request):
        return text("handler")

    _, response = app.test_client.get("/")
    assert response.status == 503
    assert response.text == "handled: could not save the session"


def test_response_middleware_error_on_a_stream_before_it_starts(app):
    @app.on_response
    async def fail(request, response):
        raise Unauthorized("no")

    @app.get("/")
    async def handler(request):
        response = await request.respond(content_type="text/plain")
        await response.send("never")

    _, response = app.test_client.get("/")
    assert response.status == 401
    assert "never" not in response.text


def test_response_middleware_error_on_an_error_response_is_only_logged(app, caplog):
    @app.on_response
    async def fail(request, response):
        raise Exception("middleware failure")

    @app.get("/")
    async def handler(request):
        raise NotFound("missing")

    with caplog.at_level(logging.ERROR):
        _, response = app.test_client.get("/")
    assert response.status == 404
    assert (
        "oldman_server.error",
        logging.ERROR,
        "Exception occurred in one of response middleware handlers",
    ) in caplog.record_tuples


def test_an_error_handler_that_responds_itself_keeps_its_response(app):
    # The error handler sends with request.respond(): already the error
    # response, so a failing response middleware is only logged.
    @app.on_response
    async def fail(request, response):
        raise ValueError("middleware fails")

    @app.exception(Unauthorized)
    async def handle(request, exception):
        await request.respond(text("from the error handler", status=418))

    @app.get("/")
    async def handler(request):
        raise Unauthorized("no")

    _, response = app.test_client.get("/")
    assert response.status == 418
    assert response.text == "from the error handler"


def test_a_handler_may_catch_the_middleware_error_and_answer_otherwise(app):
    @app.on_response
    async def fail(request, response):
        raise ValueError("middleware fails")

    @app.get("/")
    async def handler(request):
        try:
            await request.respond(text("first"))
        except ValueError:
            return text("second", status=202)

    _, response = app.test_client.get("/")
    assert response.status == 202
    assert response.text == "second"
