import json

from functools import partial
from unittest.mock import Mock

import pytest

from oldman_server import Request, OldmanServer
from oldman_server.exceptions import OldmanServerException
from oldman_server.response import json as json_response
from oldman_server.response.types import JSONResponse


JSON_BODY = {"ok": True}
json_dumps = partial(json.dumps, separators=(",", ":"))


@pytest.fixture
def json_app(app: OldmanServer):
    @app.get("/json")
    async def handle(request: Request):
        return json_response(JSON_BODY)

    return app


def test_body_can_be_retrieved(json_app: OldmanServer):
    _, resp = json_app.test_client.get("/json")
    assert resp.body == json_dumps(JSON_BODY).encode()


def test_body_can_be_set(json_app: OldmanServer):
    new_body = b'{"hello":"world"}'

    @json_app.on_response
    def set_body(request: Request, response: JSONResponse):
        response.body = new_body

    _, resp = json_app.test_client.get("/json")
    assert resp.body == new_body


def test_raw_body_can_be_retrieved(json_app: OldmanServer):
    @json_app.on_response
    def check_body(request: Request, response: JSONResponse):
        assert response.raw_body == JSON_BODY

    json_app.test_client.get("/json")


def test_raw_body_can_be_set(json_app: OldmanServer):
    new_body = {"hello": "world"}

    @json_app.on_response
    def set_body(request: Request, response: JSONResponse):
        response.raw_body = new_body
        assert response.raw_body == new_body
        assert response.body == json_dumps(new_body).encode()

    json_app.test_client.get("/json")


def test_raw_body_cant_be_retrieved_after_body_set(json_app: OldmanServer):
    new_body = b'{"hello":"world"}'

    @json_app.on_response
    def check_raw_body(request: Request, response: JSONResponse):
        response.body = new_body
        with pytest.raises(OldmanServerException):
            response.raw_body

    json_app.test_client.get("/json")


def test_raw_body_can_be_reset_after_body_set(json_app: OldmanServer):
    new_body = b'{"hello":"world"}'
    new_new_body = {"lorem": "ipsum"}

    @json_app.on_response
    def set_bodies(request: Request, response: JSONResponse):
        response.body = new_body
        response.raw_body = new_new_body

    _, resp = json_app.test_client.get("/json")
    assert resp.body == json_dumps(new_new_body).encode()


def test_set_body_method(json_app: OldmanServer):
    new_body = {"lorem": "ipsum"}

    @json_app.on_response
    def set_body(request: Request, response: JSONResponse):
        response.set_body(new_body)

    _, resp = json_app.test_client.get("/json")
    assert resp.body == json_dumps(new_body).encode()


def test_set_body_method_after_body_set(json_app: OldmanServer):
    new_body = b'{"hello":"world"}'
    new_new_body = {"lorem": "ipsum"}

    @json_app.on_response
    def set_body(request: Request, response: JSONResponse):
        response.body = new_body
        response.set_body(new_new_body)

    _, resp = json_app.test_client.get("/json")
    assert resp.body == json_dumps(new_new_body).encode()


def test_custom_dumps_and_kwargs(json_app: OldmanServer):
    custom_dumps = Mock(return_value="custom")

    @json_app.get("/json-custom")
    async def handle_custom(request: Request):
        return json_response(JSON_BODY, dumps=custom_dumps, prry="platypus")

    _, resp = json_app.test_client.get("/json-custom")
    assert resp.body == b"custom"
    custom_dumps.assert_called_once_with(JSON_BODY, prry="platypus")


def test_override_dumps_and_kwargs(json_app: OldmanServer):
    custom_dumps_1 = Mock(return_value="custom1")
    custom_dumps_2 = Mock(return_value="custom2")

    @json_app.get("/json-custom")
    async def handle_custom(request: Request):
        return json_response(JSON_BODY, dumps=custom_dumps_1, prry="platypus")

    @json_app.on_response
    def set_body(request: Request, response: JSONResponse):
        response.set_body(JSON_BODY, dumps=custom_dumps_2, platypus="prry")

    _, resp = json_app.test_client.get("/json-custom")

    assert resp.body == b"custom2"
    custom_dumps_1.assert_called_once_with(JSON_BODY, prry="platypus")
    custom_dumps_2.assert_called_once_with(JSON_BODY, platypus="prry")


def test_append(json_app: OldmanServer):
    @json_app.get("/json-append")
    async def handler_append(request: Request):
        return json_response(["a", "b"], status=200)

    @json_app.on_response
    def do_append(request: Request, response: JSONResponse):
        response.append("c")

    _, resp = json_app.test_client.get("/json-append")
    assert resp.body == json_dumps(["a", "b", "c"]).encode()


def test_extend(json_app: OldmanServer):
    @json_app.get("/json-extend")
    async def handler_extend(request: Request):
        return json_response(["a", "b"], status=200)

    @json_app.on_response
    def do_extend(request: Request, response: JSONResponse):
        response.extend(["c", "d"])

    _, resp = json_app.test_client.get("/json-extend")
    assert resp.body == json_dumps(["a", "b", "c", "d"]).encode()


def test_update(json_app: OldmanServer):
    @json_app.get("/json-update")
    async def handler_update(request: Request):
        return json_response({"a": "b"}, status=200)

    @json_app.on_response
    def do_update(request: Request, response: JSONResponse):
        response.update({"c": "d"}, e="f")

    _, resp = json_app.test_client.get("/json-update")
    assert resp.body == json_dumps({"a": "b", "c": "d", "e": "f"}).encode()


def test_pop_dict(json_app: OldmanServer):
    @json_app.get("/json-pop")
    async def handler_pop(request: Request):
        return json_response({"a": "b", "c": "d"}, status=200)

    @json_app.on_response
    def do_pop(request: Request, response: JSONResponse):
        val = response.pop("c")
        assert val == "d"

        val_default = response.pop("e", "f")
        assert val_default == "f"

    _, resp = json_app.test_client.get("/json-pop")
    assert resp.body == json_dumps({"a": "b"}).encode()


def test_pop_list(json_app: OldmanServer):
    @json_app.get("/json-pop")
    async def handler_pop(request: Request):
        return json_response(["a", "b"], status=200)

    @json_app.on_response
    def do_pop(request: Request, response: JSONResponse):
        val = response.pop(0)
        assert val == "a"

        with pytest.raises(
            TypeError, match="pop doesn't accept a default argument for lists"
        ):
            response.pop(21, "nah nah")

    _, resp = json_app.test_client.get("/json-pop")
    assert resp.body == json_dumps(["b"]).encode()


def test_json_response_class_sets_proper_content_type(json_app: OldmanServer):
    @json_app.get("/json-class")
    async def handler(request: Request):
        return JSONResponse(JSON_BODY)

    _, resp = json_app.test_client.get("/json-class")
    assert resp.headers["content-type"] == "application/json"


@pytest.mark.parametrize("dumped, body", [
    (bytearray(b"{}"), b"{}"), (memoryview(b"{}"), b"{}"), (None, b""),
    (type("Raw", (bytes,), {})(b"{}"), b"{}"), ("{}", b"{}"),
])
def test_custom_dumps_return_values_as_any_response_body(dumped, body):
    # As HTTPResponse's body setter takes them: str is encoded, None is
    # empty, and bytes-like values are used as they are.
    response = json_response({}, dumps=lambda value: dumped)
    assert bytes(response.body) == body


def test_a_bytes_like_body_is_sent(app: OldmanServer):
    @app.get("/")
    async def handler(request: Request):
        return json_response({}, dumps=lambda value: bytearray(b'{"a":1}'))

    _, response = app.test_client.get("/")
    assert response.status == 200 and response.body == b'{"a":1}'


def test_decimal_is_encoded_as_a_number(app: OldmanServer):
    """A Decimal (a database DECIMAL or NUMERIC column) is a JSON number,
    through float, as ujson encoded it; default= replaces that rule."""
    from decimal import Decimal

    @app.get("/number")
    async def number(request: Request):
        return json_response({"budget": Decimal("1234.50"), "n": [Decimal("1")]})

    @app.get("/text")
    async def as_text(request: Request):
        return json_response({"budget": Decimal("1234.50")}, default=str)

    _, response = app.test_client.get("/number")
    assert response.body == b'{"budget":1234.5,"n":[1.0]}'
    _, response = app.test_client.get("/text")
    assert response.body == b'{"budget":"1234.50"}'


def test_unknown_types_still_raise():
    from oldman_server.helpers import json_dumps

    with pytest.raises(TypeError):
        json_dumps({"value": object()})
