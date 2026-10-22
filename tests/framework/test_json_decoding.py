from json import loads as sloads

import pytest


try:
    from ujson import loads as uloads

    NO_UJSON = False
    DEFAULT_LOADS = uloads
except ModuleNotFoundError:
    NO_UJSON = True
    DEFAULT_LOADS = sloads

from oldman_server import Request, OldmanServer, json


@pytest.fixture(autouse=True)
def default_back_to_ujson():
    yield
    Request._loads = DEFAULT_LOADS


def test_change_decoder():
    OldmanServer("Test", loads=sloads)
    assert Request._loads == sloads


def test_change_decoder_to_some_custom():
    def my_custom_decoder(some_str: str):
        dict = sloads(some_str)
        dict["some_key"] = "new_value"
        return dict

    app = OldmanServer("Test", loads=my_custom_decoder)
    assert Request._loads == my_custom_decoder

    req_body = {"some_key": "some_value"}

    @app.post("/test")
    def handler(request):
        new_json = request.json
        return json(new_json)

    req, res = app.test_client.post("/test", json=req_body)
    assert sloads(res.body) == {"some_key": "new_value"}


@pytest.mark.skipif(NO_UJSON is True, reason="ujson not installed")
def test_default_decoder():
    OldmanServer("Test")
    assert Request._loads == uloads


def test_big_integers_need_the_standard_library_loads():
    # The documented trade-off of the default (orjson): beyond 64 bits an
    # integer becomes a float; json.loads, which an application can pass as
    # loads=, keeps it exact.
    import json

    from oldman_server import OldmanServer
    from oldman_server.response import text

    body = b'{"id": 18446744073709551616}'
    # The default loads, named: an earlier app's loads= stays on the class.
    from orjson import loads as orjson_loads
    default = OldmanServer("BigIntDefault", loads=orjson_loads)

    @default.post("/")
    async def echo(request):
        return text(repr(request.json["id"]))

    _, response = default.test_client.post("/", content=body)
    assert response.text == repr(1.8446744073709552e19)

    exact = OldmanServer("BigIntExact", loads=json.loads)

    @exact.post("/")
    async def echo_exact(request):
        return text(repr(request.json["id"]))

    _, response = exact.test_client.post("/", content=body)
    assert response.text == "18446744073709551616"
