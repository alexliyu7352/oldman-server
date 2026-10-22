from dataclasses import dataclass
from json import dumps as sdumps

import orjson
import pytest

from oldman_server import OldmanServer
from oldman_server.response import BaseHTTPResponse, json, json_dumps


@dataclass
class Foo:
    bar: str


@pytest.fixture
def foo():
    return Foo(bar="bar")


@pytest.fixture
def payload(foo: Foo):
    return {"foo": foo}


@pytest.fixture(autouse=True)
def default_back_to_orjson():
    yield
    BaseHTTPResponse._dumps = json_dumps


def test_default_encoder_is_orjson():
    assert BaseHTTPResponse._dumps is json_dumps
    assert json_dumps({"a": [1, None]}) == orjson.dumps({"a": [1, None]})


def test_keys_that_are_not_str_become_strings():
    # As with the standard library (and ujson): orjson alone refuses them.
    assert json({200: "ok", 1.5: "x"}).body == b'{"200":"ok","1.5":"x"}'
    assert json_dumps({1: "a"}, option=orjson.OPT_SORT_KEYS) == b'{"1":"a"}'


def test_change_encoder():
    OldmanServer("Test", dumps=sdumps)
    assert BaseHTTPResponse._dumps == sdumps


def test_change_encoder_to_some_custom():
    def my_custom_encoder():
        return "foo"

    OldmanServer("Test", dumps=my_custom_encoder)
    assert BaseHTTPResponse._dumps == my_custom_encoder


def test_json_response_orjson(payload: dict[str, Foo]):
    """orjson encodes dataclasses; the standard library does not."""
    response = json(payload)
    assert response.body == b'{"foo":{"bar":"bar"}}'

    with pytest.raises(
        TypeError, match="Object of type Foo is not JSON serializable"
    ):
        json(payload, dumps=sdumps)

    OldmanServer("Test", dumps=sdumps)
    with pytest.raises(
        TypeError, match="Object of type Foo is not JSON serializable"
    ):
        json(payload)


def test_json_response_json():
    """orjson refuses integers over 64 bits; the standard library takes them."""
    too_big = 111111111111111111111

    with pytest.raises(TypeError, match="Integer exceeds 64-bit range"):
        json(too_big)

    response = json(too_big, dumps=sdumps)
    assert response.body == b"111111111111111111111"

    OldmanServer("Test", dumps=sdumps)
    response = json(too_big)
    assert response.body == b"111111111111111111111"


def test_json_writes_utf8_not_escapes():
    assert json({"é": "/x"}).body == '{"é":"/x"}'.encode()


def test_json_keyword_arguments_are_orjson_options():
    response = json({"b": 1, "a": 2}, option=orjson.OPT_SORT_KEYS)
    assert response.body == b'{"a":2,"b":1}'
