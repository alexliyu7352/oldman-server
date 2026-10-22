"""request.headers on the native host is a NativeHeaders: lookups read the
native request, everything else a Header built once. Either way it answers
as the Header that the asyncio server's requests have."""
from collections.abc import Mapping

import pytest

from multidict import MultiMapping

from oldman_server.compat import Header, NativeHeaders
from oldman_server.response import json


HEADERS = (
    (b"Host", b"example.com"),
    (b"X-Multi", b"1"),
    (b"x-multi", b"2"),
    (b"Content-Type", b"text/plain; charset=utf-8"),
    (b"Cookie", b"name=caf\xc3\xa9"),
    (b"X-Raw", b"\xff"),  # not UTF-8: surrogateescape, as Header gets it
)


@pytest.fixture
def pair():
    try:
        from oldman_server.native.engine import ApplicationRequest
    except ImportError:  # the native extension needs scripts/env.sh
        pytest.skip("native extension not loadable")
    native = ApplicationRequest("GET", b"/", HEADERS, b"")
    return NativeHeaders(native), Header(native.sanic_headers())


LOOKUPS = [
    lambda h: h["host"],
    lambda h: h["X-MULTI"],
    lambda h: h.get("cookie"),
    lambda h: h.get("missing"),
    lambda h: h.get("missing", 5),
    lambda h: h.get("é"),
    lambda h: h.getone("x-multi"),
    lambda h: h.getone("missing", None),
    lambda h: h.getall("x-multi"),
    lambda h: h.getall("missing", []),
    lambda h: h.get_all("x-multi"),
    lambda h: h.get_all("missing"),
    lambda h: h.get("x-raw"),
    lambda h: "HOST" in h,
    lambda h: "missing" in h,
    lambda h: 1 in h,
    lambda h: "é" in h,
    lambda h: h.content_type,
    lambda h: h.x_multi,
    lambda h: h.missing_header,
]


@pytest.mark.parametrize("lookup", LOOKUPS)
def test_lookups_answer_as_a_header(pair, lookup):
    native, header = pair
    assert lookup(native) == lookup(header)
    assert native._header is None  # no Header was built


@pytest.mark.parametrize("lookup", [
    lambda h: h["missing"],
    lambda h: h.getone("missing"),
    lambda h: h.getall("missing"),
    lambda h: h.get(1),
])
def test_failed_lookups_raise_as_a_header(pair, lookup):
    native, header = pair
    with pytest.raises(Exception) as expected:
        lookup(header)
    with pytest.raises(type(expected.value)):
        lookup(native)


@pytest.mark.parametrize("operation", [
    list, len, bool, repr, dict,
    lambda h: list(h.items()),
    lambda h: list(h.keys()),
    lambda h: list(h.values()),
    lambda h: h.copy(),
])
def test_the_rest_answers_as_a_header(pair, operation):
    native, header = pair
    assert operation(native) == operation(header)


def test_equality(pair):
    native, header = pair
    assert native == header
    assert native == NativeHeaders(native._native)
    assert native != Header({"host": "other"})


def test_changes_go_to_a_header_that_later_lookups_see(pair):
    native, _ = pair
    native["X-Added"] = "yes"
    native.add("x-multi", "3")
    del native["cookie"]
    assert native["x-added"] == "yes"
    assert native.getall("x-multi") == ["1", "2", "3"]
    assert "cookie" not in native
    assert native.pop("x-added") == "yes"
    assert "x-added" not in native


def test_it_is_a_multimapping_not_a_cimultidict(pair):
    native, _ = pair
    assert isinstance(native, MultiMapping)
    assert isinstance(native, Mapping)


def test_handlers_see_the_same_headers_on_either_server(app):
    @app.post("/")
    async def echo(request):
        headers = request.headers
        return json({
            "host": headers["host"], "type": headers.get("content-type"),
            "multi": headers.getall("x-multi"), "attribute": headers.x_multi,
            "missing": headers.get("x-missing"), "has": "X-MULTI" in headers,
            "items": sorted(
                (k.lower(), v) for k, v in headers.items()
                if k.lower() in ("x-multi", "content-type")
            ),
        })

    _, response = app.test_client.post(
        "/", content=b"body",
        headers=[("Content-Type", "text/plain"), ("X-Multi", "1"), ("X-Multi", "2")],
    )
    assert response.json == {
        "host": response.json["host"], "type": "text/plain", "multi": ["1", "2"],
        "attribute": "1,2", "missing": None, "has": True,
        "items": [["content-type", "text/plain"], ["x-multi", "1"], ["x-multi", "2"]],
    }
    assert response.json["host"].startswith("127.0.0.1:")
