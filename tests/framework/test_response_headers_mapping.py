"""Response helpers take any Mapping of headers and copy it: the caller's
mapping is never changed, and later changes to it do not reach the response."""
from collections.abc import Mapping
from types import MappingProxyType

import pytest

from multidict import CIMultiDict

from oldman_server import OldmanServer
from oldman_server.compat import Header
from oldman_server.response import (
    ResponseStream,
    empty,
    file,
    file_stream,
    html,
    json,
    raw,
    redirect,
    text,
)


class Headers(Mapping):
    """A read-only Mapping that is neither a dict nor a multidict."""

    def __init__(self, items):
        self._items = dict(items)

    def __getitem__(self, key):
        return self._items[key]

    def __iter__(self):
        return iter(self._items)

    def __len__(self):
        return len(self._items)


HELPERS = {
    "text": lambda headers: text("x", headers=headers),
    "html": lambda headers: html("x", headers=headers),
    "json": lambda headers: json({}, headers=headers),
    "raw": lambda headers: raw(b"x", headers=headers),
    "redirect": lambda headers: redirect("/next", headers=headers),
    "empty": lambda headers: empty(headers=headers),
}

MAPPINGS = {
    "proxy": (lambda: MappingProxyType({"X-A": "1"}), ["1"]),
    "custom": (lambda: Headers({"X-A": "1"}), ["1"]),
    "multidict": (lambda: CIMultiDict([("X-A", "1"), ("X-A", "2")]), ["1", "2"]),
    "header": (lambda: Header([("X-A", "1"), ("X-A", "2")]), ["1", "2"]),
}


@pytest.mark.parametrize("helper", HELPERS)
@pytest.mark.parametrize("kind", MAPPINGS)
def test_helpers_take_any_mapping(helper, kind):
    make, values = MAPPINGS[kind]
    given = make()
    response = HELPERS[helper](given)
    assert response.headers.getall("x-a") == values
    response.headers["x-b"] = "changed"
    assert "x-b" not in given


@pytest.mark.parametrize("helper", HELPERS)
def test_helpers_leave_the_callers_dict_alone(helper):
    given = {"X-A": "1"}
    response = HELPERS[helper](given)
    response.headers["x-b"] = "2"
    assert given == {"X-A": "1"}


def test_redirect_reusing_one_headers_dict():
    shared = {"Cache-Control": "no-store"}
    first = redirect("/one", headers=shared)
    second = redirect("/two", headers=shared)
    assert shared == {"Cache-Control": "no-store"}
    assert first.headers["location"] == "/one"
    assert second.headers["location"] == "/two"


async def test_file_and_file_stream_copy_the_headers(tmp_path):
    path = tmp_path / "a.txt"
    path.write_text("hello")
    given = {"X-A": "1"}
    readonly = MappingProxyType({"X-A": "1"})
    for headers in (given, readonly):
        response = await file(path, headers=headers, filename="a.txt", max_age=10)
        assert response.headers["x-a"] == "1"
        assert "last-modified" in response.headers
        stream = await file_stream(path, headers=headers, filename="a.txt")
        assert stream.headers["x-a"] == "1"
        assert "content-disposition" in stream.headers
    assert given == {"X-A": "1"}


def test_response_stream_has_its_own_headers():
    async def streaming(response):
        await response.write("x")

    given = Header({"X-A": "1"})
    stream = ResponseStream(streaming, headers=given)
    stream.headers["x-b"] = "2"
    assert stream.headers is not given
    assert "x-b" not in given


def test_mappings_reach_the_client(app: OldmanServer):
    shared = MappingProxyType({"X-A": "1"})

    @app.get("/redirect")
    async def to_target(request):
        return redirect("/target", headers=shared)

    @app.get("/target")
    async def target(request):
        return text("target", headers=Headers({"X-B": "2"}))

    @app.get("/respond")
    async def respond(request):
        response = await request.respond(headers=shared)
        await response.send("streamed", True)

    _, response = app.test_client.get("/redirect", allow_redirects=False)
    assert response.status == 302
    assert response.headers["location"] == "/target"
    assert response.headers["x-a"] == "1"

    _, response = app.test_client.get("/target")
    assert response.headers["x-b"] == "2"

    _, response = app.test_client.get("/respond")
    assert response.text == "streamed"
    assert response.headers["x-a"] == "1"
    assert dict(shared) == {"X-A": "1"}
