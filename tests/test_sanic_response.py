"""The buffered send boundary freezes encoded headers before application completion."""
import pytest
import uvloop
from oldman_server.response import HTTPResponse

from oldman_server.native.host import _BufferedExchange


def test_send_keeps_encoded_bytes_and_freezes_response():
    async def scenario():
        exchange = _BufferedExchange(b"", 64)
        response = HTTPResponse(b"before", status=201,
            headers={"X-Value": "café\udcff", "Content-Length": "6"}, content_type="text/plain")
        exchange.respond(response)
        await response.send()
        response.status, response.body = 202, b"after"
        response.headers["X-Value"] = "changed"
        status, body, headers = exchange.finish()
        assert (status, body) == (201, b"before")
        assert dict(headers) == {b"X-Value": b"caf\xc3\xa9\xff", b"content-type": b"text/plain"}
        assert all(type(name) is type(value) is bytes for name, value in headers)
        # Sanic's second finalization: the exchange no longer sends (Http.send
        # is None), so BaseHTTPResponse.send ignores it.
        assert exchange.send is None
        await response.send(end_stream=True)
        assert exchange.finish() == (status, body, headers)
    uvloop.run(scenario())


def test_send_preserves_header_getter_decode_hooks_and_mutable_values():
    calls = []
    value = bytearray(b"before\xff")
    class Encoded(bytes):
        def decode(self, encoding):
            calls.append(encoding)
            return "overridden"
    class Custom(HTTPResponse):
        @property
        def processed_headers(self):
            calls.append("getter")
            yield b"X-Mutable", value
            yield b"X-Custom", Encoded(b"original")
    async def scenario():
        exchange = _BufferedExchange(b"", 64)
        exchange.respond(Custom(b"ok"))
        await exchange.send(b"", end_stream=True)
        value[:] = b"after"
        assert exchange.finish() == (200, b"ok", (
            (b"X-Mutable", b"before\xff"), (b"X-Custom", b"overridden")))
        assert calls == ["getter", "latin-1"]
    uvloop.run(scenario())


def test_send_keeps_repeated_header_names_in_order():
    # The framework host writes each field line, e.g. several Set-Cookie headers.
    class Custom(HTTPResponse):
        @property
        def processed_headers(self):
            yield b"Set-Cookie", b"a=1"
            yield b"set-cookie", b"b=2"
    async def scenario():
        exchange = _BufferedExchange(b"", 64)
        exchange.respond(Custom(b"ok"))
        await exchange.send(b"", end_stream=True)
        assert exchange.finish() == (200, b"ok", ((b"Set-Cookie", b"a=1"), (b"set-cookie", b"b=2")))
    uvloop.run(scenario())


@pytest.mark.parametrize("headers,exception,seen_count", [
    ([(b"X-\xff", b"ok")], UnicodeDecodeError, 1),
    ([(b"X", b"v" * 8188)], ValueError, 1),
    ([(f"X-{i}".encode(), b"v") for i in range(130)], ValueError, 129),
])
def test_send_rejects_at_original_boundary_and_leaves_the_exchange(headers, exception, seen_count):
    # Nothing is handed over: the exchange is still the handler's, so the
    # error handler can answer instead; the same response is refused again,
    # and with no other response the exchange ends incomplete.
    from oldman_server.http import Stage
    seen = []
    class Custom(HTTPResponse):
        @property
        def processed_headers(self):
            for item in headers:
                seen.append(item)
                yield item
    async def scenario():
        exchange = _BufferedExchange(b"", 64)
        exchange.respond(Custom(b"ok"))
        with pytest.raises(exception):
            await exchange.send(b"", end_stream=True)
        assert len(seen) == seen_count
        assert exchange.stage is Stage.HANDLER and exchange.failure is None
        with pytest.raises(exception):
            await exchange.send(b"", end_stream=True)
        with pytest.raises(ValueError, match="incomplete response"):
            exchange.finish()
    uvloop.run(scenario())


def test_invalid_custom_decoder_result_is_left_to_native_validation():
    class Text(str):
        pass
    for invalid in (123, Text("value"), b"already bytes", "\u2603"):
        class Encoded(bytes):
            def decode(self, encoding):
                return invalid
        class Custom(HTTPResponse):
            @property
            def processed_headers(self):
                yield b"X", Encoded(b"value")
        async def scenario():
            exchange = _BufferedExchange(b"", 64)
            exchange.respond(Custom(b"ok"))
            await exchange.send(b"", end_stream=True)
            # The old str boundary rejected these after the application chain;
            # the native boundary does now. Do not invent an earlier exception.
            exchange.finish()
        uvloop.run(scenario())


@pytest.mark.parametrize("cumulative", [False, True])
def test_expanding_decoder_is_bounded_before_copy_without_ending_the_chain(cumulative):
    import tracemalloc
    events = []
    expanded = "a" * (4090 if cumulative else 2 * 1024 * 1024)
    class Encoded(bytes):
        def decode(self, encoding):
            events.append("decode")
            return expanded
    class Custom(HTTPResponse):
        @property
        def processed_headers(self):
            if cumulative:
                yield b"X-Base", b"b" * 4090
            yield b"X-Expand", Encoded(b"x")
            events.append("tail")
            yield b"X-Tail", Encoded(b"y")
    async def scenario():
        exchange = _BufferedExchange(b"", 64)
        exchange.respond(Custom(b"ok"))
        tracemalloc.start()
        try:
            await exchange.send(b"", end_stream=True)
            events.append("after_send")
            result = exchange.finish()
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        # Preexisting decoder strings are application-owned; adapting a short
        # value must not copy megabytes, or queue more than the header budget.
        assert peak < 128 * 1024
        assert sum(len(n) + len(v) + 4 for n, v in result[2]
                   if type(n) is type(v) is bytes) <= 8192
        assert events == ["decode", "tail", "decode", "after_send"]
    uvloop.run(scenario())


def test_subclass_listing_headers_in_its_slots_keeps_lazy_headers():
    # Written for Sanic, where headers is a slot (sanic-ext's LazyResponse).
    class Slotted(HTTPResponse):
        __slots__ = ("body", "status", "content_type", "headers", "_cookies", "context")

        def __init__(self, context, headers=None):
            super().__init__(b"x", headers=headers, content_type="text/plain")
            self.context = context

    plain, given = Slotted({}), Slotted({}, headers={"x-a": "1"})
    assert dict(plain.headers) == {} and given.headers["x-a"] == "1"
    plain.headers["x-b"] = "2"
    assert (b"x-b", b"2") in list(plain.processed_headers)
    plain.add_cookie("c", "v")
    assert "c=v" in str(plain.headers["set-cookie"])
