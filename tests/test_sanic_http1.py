"""HTTP/1.1 wire behavior of the framework host, over raw sockets."""
import asyncio
import dataclasses
import os
from http import HTTPStatus
from pathlib import Path

from oldman_server import OldmanServer
from oldman_server.response import HTTPResponse, empty, file_stream, json, raw, text
import pytest
import uvloop

from test_http import config


def serve():
    from oldman_server.native.host import serve
    return serve


def make_app(name):
    app = OldmanServer(name, configure_logging=False)
    app.config.ACCESS_LOG = False

    @app.route("/small", methods=["GET", "HEAD"])
    async def small(request):
        return json({"message": "Hello, World!"})

    @app.route("/echo", methods=["POST", "PUT"])
    async def echo(request):
        return raw(request.body, content_type="application/octet-stream")

    @app.get("/empty")
    async def nothing(request):
        return empty()

    @app.get("/notmodified")
    async def not_modified(request):
        return empty(status=304)

    @app.get("/cookies")
    async def cookies(request):
        response = text("cookies")
        response.add_cookie("a", "1")
        response.add_cookie("b", "2")
        response.headers.add("Vary", "Accept")
        response.headers.add("Vary", "Origin")
        return response

    @app.get("/version")
    async def version(request):
        return text(request.version)

    @app.get("/large")
    async def large(request):
        return raw(bytes(range(256)) * 400)  # 102400 bytes, written as its own buffer

    @app.route("/stream", methods=["GET", "HEAD"])
    async def stream(request):
        response = await request.respond(content_type="text/plain")
        for part in (b"alpha,", b"beta,", b"gamma"):
            await response.send(part)
        await response.eof()

    @app.get("/stream-length")
    async def stream_length(request):
        response = await request.respond(headers={"Content-Length": "11"})
        await response.send(b"hello ")
        await response.send(b"world")
        await response.eof()

    @app.get("/stream-big")
    async def stream_big(request):
        response = await request.respond()
        for index in range(12):  # 12 x 32 KiB: well past the 64 KiB window
            await response.send(bytes([index]) * 32768)
        await response.eof()

    @app.get("/stream-fail")
    async def stream_fail(request):
        response = await request.respond()
        await response.send(b"partial")
        raise RuntimeError("fails mid-stream")

    @app.get("/stream-forever")
    async def stream_forever(request):
        response = await request.respond()
        try:
            while True:
                await response.send(b"tick\n")
                await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            app.ctx.cancelled = True
            raise

    @app.get("/stream-file")
    async def stream_file(request):
        return await file_stream(__file__, chunk_size=1000)

    @app.post("/streamed-in", stream=True)
    async def streamed_in(request):
        parts = []
        while True:
            body = await request.stream.read()
            if body is None:
                break
            parts.append(body)
        return text(f"{len(parts)}:{b''.join(parts).decode()}")

    @app.post("/slow-body", stream=True)
    async def slow_body(request):
        try:
            parts = [part async for part in request.stream]
        except asyncio.CancelledError:
            app.ctx.body_cancelled = True
            raise
        return text(f"{len(parts)}:{b''.join(parts).decode()}")

    @app.get("/body-cancelled")
    async def body_cancelled(request):
        return text(str(getattr(app.ctx, "body_cancelled", False)))

    @app.get("/cancelled")
    async def cancelled(request):
        return text(str(getattr(app.ctx, "cancelled", False)))

    @app.get("/informational")
    async def informational(request):
        # Not a final status: the framework raises RuntimeError("Invalid response status").
        kind = request.args.get("kind")
        if kind == "stream":
            response = await request.respond(status=101, headers={"Upgrade": "x"})
            await response.send(b"data")
            return
        if kind == "enum":  # an int subclass is a valid status
            return HTTPResponse(b"created", status=HTTPStatus.CREATED)
        return HTTPResponse(status=int(request.args.get("status", 101)),
                            headers={"Upgrade": "x"} if kind == "headers" else None)
    return app


async def read_response(reader):
    """One response: status, ordered header pairs, body (Content-Length framing)."""
    head = await reader.readuntil(b"\r\n\r\n")
    lines = head.decode("latin-1").split("\r\n")
    status = int(lines[0].split(" ")[1])
    headers = [tuple(line.split(": ", 1)) for line in lines[1:] if line]
    lengths = [int(v) for k, v in headers if k.lower() == "content-length"]
    body = await reader.readexactly(lengths[0]) if lengths else b""
    return status, headers, body


async def read_chunked(reader):
    """One chunked response: status, headers and the decoded body."""
    head = await reader.readuntil(b"\r\n\r\n")
    lines = head.decode("latin-1").split("\r\n")
    headers = [tuple(line.split(": ", 1)) for line in lines[1:] if line]
    assert ("Transfer-Encoding", "chunked") in headers
    body = b""
    while True:
        size = int((await reader.readuntil(b"\r\n"))[:-2], 16)
        data = await reader.readexactly(size + 2)
        if size == 0:
            return int(lines[0].split(" ")[1]), headers, body
        body += data[:-2]


def run(name, *scenarios, app_factory=None, backend=None, **overrides):
    """Each scenario gets its own connection; one native runtime per test.
    backend: the shards' I/O (default epoll)."""
    async def main():
        app = (app_factory or make_app)(name)
        cfg = config(application_dispatch_all=True, **overrides)
        if backend is not None:
            cfg = dataclasses.replace(cfg, reactor_backend=backend)
        # As native_config maps it: the host's body limit is REQUEST_MAX_SIZE.
        app.config.REQUEST_MAX_SIZE = cfg.server.max_body_bytes
        results = []
        async with serve()(app, cfg):
            for scenario in scenarios:
                reader, writer = await asyncio.open_connection("127.0.0.1", cfg.server.http_port)
                try:
                    results.append(await scenario(reader, writer))
                finally:
                    writer.close()
        return results
    return uvloop.run(main())


def names(headers):
    return [k.lower() for k, _ in headers]


def test_keep_alive_pipelining_and_content_length_framing():
    async def scenario(reader, writer):
        writer.write(b"GET /small HTTP/1.1\r\nHost: x\r\n\r\n" * 3 +
                     b"POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhello")
        replies = [await read_response(reader) for _ in range(4)]
        for status, headers, body in replies[:3]:
            assert (status, body) == (200, b'{"message":"Hello, World!"}')
            # The framework's header set: no Date; Connection on every response.
            assert "transfer-encoding" not in names(headers) and "date" not in names(headers)
            assert ("Connection", "keep-alive") in headers
            assert ("content-type", "application/json") in [(k.lower(), v) for k, v in headers]
        assert replies[3][0::2] == (200, b"hello")
    run("http1_pipeline", scenario)


def test_head_and_bodyless_statuses():
    async def scenario(reader, writer):
        writer.write(b"HEAD /small HTTP/1.1\r\nHost: x\r\n\r\n")
        head = await reader.readuntil(b"\r\n\r\n")
        assert head.startswith(b"HTTP/1.1 ") and b"Content-Length: 27\r\n" in head
        writer.write(b"GET /empty HTTP/1.1\r\nHost: x\r\n\r\nGET /notmodified HTTP/1.1\r\nHost: x\r\n\r\n")
        for expected in (204, 304):
            status, headers, body = await read_response(reader)
            assert status == expected and body == b""
            assert "content-length" not in names(headers) and "transfer-encoding" not in names(headers)
        # The connection is still usable: nothing was left unread after HEAD.
        writer.write(b"GET /small HTTP/1.1\r\nHost: x\r\n\r\n")
        assert (await read_response(reader))[0] == 200
    run("http1_bodyless", scenario)


def test_repeated_response_headers_are_preserved():
    async def scenario(reader, writer):
        writer.write(b"GET /cookies HTTP/1.1\r\nHost: x\r\n\r\n")
        status, headers, body = await read_response(reader)
        assert (status, body) == (200, b"cookies")
        cookies = [v for k, v in headers if k.lower() == "set-cookie"]
        assert len(cookies) == 2 and cookies[0].startswith("a=1") and cookies[1].startswith("b=2")
        assert [v for k, v in headers if k.lower() == "vary"] == ["Accept", "Origin"]
    run("http1_repeated", scenario)


def test_connection_close_and_http10():
    async def scenario(reader, writer):
        writer.write(b"GET /version HTTP/1.0\r\n\r\n")
        status, headers, body = await read_response(reader)
        assert (status, body) == (200, b"1.0") and ("Connection", "close") in headers
        assert await reader.read() == b""

    async def keep(reader, writer):
        writer.write(b"GET /version HTTP/1.0\r\nConnection: keep-alive\r\n\r\n"
                     b"GET /small HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        assert (await read_response(reader))[0::2] == (200, b"1.0")
        status, headers, _ = await read_response(reader)
        assert status == 200 and ("Connection", "close") in headers
        assert await reader.read() == b""
    run("http1_close", scenario, keep)


def test_expect_continue():
    async def scenario(reader, writer):
        writer.write(b"PUT /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\nExpect: 100-continue\r\n\r\n")
        assert await reader.readuntil(b"\r\n\r\n") == b"HTTP/1.1 100 Continue\r\n\r\n"
        writer.write(b"data")
        assert (await read_response(reader))[0::2] == (200, b"data")
    run("http1_continue", scenario)


def test_malformed_and_limited_requests_close_with_errors():
    # Statuses and messages of the framework's Http for the same input, rendered by the
    # application's error handler.
    cases = [
        (b"GET /small\r\n\r\n", 400, b"Bad Request"),
        (b"GET /small HTTP/2.0\r\n\r\n", 400, b"Bad Request"),
        (b"GET /small HTTP/1.1\r\nBad Header: x\r\n\r\n", 400, b"Bad Request"),
        (b"GET /small HTTP/1.1\r\n folded: x\r\n\r\n", 400, b"Bad Request"),
        (b"POST /echo HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\na", 400, b"Bad Request"),
        (b"POST /echo HTTP/1.1\r\nContent-Length: +1\r\n\r\na", 400, b"Bad content-length"),
        (b"GET /caf\xc3\xa9 HTTP/1.1\r\n\r\n", 400, b"URL may only contain US-ASCII characters."),
        (b"POST /echo HTTP/1.1\r\nContent-Length: 1\r\nExpect: nothing\r\n\r\na", 417,
         b"Unknown Expect: nothing"),
        (b"POST /echo HTTP/1.1\r\nContent-Length: 99999999\r\n\r\n", 413,
         b"Request body exceeds the size limit"),
        (b"GET /small HTTP/1.1\r\nX-Big: " + b"a" * 9000 + b"\r\n\r\n", 413,
         b"Request header exceeds the size limit"),
    ]
    def scenario(request):
        async def check(reader, writer):
            writer.write(request)
            status, headers, body = await read_response(reader)
            assert ("Connection", "close") in headers
            assert await reader.read() == b""
            return status, body
        return check
    results = run("http1_errors", *(scenario(request) for request, _, _ in cases))
    for (status, body), (_, expected, message) in zip(results, cases):
        assert status == expected and message in body, (status, body)


def test_keep_alive_idle_and_request_deadlines():
    async def idle(reader, writer):
        writer.write(b"GET /small HTTP/1.1\r\nHost: x\r\n\r\n")
        assert (await read_response(reader))[0] == 200
        assert await asyncio.wait_for(reader.read(), 2) == b""  # closed after ~0.2 s idle

    async def slow_head(reader, writer):
        writer.write(b"GET /small HTTP/1.1\r\nHost:")
        status, headers, body = await asyncio.wait_for(read_response(reader), 2)
        # RequestTimeout through the error handler, as on the asyncio server.
        assert status == 408 and ("Connection", "close") in headers and b"Request Timeout" in body
        assert await reader.read() == b""

    async def slow_body(reader, writer):
        # While the handler runs, the asyncio server times the body by RESPONSE_TIMEOUT
        # (from the last data received), not REQUEST_TIMEOUT.
        writer.write(b"POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 10\r\n\r\nabc")
        status, _, body = await asyncio.wait_for(read_response(reader), 2)
        assert status == 503 and b"Response Timeout" in body

    async def progressing_body(reader, writer):
        # A body that keeps arriving outlasts both deadlines (it took a 408
        # once REQUEST_TIMEOUT passed since the request began).
        writer.write(b"POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 12\r\n\r\n")
        for _ in range(12):
            await asyncio.sleep(0.1)
            writer.write(b"x")
        return (await asyncio.wait_for(read_response(reader), 2))[0::2]

    *_, progressed = run("http1_deadlines", idle, slow_head, slow_body, progressing_body,
                         keep_alive_timeout_ms=200, request_timeout_ms=300, decision_timeout_ms=400)
    assert progressed == (200, b"x" * 12)


def test_sanic_timeouts_map_to_the_host():
    # 0 meant "no deadline" to the host but "at the first check" (0.1 s) to
    # the asyncio server; values past a day or infinite made the server fail to start.
    from oldman_server.native.host import native_config
    app = OldmanServer("timeout_mapping", configure_logging=False)
    app.config.KEEP_ALIVE_TIMEOUT, app.config.REQUEST_TIMEOUT = 0, 0.05
    app.config.RESPONSE_TIMEOUT, app.config.GRACEFUL_TCP_CLOSE_TIMEOUT = 0, 0
    server = native_config(app).server
    assert (server.keep_alive_timeout_ms, server.request_timeout_ms, server.decision_timeout_ms,
            server.write_timeout_ms) == (100, 100, 100, 100)
    app.config.KEEP_ALIVE_TIMEOUT, app.config.REQUEST_TIMEOUT = float("inf"), 200_000
    app.config.RESPONSE_TIMEOUT = float("inf")
    server = native_config(app).server
    assert (server.keep_alive_timeout_ms, server.request_timeout_ms, server.decision_timeout_ms,
            server.write_timeout_ms) == (0, 86_400_000, 86_400_000, 0)


def test_keep_alive_disabled_closes_after_each_response():
    async def scenario(reader, writer):
        writer.write(b"GET /small HTTP/1.1\r\nHost: x\r\n\r\n")
        status, headers, _ = await read_response(reader)
        assert status == 200 and ("Connection", "close") in headers
        assert await reader.read() == b""
    run("http1_no_keepalive", scenario, keep_alive=False)


def test_large_response_body_is_complete():
    async def scenario(reader, writer):
        writer.write(b"GET /large HTTP/1.1\r\nHost: x\r\n\r\nGET /small HTTP/1.1\r\nHost: x\r\n\r\n")
        status, headers, body = await read_response(reader)
        assert status == 200 and body == bytes(range(256)) * 400
        assert (await read_response(reader))[0::2] == (200, b'{"message":"Hello, World!"}')
    run("http1_large", scenario, max_response_bytes=1 << 20)


def test_streamed_responses():
    async def chunked(reader, writer):
        writer.write(b"GET /stream HTTP/1.1\r\nHost: x\r\n\r\n")
        status, headers, body = await read_chunked(reader)
        assert (status, body) == (200, b"alpha,beta,gamma")
        assert ("content-type", "text/plain") in headers
        # The connection stays usable after a stream.
        writer.write(b"GET /small HTTP/1.1\r\nHost: x\r\n\r\n")
        assert (await read_response(reader))[0] == 200

    async def length(reader, writer):
        writer.write(b"GET /stream-length HTTP/1.1\r\nHost: x\r\n\r\n")
        status, headers, body = await read_response(reader)
        assert (status, body) == (200, b"hello world") and "transfer-encoding" not in names(headers)

    async def http10(reader, writer):
        writer.write(b"GET /stream HTTP/1.0\r\n\r\n")
        data = await asyncio.wait_for(reader.read(), 2)
        head, body = data.split(b"\r\n\r\n", 1)
        assert b"Connection: close" in head and b"Transfer-Encoding" not in head
        assert body == b"alpha,beta,gamma"

    async def head(reader, writer):
        writer.write(b"HEAD /stream HTTP/1.1\r\nHost: x\r\n\r\nGET /small HTTP/1.1\r\nHost: x\r\n\r\n")
        response_head = await reader.readuntil(b"\r\n\r\n")
        assert response_head.startswith(b"HTTP/1.1 200")
        assert (await read_response(reader))[0::2] == (200, b'{"message":"Hello, World!"}')

    async def big(reader, writer):
        writer.write(b"GET /stream-big HTTP/1.1\r\nHost: x\r\n\r\n")
        status, _, body = await asyncio.wait_for(read_chunked(reader), 5)
        assert status == 200 and body == b"".join(bytes([i]) * 32768 for i in range(12))

    async def file(reader, writer):
        writer.write(b"GET /stream-file HTTP/1.1\r\nHost: x\r\n\r\n")
        status, _, body = await asyncio.wait_for(read_chunked(reader), 5)
        assert status == 200 and Path(__file__).read_bytes() == body

    run("http1_streams", chunked, length, http10, head, big, file, max_response_bytes=1 << 20)


def test_stream_failure_ends_the_stream_and_disconnect_cancels_handler():
    async def failure(reader, writer):
        # Like the framework's Http, an error after the response started is only logged;
        # the stream then ends normally and the connection stays usable.
        writer.write(b"GET /stream-fail HTTP/1.1\r\nHost: x\r\n\r\n")
        status, _, body = await asyncio.wait_for(read_chunked(reader), 2)
        assert (status, body) == (200, b"partial")
        writer.write(b"GET /small HTTP/1.1\r\nHost: x\r\n\r\n")
        assert (await read_response(reader))[0] == 200

    async def disconnect(reader, writer):
        writer.write(b"GET /stream-forever HTTP/1.1\r\nHost: x\r\n\r\n")
        await reader.readuntil(b"tick")
        writer.close()
        await asyncio.sleep(0.5)

    async def check(reader, writer):
        writer.write(b"GET /cancelled HTTP/1.1\r\nHost: x\r\n\r\n")
        return (await read_response(reader))[2]

    assert run("http1_stream_failures", failure, disconnect, check)[2] == b"True"


def test_chunked_request_bodies_and_stream_handlers():
    async def chunked(reader, writer):
        writer.write(b"POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"
                     b"5;ext=1\r\nhello\r\n")
        await asyncio.sleep(0.05)  # the rest arrives later
        writer.write(b"6\r\n world\r\n0\r\n\r\n"
                     b"GET /small HTTP/1.1\r\nHost: x\r\n\r\n")
        assert (await read_response(reader))[0::2] == (200, b"hello world")
        assert (await read_response(reader))[0] == 200

    async def stream_handler(reader, writer):
        writer.write(b"POST /streamed-in HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\n\r\ndata")
        assert (await read_response(reader))[0::2] == (200, b"1:data")

    async def chunks_as_reads(reader, writer):
        # A stream handler reads each chunk on its own, as with the framework's Http.
        writer.write(b"POST /streamed-in HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"
                     b"3\r\nfoo\r\n3\r\nbar\r\n0\r\n\r\n")
        assert (await read_response(reader))[0::2] == (200, b"2:foobar")

    def failing(request, expected):
        async def check(reader, writer):
            writer.write(request)
            status, headers, body = await read_response(reader)
            assert ("Connection", "close") in headers
            return status, body
        return check

    results = run("http1_chunked_in", chunked, stream_handler, chunks_as_reads,
                  failing(b"POST /echo HTTP/1.1\r\nTransfer-Encoding: gzip\r\n\r\n", 400),
                  failing(b"POST /echo HTTP/1.1\r\nTransfer-Encoding: chunked\r\nContent-Length: 3\r\n\r\n", 400),
                  failing(b"POST /echo HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n", 400),
                  failing(b"POST /echo HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n"
                          b"1\r\na\r\n0\r\nX-Trailer: t\r\n\r\n", 400),
                  failing(b"POST /echo HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n10000\r\n", 413),
                  max_body_bytes=1024)
    messages = [b"Bad content-length", b"Bad Request", b"Bad chunked encoding",
                b"Bad chunked encoding", b"Request body exceeds the size limit"]
    assert [status for status, _ in results[3:]] == [400, 400, 400, 400, 413]
    for (_, body), message in zip(results[3:], messages):
        assert message in body, body


def test_request_bodies_arriving_while_the_handler_runs():
    async def pieces_to_plain_route(reader, writer):
        writer.write(b"POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 10\r\n\r\nabc")
        await asyncio.sleep(0.05)
        writer.write(b"defg")
        await asyncio.sleep(0.05)
        writer.write(b"hij")
        assert (await read_response(reader))[0::2] == (200, b"abcdefghij")

    async def pieces_to_stream_route(reader, writer):
        writer.write(b"POST /slow-body HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"
                     b"3\r\nabc\r\n")
        await asyncio.sleep(0.05)
        writer.write(b"2\r\nde\r\n0\r\n\r\n")
        status, _, body = await read_response(reader)
        count, data = body.decode().split(":")
        assert status == 200 and data == "abcde" and int(count) >= 2  # read as it arrived

    async def disconnect_mid_body(reader, writer):
        writer.write(b"POST /slow-body HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\nabc")
        await asyncio.sleep(0.1)
        writer.close()
        await asyncio.sleep(0.3)

    async def check(reader, writer):
        writer.write(b"GET /body-cancelled HTTP/1.1\r\nHost: x\r\n\r\n")
        return (await read_response(reader))[2]

    results = run("http1_body_stream", pieces_to_plain_route, pieces_to_stream_route,
                  disconnect_mid_body, check)
    assert results[3] == b"True"


def sanic_semantics_app(name):
    """Routes whose behavior the framework defines beyond the wire format."""
    from oldman_server.exceptions import PayloadTooLarge
    from oldman_server.response import HTTPResponse
    app = make_app(name)
    app.config.RESPONSE_TIMEOUT = 0.2

    @app.exception(PayloadTooLarge)
    async def too_large(request, exception):
        return text(f"custom: {exception}", status=413)

    @app.get("/sleep")
    async def sleep(request):
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            app.ctx.sleep_cancelled = True
            raise
        return text("late")

    @app.get("/sleep-cancelled")
    async def sleep_cancelled(request):
        return text(str(getattr(app.ctx, "sleep_cancelled", False)))

    @app.get("/raise-cancel")
    async def raise_cancel(request):
        raise asyncio.CancelledError("handler")

    @app.exception(asyncio.CancelledError)
    async def on_cancel(request, exception):
        return text("cancel handled", status=418)

    @app.route("/declared", methods=["GET", "HEAD"])
    async def declared(request):
        if request.method == "HEAD":
            return HTTPResponse(headers={"Content-Length": "1234"})
        return raw(b"short", headers={"Content-Length": "99"})

    @app.get("/connection")
    async def connection(request):
        ctx = request.conn_info.ctx
        ctx.count = getattr(ctx, "count", 0) + 1
        return text(f"{ctx.count}:{request.protocol.state['requests_count']}")
    return app


def test_sanic_error_handling_timeouts_and_connection_state():
    from oldman_server.models.server_types import native_connections

    async def custom_error_handler(reader, writer):
        writer.write(b"POST /echo HTTP/1.1\r\nContent-Length: 99999999\r\n\r\n")
        status, _, body = await read_response(reader)
        return status, body

    async def response_timeout(reader, writer):
        writer.write(b"GET /sleep HTTP/1.1\r\nHost: x\r\n\r\n")
        status, headers, body = await asyncio.wait_for(read_response(reader), 3)
        assert ("Connection", "close") in headers
        return status, b"Response Timeout" in body

    async def cancelled_by_timeout(reader, writer):
        writer.write(b"GET /sleep-cancelled HTTP/1.1\r\nHost: x\r\n\r\n")
        return (await read_response(reader))[2]

    async def raised_cancel(reader, writer):
        writer.write(b"GET /raise-cancel HTTP/1.1\r\nHost: x\r\n\r\n")
        return (await read_response(reader))[0::2]

    async def declared_lengths(reader, writer):
        writer.write(b"HEAD /declared HTTP/1.1\r\nHost: x\r\n\r\n")
        head = await reader.readuntil(b"\r\n\r\n")  # HEAD keeps its own length
        writer.write(b"GET /declared HTTP/1.1\r\nHost: x\r\n\r\n")
        return b"Content-Length: 1234\r\n" in head, (await read_response(reader))[0::2]

    async def connection_state(reader, writer):
        bodies = []
        for _ in range(3):
            writer.write(b"GET /connection HTTP/1.1\r\nHost: x\r\n\r\n")
            bodies.append((await read_response(reader))[2])
        return bodies

    async def released(reader, writer):
        await asyncio.sleep(0.2)  # the earlier connections' close events
        return len(native_connections)

    results = run("http1_sanic_semantics", custom_error_handler, response_timeout,
                  cancelled_by_timeout, raised_cancel, declared_lengths, connection_state,
                  released, app_factory=sanic_semantics_app)
    assert results[0] == (413, b"custom: Request body exceeds the size limit")
    assert results[1] == (503, True)
    assert results[2] == b"True"
    assert results[3] == (418, b"cancel handled")
    assert results[4] == (True, (200, b"short"))
    assert results[5] == [b"1:1", b"2:2", b"3:3"]
    assert results[6] == 0  # only connections Python still keeps state for


def test_informational_status_from_a_handler_is_an_error():
    # Only the WebSocket handshake sends a 101; a handler's 1xx response goes
    # to the error handler (500), as RuntimeError in Http.http1_response_header.
    def request(query):
        async def scenario(reader, writer):
            writer.write(f"GET /informational?{query} HTTP/1.1\r\nHost: x\r\n\r\n".encode())
            return (await reader.readuntil(b"\r\n")).decode().split(" ")[1]
        return scenario
    queries = ("kind=plain", "kind=headers", "kind=stream", "status=102")
    statuses = run("http1_informational", *(request(query) for query in (*queries, "kind=enum")))
    assert statuses == ["500"] * 4 + ["201"]
    # The native boundary holds too, without the adapter's check: the host
    # answers an empty 500, and a 101 stream start still opens a tunnel.
    from oldman_server.native import host
    check, host._final_status = host._final_status, lambda status: status
    try:
        statuses = run("http1_informational_native", *(request(query) for query in queries))
    finally:
        host._final_status = check
    assert statuses == ["500", "500", "101", "500"]


def unsendable_app(name, request_class=None):
    """Responses the native host refuses before anything is sent, and the
    error handler that answers instead."""
    app = OldmanServer(name, request_class=request_class, configure_logging=False)
    app.config.ACCESS_LOG = False

    @app.exception(Exception)
    async def handled(request, exception):
        return text(f"handled {type(exception).__name__}: {exception}", status=500)

    @app.get("/small")
    async def small(request):
        return text("small")

    @app.get("/many-headers")
    async def many_headers(request):
        return text("x", headers={f"x-{i}": "v" for i in range(129)})

    @app.get("/big-header")
    async def big_header(request):
        return text("x", headers={"x-big": "v" * 9000})

    @app.get("/status99")
    async def status99(request):
        return HTTPResponse(b"x", status=99)

    @app.get("/oversize")
    async def oversize(request):
        return raw(b"y" * 5000)

    @app.get("/stream-headers")
    async def stream_headers(request):
        response = await request.respond(headers={f"x-{i}": "v" for i in range(200)})
        await response.send(b"a")

    @app.get("/stream-oversize")
    async def stream_oversize(request):
        response = await request.respond(content_type="text/plain")
        await response.send(b"ok")
        await response.send(b"y" * 5000)
    return app


@pytest.mark.parametrize("path", ["flat", "full"])
def test_a_response_the_host_cannot_send_goes_to_the_error_handler(path):
    # Over the header limit, an invalid status, a body over
    # NATIVE_MAX_RESPONSE_SIZE, too many headers at a stream's start: nothing
    # is sent yet, so the error handler answers and the connection stays
    # usable. Once a stream has started, a failure can only end it.
    from oldman_server.request import Request

    class Custom(Request):
        pass

    factory = ((lambda name: unsendable_app(name)) if path == "flat"
               else (lambda name: unsendable_app(name, Custom)))

    def refused(target, message):
        async def scenario(reader, writer):
            writer.write(f"GET {target} HTTP/1.1\r\nHost: x\r\n\r\n"
                         "GET /small HTTP/1.1\r\nHost: x\r\n\r\n".encode())
            first = await read_response(reader)
            second = await read_response(reader)
            return first[0], first[2], second[0], second[2], message
        return scenario

    async def started(reader, writer):
        writer.write(b"GET /stream-oversize HTTP/1.1\r\nHost: x\r\n\r\n")
        head = await reader.readuntil(b"\r\n\r\n")
        rest = await reader.read()  # the connection ends after what was sent
        return int(head.split(b" ")[1]), rest

    results = run(f"http1_unsendable_{path}",
                  refused("/many-headers", b"handled ValueError: response headers exceed the native limit"),
                  refused("/big-header", b"handled ValueError: response headers exceed the native limit"),
                  refused("/status99", b"handled RuntimeError: Invalid response status 99"),
                  refused("/oversize", b"handled ValueError: response body exceeds the native limit"),
                  refused("/stream-headers", b"handled ValueError: response headers exceed the native limit"),
                  started, app_factory=factory, max_response_bytes=1000)
    for status, body, next_status, next_body, message in results[:5]:
        assert (status, body) == (500, message)
        assert (next_status, next_body) == (200, b"small")
    status, rest = results[5]
    assert status == 200 and b"ok" in rest and not rest.endswith(b"0\r\n\r\n")


def test_a_request_subclass_has_the_request_head():
    # A custom request class takes the general path: the same head as the
    # exact class (rebuilt from the parsed request line and fields).
    from oldman_server.request import Request

    class Custom(Request):
        pass

    def factory(name):
        app = OldmanServer(name, request_class=Custom, configure_logging=False)
        app.config.ACCESS_LOG = False

        @app.get("/head")
        async def head(request):
            return json({"class": type(request).__name__, "head": request.head.decode(),
                         "line": request.request_line.decode(),
                         "raw": request.raw_headers.decode()})
        return app

    async def scenario(reader, writer):
        writer.write(b"GET /head?a=1 HTTP/1.1\r\nHost: x\r\nX-A: 1\r\n\r\n")
        return await read_response(reader)

    (status, _, body), = run("http1_request_subclass", scenario, app_factory=factory)
    import json as json_module
    assert status == 200 and json_module.loads(body) == {
        "class": "Custom", "head": "GET /head?a=1 HTTP/1.1\r\nHost: x\r\nX-A: 1",
        "line": "GET /head?a=1 HTTP/1.1", "raw": "Host: x\r\nX-A: 1"}


def test_blank_lines_before_a_request_count_towards_the_head_limit():
    # RFC 9112 2.2 lets a server skip empty lines before a request line; they
    # count towards REQUEST_MAX_HEADER_SIZE like the head itself, or a client
    # sending CRLF without end filled the buffer and kept the shard rescanning.
    async def few(reader, writer):
        writer.write(b"\r\n\r\nGET /small HTTP/1.1\r\nHost: x\r\n\r\n")
        return (await read_response(reader))[0]

    async def many_then_request(reader, writer):
        writer.write(b"\r\n" * 5000 + b"GET /small HTTP/1.1\r\nHost: x\r\n\r\n")
        status, headers, _ = await read_response(reader)
        return status, ("Connection", "close") in headers

    async def only_blank_lines(reader, writer):
        for _ in range(9):
            writer.write(b"\r\n" * 512)
            await writer.drain()
        status, _, _ = await asyncio.wait_for(read_response(reader), 5)
        return status, await asyncio.wait_for(reader.read(), 5)

    results = run("http1_blank_lines", few, many_then_request, only_blank_lines)
    assert results == [200, (413, True), (413, b"")]


def test_many_short_header_fields_within_the_size_limit():
    # Only the head's size is limited, as in the framework's Http: 150 short fields fit.
    async def scenario(reader, writer):
        fields = b"".join(b"X-%d: v\r\n" % index for index in range(150))
        writer.write(b"GET /small HTTP/1.1\r\nHost: x\r\n" + fields + b"\r\n")
        return (await read_response(reader))[0]
    assert run("http1_many_fields", scenario) == [200]


def test_accepting_resumes_after_running_out_of_descriptors():
    # One accept that failed (EMFILE: the process at its descriptor limit)
    # ended the listener's accept loop, so the process never accepted again,
    # though descriptors were free soon after. It pauses and goes on, as
    # asyncio does.
    import resource
    import subprocess
    import sys

    client = (
        "import socket, sys, time\n"
        "port, count = int(sys.argv[1]), int(sys.argv[2])\n"
        "sys.stdin.readline()\n"
        "socks = []\n"
        "for _ in range(count):\n"
        "    s = socket.socket(); s.settimeout(1.0)\n"
        "    try:\n"
        "        s.connect(('127.0.0.1', port)); s.sendall(b'GET /small HTTP/1.1\\r\\nHost: x\\r\\n\\r\\n')\n"
        "    except OSError:\n"
        "        pass\n"
        "    socks.append(s)\n"
        "time.sleep(1.0)\n"
        "for s in socks:\n"
        "    s.close()\n"
        "time.sleep(1.5)\n"
        "s = socket.socket(); s.settimeout(5.0)\n"
        "try:\n"
        "    s.connect(('127.0.0.1', port))\n"
        "    s.sendall(b'GET /small HTTP/1.1\\r\\nHost: x\\r\\nConnection: close\\r\\n\\r\\n')\n"
        "    data = s.recv(65536)\n"
        "    print(data.split(b' ')[1].decode() if data else 'EOF')\n"
        "except OSError as error:\n"
        "    print(type(error).__name__)\n"
    )

    async def main():
        app = make_app("http1_emfile")
        cfg = config(application_dispatch_all=True)
        async with serve()(app, cfg):
            port = cfg.server.http_port
            soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
            child = await asyncio.create_subprocess_exec(
                sys.executable, "-c", client, str(port), "60",
                stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            resource.setrlimit(resource.RLIMIT_NOFILE, (len(os.listdir("/proc/self/fd")) + 20, hard))
            try:
                child.stdin.write(b"go\n")
                await child.stdin.drain()
                out, _ = await asyncio.wait_for(child.communicate(), 30)
            finally:
                resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))
            return out.decode().strip()

    import os
    assert uvloop.run(main()) == "200"


def lost_app(name):
    """A handler that waits, and records how it ended, for the tests of a
    peer leaving."""
    from oldman_server.exceptions import RequestCancelled

    app = OldmanServer(name, configure_logging=False)
    app.config.ACCESS_LOG = False
    app.config.RESPONSE_TIMEOUT = 30
    app.ctx.ends = []

    @app.get("/wait")
    async def wait(request):
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            app.ctx.ends.append(("cancelled", request.conn_info.lost))
            raise
        app.ctx.ends.append("completed")
        return text("done")

    @app.post("/stream-then-read", stream=True)
    async def stream_then_read(request):
        # Answers first, then reads the body (as a proxy streaming both ways).
        response = await request.respond()
        await response.send(b"started\n")
        try:
            async for _ in request.stream:
                pass
        except asyncio.CancelledError:
            app.ctx.ends.append("reading cancelled")
            raise
        app.ctx.ends.append("body ended")
        await response.eof()

    @app.exception(RequestCancelled)
    async def cancelled(request, exception):
        app.ctx.ends.append("RequestCancelled handler")
        return text("cancelled", status=499)

    @app.get("/ends")
    async def ends(request):
        return json(app.ctx.ends)

    return app


def reset(writer):
    import socket
    import struct
    sock = writer.get_extra_info("socket")
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    writer.transport.abort()


def on_backend(backend):
    """For a test of each reactor backend (one per function: a process runs
    one native runtime, with one backend)."""
    from oldman_server.native import engine
    from test_sanic_native_run import io_uring_available
    if backend == "io_uring" and not io_uring_available():
        pytest.skip("io_uring_setup is not permitted here")
    if engine._persistent is not None and engine._persistent[2] != backend:
        pytest.skip(f"this process's native runtime uses {engine._persistent[2]}")
    return backend


def test_a_peer_leaving_cancels_its_handler_as_in_sanic():
    peer_leaving(on_backend("epoll"))


def test_a_peer_leaving_cancels_its_handler_on_io_uring():
    peer_leaving(on_backend("io_uring"))


def peer_leaving(backend):
    # The handler of a request whose client closes (FIN) or resets (RST) the
    # connection is cancelled, and RequestCancelled goes to the error
    # handler, as the asyncio server does; the native host ran it to the end unnoticed.
    async def leave(how):
        async def scenario(reader, writer):
            writer.write(b"GET /wait HTTP/1.1\r\nHost: x\r\n\r\n")
            await writer.drain()
            await asyncio.sleep(0.3)
            if how == "fin":
                writer.write_eof()
            else:
                reset(writer)
            await asyncio.sleep(0.5)
        return scenario

    async def ends(reader, writer):
        writer.write(b"GET /ends HTTP/1.1\r\nHost: x\r\n\r\n")
        return json_module.loads((await read_response(reader))[2])

    async def main_scenarios():
        return [await leave("fin"), await leave("rst"), ends]

    import json as json_module
    scenarios = uvloop.run(main_scenarios())
    *_, recorded = run(f"http1_peer_leaves_{backend}", *scenarios, app_factory=lost_app, backend=backend)
    assert recorded == [["cancelled", True], "RequestCancelled handler",
                        ["cancelled", True], "RequestCancelled handler"]


def test_a_body_that_stops_arriving_ends_its_handler():
    body_stops(on_backend("epoll"))


def test_a_body_that_stops_arriving_ends_its_handler_on_io_uring():
    body_stops(on_backend("io_uring"))


def body_stops(backend):
    # Reset mid-body: the handler reading it is cancelled at once (it waited
    # for the response deadline); the client closing after the response has
    # started: the handler still reading is cancelled (it waited forever).
    async def reset_mid_body(reader, writer):
        writer.write(b"POST /stream-then-read HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\nabc")
        await reader.readuntil(b"started\n")
        reset(writer)
        await asyncio.sleep(0.5)

    async def fin_mid_body(reader, writer):
        writer.write(b"POST /stream-then-read HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\nabc")
        await reader.readuntil(b"started\n")
        writer.write_eof()
        await asyncio.sleep(0.5)

    async def ends(reader, writer):
        writer.write(b"GET /ends HTTP/1.1\r\nHost: x\r\n\r\n")
        import json as json_module
        return json_module.loads((await read_response(reader))[2])

    *_, recorded = run(f"http1_body_stops_{backend}", reset_mid_body, fin_mid_body, ends, app_factory=lost_app,
                       backend=backend)
    assert recorded == ["reading cancelled", "reading cancelled"]


def test_connections_ending_abnormally_leave_no_state_behind():
    # Python keeps per-connection state until the host says the connection
    # closed; a reset connection was never reported, and the state stayed.
    from oldman_server.models.server_types import native_connections

    async def resets(reader, writer):
        for _ in range(20):
            r, w = await asyncio.open_connection("127.0.0.1", writer.get_extra_info("peername")[1])
            w.write(b"GET /small HTTP/1.1\r\nHost: x\r\n\r\n")
            await read_response(r)
            reset(w)
        await asyncio.sleep(0.5)
        return len(native_connections)

    assert run("http1_reset_state", resets) == [0]  # (this connection sent no request)


def output_app(name):
    app = OldmanServer(name, configure_logging=False)
    app.config.ACCESS_LOG = False
    app.ctx.handled = 0

    @app.get("/p")
    async def piece(request):
        app.ctx.handled += 1
        return raw(b"p" * 15000)  # below the size written as its own buffer

    @app.get("/tiny")
    async def tiny(request):
        return raw(b"t")

    @app.get("/big")
    async def big(request):
        return raw(b"b" * (16 << 20))

    @app.get("/handled")
    async def handled(request):
        return text(str(app.ctx.handled))
    return app


def test_pipelined_responses_wait_for_the_peer_to_read():
    # Clients pipelining requests and not reading the responses: the host
    # batched every small response into one buffer until no complete request
    # was left (20 such clients held about 80 MiB more than for 1-byte
    # responses). It writes once 64 KiB are batched; a peer that does not
    # read then holds its responses in the kernel's buffers, not ours.
    import socket

    def rss_mib():
        with open("/proc/self/statm") as statm:
            return int(statm.read().split()[1]) * os.sysconf("SC_PAGE_SIZE") / (1 << 20)

    async def growth(port, path):
        request = f"GET {path} HTTP/1.1\r\nHost: x\r\n\r\n".encode()
        batch = request * (8192 // len(request))  # what the host's first read takes
        before, socks = rss_mib(), []
        for _ in range(20):
            sock = socket.socket()
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            sock.connect(("127.0.0.1", port))
            sock.sendall(batch)
            socks.append(sock)
        await asyncio.sleep(3)
        peak = rss_mib()
        for sock in socks:
            sock.close()
        await asyncio.sleep(0.5)
        return peak - before

    async def measure(reader, writer):
        port = writer.get_extra_info("peername")[1]
        return await growth(port, "/tiny"), await growth(port, "/p")

    (tiny, piece), = run("http1_pipelined_output", measure, app_factory=output_app,
                         max_requests=100000)
    assert piece < tiny + 20, (tiny, piece)


def test_a_peer_that_stops_reading_is_cut_off():
    # Writes have a deadline (from KEEP_ALIVE_TIMEOUT and
    # GRACEFUL_TCP_CLOSE_TIMEOUT, here 0.5 s): a peer that does not take a
    # response held the connection and its memory until the server stopped.
    async def stalled(reader, writer):
        writer.write(b"GET /big HTTP/1.1\r\nHost: x\r\n\r\n")
        await writer.drain()
        await asyncio.sleep(3)
        received = 0
        try:
            while chunk := await asyncio.wait_for(reader.read(1 << 20), 5):
                received += len(chunk)
        except ConnectionError:
            pass
        return received

    async def reading(reader, writer):
        writer.write(b"GET /big HTTP/1.1\r\nHost: x\r\n\r\n")
        status, _, body = await read_response(reader)
        return status, len(body)

    received, whole = run("http1_write_deadline", stalled, reading, app_factory=output_app,
                          write_timeout_ms=500, max_response_bytes=32 << 20)
    assert 0 < received < 16 << 20
    assert whole == (200, 16 << 20)  # one that reads gets all of it


def test_the_write_deadline_follows_sanic_timeouts():
    from oldman_server.native.host import native_config
    app = OldmanServer("write_deadline_config", configure_logging=False)
    app.config.KEEP_ALIVE_TIMEOUT, app.config.GRACEFUL_TCP_CLOSE_TIMEOUT = 2, 0.5
    assert native_config(app).server.write_timeout_ms == 2500


def body_app(name):
    app = OldmanServer(name, configure_logging=False)
    app.config.ACCESS_LOG = False

    @app.post("/slow-reader", stream=True)
    async def slow_reader(request):
        total = len(await request.stream.read())
        await asyncio.sleep(1.5)  # meanwhile the client keeps sending
        while (body := await request.stream.read()) is not None:
            total += len(body)
        return text(str(total))

    @app.post("/stream-echo", stream=True)
    async def stream_echo(request):
        response = await request.respond(content_type="text/plain")
        await response.send(b"start", end_stream=False)
        try:
            while (body := await request.stream.read()) is not None:
                await response.send(body, end_stream=False)
        except asyncio.CancelledError:
            app.ctx.echo_cancelled = True
            raise
        await response.send(b"end", end_stream=True)

    @app.get("/echo-cancelled")
    async def echo_cancelled(request):
        return text(str(getattr(app.ctx, "echo_cancelled", False)))

    @app.post("/early", stream=True)
    async def early(request):
        return text("early")  # without reading the body

    @app.post("/count", stream=True)
    async def count(request):
        total = 0
        while (body := await request.stream.read()) is not None:
            total += len(body)
        return text(str(total))

    @app.post("/length")
    async def length(request):
        return text(str(len(request.body)))

    @app.get("/to-python")
    async def to_python(request):
        from oldman_server.native import host
        return text(str(host._engine.server_stats()["body_bytes_to_python"]))
    return app


def test_a_request_body_arrives_as_fast_as_the_handler_reads_it():
    # A stream handler reading slower than the client sends: the host read the
    # whole body into Python's memory (64 MiB within 2.5 s, not counted in
    # NATIVE_MAX_MEMORY). As the asyncio server pauses reading at REQUEST_BUFFER_SIZE, it
    # forwards at most the window ahead of what the handler has read.
    size = 32 << 20

    async def upload(reader, writer):
        writer.write(f"POST /slow-reader HTTP/1.1\r\nHost: x\r\nContent-Length: {size}\r\n\r\n".encode())
        piece = b"u" * (1 << 20)
        for _ in range(size // len(piece)):
            writer.write(piece)
        sending = asyncio.ensure_future(writer.drain())
        await asyncio.sleep(1)
        _, port = writer.get_extra_info("peername")
        r2, w2 = await asyncio.open_connection("127.0.0.1", port)
        w2.write(b"GET /to-python HTTP/1.1\r\nHost: x\r\n\r\n")
        forwarded = int((await read_response(r2))[2])
        w2.close()
        await sending
        status, _, body = await read_response(reader)
        return forwarded, status, int(body)

    (forwarded, status, received), = run("http1_body_window", upload, app_factory=body_app,
                                         max_body_bytes=64 << 20, body_window_bytes=65536,
                                         decision_timeout_ms=10000)
    assert (status, received) == (200, size)
    assert forwarded < 4 << 20, forwarded


def test_a_stream_handler_reads_a_body_past_request_max_size():
    # The framework lifts REQUEST_MAX_SIZE for stream=True handlers; the host answered
    # 413 to their reads. A plain handler still gets 413.
    body = b"s" * 5000

    def post(target, framing):
        async def scenario(reader, writer):
            if framing == "length":
                head = f"Content-Length: {len(body)}"
                pieces = [body[at:at + 1000] for at in range(0, len(body), 1000)]
            else:
                head = "Transfer-Encoding: chunked"
                pieces = [b"3e8\r\n" + body[at:at + 1000] + b"\r\n" for at in range(0, len(body), 1000)]
                pieces.append(b"0\r\n\r\n")
            writer.write(f"POST {target} HTTP/1.1\r\nHost: x\r\n{head}\r\n\r\n".encode())
            # Read while sending: a 413 comes before the body is all sent.
            response = asyncio.ensure_future(read_response(reader))
            try:
                for piece in pieces:  # in pieces, as they arrive while the handler reads
                    if response.done():
                        break
                    writer.write(piece)
                    await writer.drain()
                    await asyncio.sleep(0.01)
            except (ConnectionError, RuntimeError):
                pass  # the connection ended after the 413
            status, _, answer = await response
            return status, answer
        return scenario

    results = run("http1_stream_limit", post("/count", "length"), post("/count", "chunked"),
                  post("/length", "length"), post("/length", "chunked"),
                  app_factory=body_app, max_body_bytes=1024)
    assert results[:2] == [(200, b"5000"), (200, b"5000")]
    assert [status for status, _ in results[2:]] == [413, 413]


def test_request_max_size_beyond_128_mib():
    # REQUEST_MAX_SIZE above 128 MiB was cut to 128 MiB without a word: a
    # 129 MiB body got 413 (200 from the asyncio server).
    from oldman_server.native.host import native_config
    app = OldmanServer("max_size_config", configure_logging=False)
    app.config.REQUEST_MAX_SIZE = 200 << 20
    assert native_config(app).server.max_body_bytes == 200 << 20
    app.config.REQUEST_MAX_SIZE = float("inf")
    assert native_config(app).server.max_body_bytes == 1 << 62
    size = (129 << 20) + 1

    async def upload(reader, writer):
        writer.write(f"POST /length HTTP/1.1\r\nHost: x\r\nContent-Length: {size}\r\n\r\n".encode())
        piece = b"b" * (1 << 20)
        for _ in range(size >> 20):
            writer.write(piece)
            await writer.drain()
        writer.write(b"b")
        status, _, body = await read_response(reader)
        return status, int(body)

    assert run("http1_beyond_128", upload, app_factory=body_app, max_body_bytes=200 << 20) == [
        (200, size)]


def length_app(name):
    app = OldmanServer(name, configure_logging=False)
    app.config.ACCESS_LOG = False
    app.ctx.errors = []

    @app.get("/over")
    async def over(request):
        response = await request.respond(headers={"Content-Length": "5"}, content_type="text/plain")
        await response.send(b"12345")
        try:
            await response.send(b"EXTRA")
        except Exception as error:
            app.ctx.errors.append(str(error))
            raise

    @app.get("/over-at-once")
    async def over_at_once(request):
        response = await request.respond(headers={"Content-Length": "5"}, content_type="text/plain")
        await response.send(b"1234567890")
        await response.eof()

    @app.get("/errors")
    async def errors(request):
        return text("|".join(app.ctx.errors))
    return app


def test_a_streamed_response_never_goes_past_its_content_length():
    # The host wrote whatever was sent after a declared Content-Length, under
    # a keep-alive head: a client reading on would take the rest for the next
    # response. The framework raises "Response was bigger than content-length" instead
    # of sending it; the host also holds the line itself and ends the connection.
    def fetch(target):
        async def scenario(reader, writer):
            writer.write(f"GET {target} HTTP/1.1\r\nHost: x\r\n\r\n".encode())
            data = await asyncio.wait_for(reader.read(), 3)
            head, _, body = data.partition(b"\r\n\r\n")
            return body
        return scenario

    async def errors(reader, writer):
        writer.write(b"GET /errors HTTP/1.1\r\nHost: x\r\n\r\n")
        return (await read_response(reader))[2]

    over, at_once, raised = run("http1_stream_length", fetch("/over"), fetch("/over-at-once"),
                                errors, app_factory=length_app)
    assert (over, at_once) == (b"12345", b"12345")
    assert raised == b"Response was bigger than content-length"


def blocking_app(name):
    import time
    app = OldmanServer(name, configure_logging=False)
    app.config.ACCESS_LOG = False

    @app.get("/x")
    async def x(request):
        return text("ok")

    @app.get("/block")
    async def block(request):
        time.sleep(1)  # blocks the event loop: Python takes no events meanwhile
        return text("blocked")
    return app


def test_events_of_admitted_requests_do_not_fill_the_queue():
    # The queue to Python counted every event towards max_requests, those of
    # requests Python already had included (body data, connection closes): a
    # new request was refused with 500 while 2 of 4 requests were in flight.
    import socket
    import time

    def blocking_steps(port):
        def get(sock, target):
            sock.sendall(f"GET {target} HTTP/1.1\r\nHost: x\r\n\r\n".encode())
            data = b""
            while b"\r\n\r\n" not in data:
                data += sock.recv(4096)
            return int(data.split(b" ", 2)[1])

        socks = [socket.create_connection(("127.0.0.1", port), timeout=5) for _ in range(8)]
        assert [get(sock, "/x") for sock in socks] == [200] * 8
        blocker = socket.create_connection(("127.0.0.1", port), timeout=5)
        blocker.sendall(b"GET /block HTTP/1.1\r\nHost: x\r\n\r\n")
        time.sleep(0.3)  # the event loop is blocked now
        for sock in socks:
            sock.close()  # eight connection-closed events for Python
        time.sleep(0.2)
        late = socket.create_connection(("127.0.0.1", port), timeout=5)
        try:
            return get(late, "/x")
        finally:
            late.close()
            blocker.close()

    async def scenario(reader, writer):
        _, port = writer.get_extra_info("peername")
        return await asyncio.to_thread(blocking_steps, port)

    assert run("http1_admitted_events", scenario, app_factory=blocking_app, max_requests=4) == [200]


def test_the_date_header_is_english_whatever_the_locale():
    # The host formatted Date with strftime, which follows LC_TIME: with a
    # German locale an engine that writes Date sent "Date: Do, 22 Okt 2026".
    import locale
    import re
    available = [name for name in ("de_DE.UTF-8", "de_DE.utf8", "zh_TW.UTF-8", "zh_TW.utf8")
                 if _settable(locale, name)]
    if not available:
        pytest.skip("no non-English locale installed")
    previous = locale.setlocale(locale.LC_TIME)
    locale.setlocale(locale.LC_TIME, available[0])

    async def scenario(reader, writer):
        writer.write(b"GET /small HTTP/1.1\r\nHost: x\r\n\r\n")
        _, headers, _ = await read_response(reader)
        return dict(headers)["Date"]

    try:
        date, = run("http1_date_locale", scenario, generate_date_header=True)
    finally:
        locale.setlocale(locale.LC_TIME, previous)
    assert re.fullmatch(r"(Mon|Tue|Wed|Thu|Fri|Sat|Sun), \d\d (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|"
                        r"Nov|Dec) \d{4} \d\d:\d\d:\d\d GMT", date), date


def _settable(locale, name):
    previous = locale.setlocale(locale.LC_TIME)
    try:
        locale.setlocale(locale.LC_TIME, name)
        return True
    except locale.Error:
        return False
    finally:
        locale.setlocale(locale.LC_TIME, previous)


def test_a_response_goes_out_before_the_rest_of_its_body():
    # A handler that answers without reading the body: the asyncio server sends the
    # response at once (and reads the body's rest after it); the host held it
    # until the whole body had come, so a client waiting for the answer
    # before sending more waited until the keep-alive deadline.
    async def scenario(reader, writer):
        writer.write(b"POST /early HTTP/1.1\r\nHost: x\r\nContent-Length: 1000\r\n\r\n" + b"x" * 10)
        status, _, body = await asyncio.wait_for(read_response(reader), 3)
        writer.write(b"x" * 990)  # the rest: read past, the connection stays usable
        writer.write(b"POST /early HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n")
        again = await asyncio.wait_for(read_response(reader), 3)
        return (status, body), again[0::2]

    assert run("http1_early_response", scenario, app_factory=body_app) == [
        ((200, b"early"), (200, b"early"))]


def test_a_failing_connection_lost_does_not_lose_the_other_events(monkeypatch):
    # The binding took a batch of events off the native queue, then built the
    # Python side: an exception from one connection state's connection_lost
    # dropped the rest of the batch (here the other connections' closes, so
    # their entries stayed in the registry for good).
    import socket
    import time
    from oldman_server.models import server_types

    def failing(self):
        raise RuntimeError("connection_lost failed")
    monkeypatch.setattr(server_types._NativeConnection, "connection_lost", failing)

    def app_factory(name):
        app = blocking_app(name)

        @app.get("/conn")
        async def conn(request):
            return text(str(request.conn_info is not None))  # creates the connection's state

        @app.get("/registered")
        async def registered(request):
            return text(str(len(server_types.native_connections)))
        return app

    def blocking_steps(port):
        def get(sock, target):
            sock.sendall(f"GET {target} HTTP/1.1\r\nHost: x\r\n\r\n".encode())
            data = b""
            while b"\r\n\r\n" not in data or not data.endswith((b"True", b"blocked")) and len(data) < 400:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                data += chunk
            return data

        socks = [socket.create_connection(("127.0.0.1", port), timeout=5) for _ in range(4)]
        for sock in socks:
            get(sock, "/conn")
        blocker = socket.create_connection(("127.0.0.1", port), timeout=5)
        blocker.sendall(b"GET /block HTTP/1.1\r\nHost: x\r\n\r\n")
        time.sleep(0.3)
        for sock in socks:
            sock.close()  # four closes, one batch once the loop is free
        time.sleep(1.2)
        blocker.close()
        time.sleep(0.2)

    async def scenario(reader, writer):
        _, port = writer.get_extra_info("peername")
        await asyncio.to_thread(blocking_steps, port)
        writer.write(b"GET /registered HTTP/1.1\r\nHost: x\r\n\r\n")
        return (await asyncio.wait_for(read_response(reader), 3))[2]

    # Only the asking connection is left.
    assert run("http1_failing_connection_lost", scenario, app_factory=app_factory) == [b"1"]


def test_a_body_that_stalls_under_a_streamed_response_times_out():
    # Once a streamed response went out, the body read had no deadline at all:
    # a client that stopped sending held the connection and the handler for
    # good. As in the asyncio server's response stage, RESPONSE_TIMEOUT counts from the
    # last data received: the handler is cancelled and the connection ends.
    async def stall(reader, writer):
        writer.write(b"POST /stream-echo HTTP/1.1\r\nHost: x\r\nContent-Length: 10\r\n\r\nabc")
        await reader.readuntil(b"\r\n\r\n")
        try:
            while await asyncio.wait_for(reader.read(1000), 3):
                pass
            ended = True
        except TimeoutError:
            ended = False
        return ended

    async def cancelled(reader, writer):
        writer.write(b"GET /echo-cancelled HTTP/1.1\r\nHost: x\r\n\r\n")
        return (await read_response(reader))[2]

    assert run("http1_stalled_stream_body", stall, cancelled, app_factory=body_app,
               decision_timeout_ms=500) == [True, b"True"]
