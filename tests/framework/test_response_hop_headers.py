"""Response fields the server writes itself (Content-Length,
Transfer-Encoding, Connection) replace the application's; every other field
the application sets is sent as it is, including Server, Date, Keep-Alive and
other hop-by-hop names. The native host once answered such responses with an
empty 500."""
import asyncio

from oldman_server.response import empty, text

SET_BY_APP = {
    "Server": "app-server",
    "Date": "Wed, 21 Oct 2026 07:28:00 GMT",
    "Keep-Alive": "timeout=5",
    "Upgrade": "h2c",
    "Trailer": "X-Checksum",
    "Proxy-Connection": "keep-alive",
    "X-Probe-Pid": "1",
}
REQUEST = b"GET /%s HTTP/1.1\r\nHost: x\r\n\r\n"
LAST = b"GET /last HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"


def exchange(app, port, path):
    """The raw head and body of the response to GET /path on a kept-alive
    connection (a second request closes it)."""

    @app.get("/last")
    async def last(request):
        return text("last")

    @app.after_server_start
    async def fetch(app):
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(REQUEST % path.encode() + LAST)
        app.ctx.received = await asyncio.wait_for(reader.read(), 10)
        writer.close()
        app.stop()

    app.run(single_process=True, port=port, access_log=False)
    first, _, rest = app.ctx.received.partition(b"\r\n\r\n")
    lines = first.decode("latin-1").split("\r\n")
    fields = [tuple(line.split(": ", 1)) for line in lines[1:]]
    return lines[0], fields, rest


def values(fields, name):
    return [value for key, value in fields if key.lower() == name.lower()]


def test_complete_response_keeps_the_application_fields(app, port):
    @app.get("/x")
    async def handler(request):
        return text("hello", headers={**SET_BY_APP, "Connection": "close", "Content-Length": "999"})

    status, fields, rest = exchange(app, port, "x")
    assert status == "HTTP/1.1 200 OK"
    for name, value in SET_BY_APP.items():
        assert values(fields, name) == [value]
    assert values(fields, "connection") == ["keep-alive"]
    assert values(fields, "content-length") == ["5"]
    assert values(fields, "transfer-encoding") == []
    assert rest.startswith(b"hello" + b"HTTP/1.1 200 OK")


def test_streamed_response_keeps_the_application_fields(app, port):
    @app.get("/x")
    async def handler(request):
        response = await request.respond(headers={**SET_BY_APP, "Connection": "close"},
                                         content_type="text/plain")
        await response.send("hello")
        await response.eof()

    status, fields, rest = exchange(app, port, "x")
    assert status == "HTTP/1.1 200 OK"
    for name, value in SET_BY_APP.items():
        assert values(fields, name) == [value]
    assert values(fields, "connection") == ["keep-alive"]
    assert values(fields, "transfer-encoding") == ["chunked"]
    assert rest.startswith(b"5\r\nhello\r\n0\r\n\r\nHTTP/1.1 200 OK")


def test_no_body_status_drops_the_application_length(app, port):
    @app.get("/x")
    async def handler(request):
        return empty(headers={"Server": "app-server", "Content-Length": "10"})

    status, fields, rest = exchange(app, port, "x")
    assert status == "HTTP/1.1 204 No Content"
    assert values(fields, "server") == ["app-server"]
    assert values(fields, "content-length") == []
    assert rest.startswith(b"HTTP/1.1 200 OK")
