"""The Connection header is a list of options (RFC 9110 7.6.1): only close
ends the connection, and HTTP/1.0 keeps it only with keep-alive (RFC 9112
9.3). Both servers decide the same (the suite runs on each)."""
import asyncio

import pytest

from oldman_server import OldmanServer
from oldman_server.response import text


async def exchange(port, head):
    """Send `head` then a second request on the same connection: the first
    response's Connection header, and whether the second one was answered."""
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer.write(head)
        status_and_headers = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 5)
        fields = dict(
            line.split(": ", 1)
            for line in status_and_headers.decode("latin-1").split("\r\n")[1:]
            if line
        )
        fields = {name.lower(): value for name, value in fields.items()}
        await reader.readexactly(int(fields.get("content-length", "0")))
        writer.write(b"GET /ok HTTP/1.1\r\nHost: x\r\n\r\n")
        try:
            second = await asyncio.wait_for(reader.read(1024), 5)
        except ConnectionError:
            second = b""
        return fields.get("connection"), second.startswith(b"HTTP/1.1 200")
    finally:
        writer.close()


@pytest.mark.parametrize(
    "version, connection, kept",
    [
        ("1.1", ["Upgrade"], True),
        ("1.1", ["keep-alive, Upgrade"], True),
        ("1.1", ["TE"], True),
        ("1.1", ["close, Upgrade"], False),
        ("1.1", ["Upgrade", "Close"], False),
        ("1.0", ["keep-alive, x"], True),
        ("1.0", ["Upgrade"], False),
        ("1.0", [], False),
    ],
)
async def test_connection_options(version, connection, kept):
    app = OldmanServer("ConnectionOptions")

    @app.get("/ok")
    async def ok(request):
        return text("ok")

    fields = "".join(f"Connection: {value}\r\n" for value in connection)
    head = f"GET /ok HTTP/{version}\r\nHost: x\r\n{fields}\r\n".encode()
    async with app.async_test_client as client:
        header, answered = await exchange(client.port, head)
    assert header == ("keep-alive" if kept else "close")
    assert answered is kept


async def test_a_refused_websocket_upgrade_keeps_the_connection():
    # A request middleware answers an upgrade request itself (here 403): the
    # connection stays open for the next request, on both servers.
    app = OldmanServer("RefusedUpgrade")

    @app.on_request
    async def refuse(request):
        if request.path == "/ws":
            return text("no", status=403)

    @app.websocket("/ws")
    async def ws(request, ws):
        await ws.send("unreachable")

    @app.get("/ok")
    async def ok(request):
        return text("ok")

    head = (
        b"GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\n"
        b"Connection: keep-alive, Upgrade\r\n"
        b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
    )
    async with app.async_test_client as client:
        header, answered = await exchange(client.port, head)
    assert (header, answered) == ("keep-alive", True)
