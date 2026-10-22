"""response.abort(): end a streamed response without completing it, so the
client sees it is incomplete (for instance when its upstream failed)."""
import asyncio

import pytest

from oldman_server import OldmanServer
from oldman_server.exceptions import ServerError


def run(app: OldmanServer, port: int, raw: bytes = b"GET / HTTP/1.1\r\nHost: x\r\n\r\n"):
    """Serve, send one raw request and return what the client got until the
    server closed the connection."""

    @app.after_server_start
    async def fetch(app):
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(raw)
        app.ctx.received = await asyncio.wait_for(reader.read(), 10)
        writer.close()
        app.stop()

    app.run(single_process=True, port=port, access_log=False)
    return app.ctx.received


def test_abort_closes_the_connection_without_the_final_chunk(app, port):
    events = []

    @app.get("/")
    async def handler(request):
        response = await request.respond(content_type="text/plain")
        await response.send("partial")
        response.abort()
        with pytest.raises(ServerError):
            await response.send("more")
        events.append("aborted")
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            events.append("cancelled")
            raise

    received = run(app, port)
    head, _, body = received.partition(b"\r\n\r\n")
    assert b"transfer-encoding: chunked" in head.lower()
    # The data sent before, then the connection closes: no 0-length chunk.
    assert body == b"7\r\npartial\r\n"
    assert events == ["aborted", "cancelled"]


def test_abort_with_content_length_leaves_the_body_short(app, port):
    @app.get("/")
    async def handler(request):
        response = await request.respond(
            content_type="text/plain", headers={"content-length": "100"}
        )
        await response.send("partial")
        response.abort()

    received = run(app, port)
    head, _, body = received.partition(b"\r\n\r\n")
    assert b"content-length: 100" in head.lower()
    assert body == b"partial"


def test_abort_before_anything_was_sent_raises(app):
    @app.get("/")
    async def handler(request):
        response = await request.respond(content_type="text/plain")
        with pytest.raises(ServerError, match="nothing was sent"):
            response.abort()
        await response.send("whole", end_stream=True)

    _, response = app.test_client.get("/")
    assert response.status == 200
    assert response.text == "whole"


def test_abort_after_the_response_ended_does_nothing(app):
    @app.get("/")
    async def handler(request):
        response = await request.respond(content_type="text/plain")
        await response.send("whole", end_stream=True)
        response.abort()

    _, response = app.test_client.get("/")
    assert response.status == 200
    assert response.text == "whole"
