"""A client that stops reading a streamed response, and a stream that sends
slowly: both servers apply the same rules. RESPONSE_TIMEOUT runs until the
response starts; after that a write that waits for the client longer than
KEEP_ALIVE_TIMEOUT + GRACEFUL_TCP_CLOSE_TIMEOUT cuts the connection, which
cancels the handler as the client leaving does."""
import asyncio
import socket
import threading
import time

from oldman_server.response import text

CHUNK = b"x" * 65536
TOTAL = 256  # chunks: 16 MiB, far more than the socket buffers hold


def test_a_client_that_stops_reading_is_cut_off_after_the_write_deadline(app, port):
    app.config.KEEP_ALIVE_TIMEOUT = 0.5
    app.config.GRACEFUL_TCP_CLOSE_TIMEOUT = 0.5
    app.config.RESPONSE_TIMEOUT = 60
    outcome = {}
    cancelled = threading.Event()

    @app.get("/stream")
    async def stream(request):
        response = await request.respond(content_type="application/octet-stream")
        outcome["started"] = time.monotonic()
        try:
            for _ in range(TOTAL):
                await response.send(CHUNK)
            await response.eof()
            outcome["finished"] = True
        except asyncio.CancelledError:
            outcome["cancelled"] = time.monotonic()
            cancelled.set()
            raise

    def client():
        with socket.socket() as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            sock.settimeout(20)
            sock.connect(("127.0.0.1", port))
            sock.sendall(b"GET /stream HTTP/1.1\r\nHost: x\r\n\r\n")
            data = sock.recv(256)  # the head, then it stops reading
            cancelled.wait(20)
            try:
                while chunk := sock.recv(65536):
                    data += chunk
            except ConnectionError:
                pass
            outcome["received"] = data

    @app.after_server_start
    async def drive(app):
        app.ctx.client = threading.Thread(target=client)
        app.ctx.client.start()
        for _ in range(200):
            if cancelled.is_set():
                break
            await asyncio.sleep(0.1)
        app.stop()

    app.run(single_process=True, port=port)
    app.ctx.client.join(20)

    assert "finished" not in outcome
    assert "cancelled" in outcome, outcome.keys()
    # The deadline (1 s) after the buffers filled, with room for a busy machine.
    assert outcome["cancelled"] - outcome["started"] < 8
    received = outcome["received"]
    assert len(received) < TOTAL * len(CHUNK)
    assert not received.endswith(b"0\r\n\r\n")


def test_a_slow_stream_is_not_cut_by_the_response_timeout(app):
    app.config.RESPONSE_TIMEOUT = 0.3

    @app.get("/slow")
    async def slow(request):
        response = await request.respond(content_type="text/plain")
        await response.send("0")  # the response has started
        for number in (1, 2):
            await asyncio.sleep(0.5)  # longer than RESPONSE_TIMEOUT
            await response.send(str(number))
        await response.eof()

    @app.get("/late")
    async def late(request):
        await asyncio.sleep(1)
        return text("late")

    _, response = app.test_client.get("/slow")
    assert response.status == 200
    assert response.text == "012"
    # Before the response starts, RESPONSE_TIMEOUT applies as before.
    _, response = app.test_client.get("/late")
    assert response.status == 503
