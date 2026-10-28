import asyncio
import logging
import socket
import threading
import time

import pytest

from pytest import LogCaptureFixture

from oldman_server.native import native_server
from oldman_server.response import empty


def test_no_exceptions_when_cancel_pending_request(
    app, caplog: LogCaptureFixture, port
):
    app.config.GRACEFUL_SHUTDOWN_TIMEOUT = 1
    cancelled = []

    @app.get("/")
    async def handler(request):
        request.app.ctx.handling.set()
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            cancelled.append(True)
            raise

    @app.listener("after_server_start")
    async def _request(sanic, loop):
        sanic.ctx.handling = asyncio.Event()
        # Kept on the app: a collected StreamWriter closes its connection.
        _, sanic.ctx.writer = await asyncio.open_connection("127.0.0.1", port)
        sanic.ctx.writer.write(b"GET / HTTP/1.1\r\n\r\n")
        # Stop only once the request is in its handler (else the server
        # closes the connection as idle).
        await sanic.ctx.handling.wait()
        app.stop()

    started = time.monotonic()
    with caplog.at_level(logging.INFO):
        app.run(single_process=True, access_log=True, port=port)

    # Cut off after the graceful timeout, not awaited to its end.
    assert time.monotonic() - started < 4
    assert cancelled == [True]
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    if not native_server(app.config):
        # The asyncio server logs the request it cut off.
        assert "Request: GET http:/// stopped. Transport is closed." in caplog.text


def test_completes_request(app, caplog: LogCaptureFixture, port):
    app.config.GRACEFUL_SHUTDOWN_TIMEOUT = 1

    @app.get("/")
    async def handler(request):
        await asyncio.sleep(0.5)
        return empty()

    @app.listener("after_server_start")
    async def _request(sanic, loop):
        connect = asyncio.open_connection("127.0.0.1", port)
        _, writer = await connect
        writer.write(b"GET / HTTP/1.1\r\n\r\n")
        app.stop()

    with caplog.at_level(logging.INFO):
        app.run(single_process=True, access_log=True, port=port)

    assert ("oldman_server.access", 20, "") in caplog.record_tuples

    # Make sure that the server starts shutdown process before access log
    index_stopping = 0
    for idx, record in enumerate(caplog.records):
        if record.message.startswith("Stopping worker"):
            index_stopping = idx
            break
    index_request = caplog.record_tuples.index(("oldman_server.access", 20, ""))
    assert index_request > index_stopping > 0


@pytest.mark.parametrize("attempt", range(5))
def test_a_request_cut_off_after_the_grace_period_gets_no_answer(app, port, attempt):
    """A handler still running when GRACEFUL_SHUTDOWN_TIMEOUT ends is
    cancelled and its connection closed without any answer, on both servers
    (the native host used to race a 503 against the close)."""
    app.config.GRACEFUL_SHUTDOWN_TIMEOUT = 0.2
    received = []

    @app.get("/")
    async def handler(request):
        request.app.ctx.handling.set()
        await asyncio.sleep(100)

    def client():
        with socket.create_connection(("127.0.0.1", port), timeout=10) as sock:
            sock.sendall(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
            data = b""
            while chunk := sock.recv(4096):
                data += chunk
            received.append(data)

    @app.listener("after_server_start")
    async def _request(app, loop):
        app.ctx.handling = asyncio.Event()
        app.ctx.client = threading.Thread(target=client)
        app.ctx.client.start()
        await app.ctx.handling.wait()
        app.stop()

    app.run(single_process=True, port=port)
    app.ctx.client.join(timeout=10)
    assert received == [b""]
