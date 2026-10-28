"""How a WebSocket connection ends: its tasks finish with it, and a lost
connection reaches the handler. The suite runs on both servers (the native
host, and the asyncio server with OLDMAN_SERVER_NATIVE_SERVER=false)."""
import asyncio
import os
import signal
import socket
import subprocess
import sys
import textwrap
import time

import pytest

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosedOK

from oldman_server import OldmanServer


def websocket_tasks():
    """Pending tasks of WebSocket connections (auto-closer, keepalive ping)."""
    return sorted(
        task.get_coro().__qualname__
        for task in asyncio.all_tasks()
        if task.get_coro().__qualname__.startswith("WebsocketImplProtocol.")
    )


async def until(condition, timeout=3.0):
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            return False
        await asyncio.sleep(0.02)
    return True


async def test_websockets_the_server_closes_leave_no_tasks():
    app = OldmanServer("WsLeaveNoTasks")

    @app.websocket("/greet")
    async def greet(request, ws):
        await ws.send("hi")  # then the handler ends: the server closes

    async with app.async_test_client as client:
        for _ in range(20):
            async with connect(f"ws://127.0.0.1:{client.port}/greet") as ws:
                assert await ws.recv() == "hi"
                with pytest.raises(ConnectionClosedOK):
                    await ws.recv()
        assert await until(lambda: not websocket_tasks()), websocket_tasks()


async def test_a_lost_connection_cancels_a_handler_that_does_not_receive():
    app = OldmanServer("WsLostCancels")
    app.ctx.events = []

    @app.websocket("/sleep")
    async def sleep(request, ws):
        try:
            await asyncio.sleep(30)  # not reading: the peer's end cannot reach it
        except asyncio.CancelledError:
            app.ctx.events.append("cancelled")
            raise

    async with app.async_test_client as client:
        ws = await connect(f"ws://127.0.0.1:{client.port}/sleep")
        await asyncio.sleep(0.2)
        ws.transport.abort()  # no closing handshake
        assert await until(lambda: app.ctx.events == ["cancelled"])
        assert await until(lambda: not websocket_tasks()), websocket_tasks()


SERVER = textwrap.dedent(
    """
    import asyncio

    from oldman_server import OldmanServer
    from oldman_server.response import text

    app = OldmanServer("WsStopQuietly")


    @app.websocket("/greet")
    async def greet(request, ws):
        await ws.send("hi")


    @app.get("/tasks")
    async def tasks(request):
        return text(str(sum(
            task.get_coro().__qualname__.startswith("WebsocketImplProtocol.")
            for task in asyncio.all_tasks()
        )))


    if __name__ == "__main__":
        app.run(host="127.0.0.1", port={port}, single_process=True)
    """
)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX signals")
def test_stopping_after_websockets_logs_nothing_about_them(tmp_path):
    # A real run whose loop closes at the end: a WebSocket task left pending
    # would be destroyed with it ("Task was destroyed but it is pending",
    # "Error closing websocket connection", "Event loop is closed").
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    (tmp_path / "server.py").write_text(SERVER.replace("{port}", str(port)))
    process = subprocess.Popen(
        [sys.executable, "server.py"], cwd=tmp_path, env=os.environ.copy(),
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    try:
        deadline = time.monotonic() + 20
        while True:
            try:
                socket.create_connection(("127.0.0.1", port), 0.2).close()
                break
            except OSError:
                assert process.poll() is None and time.monotonic() < deadline
                time.sleep(0.05)

        async def sessions():
            for _ in range(20):
                async with connect(f"ws://127.0.0.1:{port}/greet") as ws:
                    assert await ws.recv() == "hi"
                    with pytest.raises(ConnectionClosedOK):
                        await ws.recv()
            await asyncio.sleep(0.3)
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b"GET /tasks HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            data = await reader.read()
            writer.close()
            return data.split(b"\r\n\r\n", 1)[1]

        assert asyncio.run(sessions()) == b"0"
        process.send_signal(signal.SIGINT)
        output, _ = process.communicate(timeout=30)
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()
    assert process.returncode == 0, output
    for message in ("Task was destroyed", "Error closing websocket connection",
                    "Event loop is closed"):
        assert message not in output, output
