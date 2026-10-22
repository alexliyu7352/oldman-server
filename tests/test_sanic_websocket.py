"""Sanic WebSocket routes on the native host (a tunnel after the 101)."""
import asyncio

from oldman_server import OldmanServer
from oldman_server.response import text
import uvloop
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from test_http import config


def make_app(name):
    app = OldmanServer(name, configure_logging=False)
    app.config.ACCESS_LOG = False
    app.ctx.events = []

    @app.websocket("/echo")
    async def echo(request, ws):
        async for message in ws:
            await ws.send(message)

    @app.websocket("/greet")
    async def greet(request, ws):
        await ws.send(f"hello {request.args.get('name')}")  # then the handler ends

    @app.websocket("/held")
    async def held(request, ws):
        try:
            await ws.recv()
        except (ConnectionClosed, asyncio.CancelledError) as error:
            app.ctx.events.append(type(error).__name__)
            raise

    @app.websocket("/slow-reader")
    async def slow_reader(request, ws):
        await ws.recv()  # the go-ahead, then the client floods while we wait
        await asyncio.sleep(0.5)
        received = 0
        for _ in range(200):
            received += len(await ws.recv())
        await ws.send(str(received))

    @app.get("/state")
    async def state(request):
        from oldman_server.models.server_types import native_connections
        from oldman_server.native.host import _engine
        return text(f"{len(native_connections)}:{len(_engine._application_tunnels)}:"
                    f"{','.join(app.ctx.events)}")
    return app


def run(name, *scenarios):
    async def main():
        from oldman_server.native.host import serve
        app = make_app(name)
        # A 64 KiB response limit: larger messages go out in several pieces.
        cfg = config(application_dispatch_all=True, max_response_bytes=65536)
        results = []
        async with serve(app, cfg) as engine:
            app.ctx.engine = engine
            for scenario in scenarios:
                results.append(await asyncio.wait_for(scenario(cfg.server.http_port), 10))
        return results
    return uvloop.run(main())


async def state(port):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(b"GET /state HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
    data = await reader.read()
    writer.close()
    return data.split(b"\r\n\r\n", 1)[1].decode()


def test_websocket_messages_close_and_disconnect():
    async def echo(port):
        async with connect(f"ws://127.0.0.1:{port}/echo") as ws:
            await ws.send("text")
            await ws.send(b"\x00binary\xff")
            await ws.send("x" * 200_000)  # several reads and writes
            return [await ws.recv(), await ws.recv(), len(await ws.recv())]

    async def server_closes(port):
        async with connect(f"ws://127.0.0.1:{port}/greet?name=ws") as ws:
            message = await ws.recv()
            try:
                await ws.recv()
            except ConnectionClosed as closed:
                return message, closed.rcvd.code

    async def client_leaves(port):
        ws = await connect(f"ws://127.0.0.1:{port}/held")
        ws.transport.abort()  # no close handshake
        await asyncio.sleep(0.3)
        return await state(port)

    async def early_frames(port):
        # A frame sent right behind the upgrade request is in the request
        # buffer when the tunnel starts.
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET /echo HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\n"
                     b"Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                     b"Sec-WebSocket-Version: 13\r\n\r\n"
                     # masked text frame "hi"
                     b"\x81\x82\x00\x00\x00\x00hi")
        head = await reader.readuntil(b"\r\n\r\n")
        frame = await reader.readexactly(4)
        writer.close()
        return head.split(b"\r\n", 1)[0], frame

    async def invalid_upgrade(port):
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET /echo HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        data = await reader.read()
        writer.close()
        return data.split(b"\r\n", 1)[0]

    async def released(port):
        await asyncio.sleep(0.2)
        return await state(port)

    results = run("ws_basic", echo, server_closes, client_leaves, early_frames, invalid_upgrade,
                  released)
    assert results[0] == ["text", b"\x00binary\xff", 200_000]
    assert results[1] == ("hello ws", 1000)
    assert results[2].endswith(":CancelledError") or results[2].endswith(":ConnectionClosedError")
    assert results[3] == (b"HTTP/1.1 101 Switching Protocols", b"\x81\x02hi")
    assert results[4] == b"HTTP/1.1 426 Upgrade Required"
    # Only the /state request's own connection is registered; no tunnels remain.
    assert results[5].split(":")[:2] == ["1", "0"]


def test_websocket_backpressure_pauses_reading_the_peer(monkeypatch):
    from oldman_server.native import host as seastar_sanic
    pauses = []
    pause = seastar_sanic._NativeTunnel.pause
    monkeypatch.setattr(seastar_sanic._NativeTunnel, "pause",
                        lambda self, paused: (pauses.append(paused), pause(self, paused)))

    async def flood(port):
        async with connect(f"ws://127.0.0.1:{port}/slow-reader", max_size=None) as ws:
            await ws.send("go")
            for _ in range(200):
                await ws.send(b"x" * 65536)
            # The handler does not read for 0.5 s: its queue fills and the
            # tunnel stops the shard reading (pause_reading) until it reads again.
            return await ws.recv()

    results = run("ws_pause", flood)
    assert results[0] == str(200 * 65536)
    assert True in pauses and pauses[-1] is False


def test_a_late_tunnel_call_reaches_only_its_own_engine():
    # The WebSocket's close timeout aborts the transport 10 s later; by then
    # the process may serve again with another Engine whose request ids
    # repeat. The tunnel keeps acting on the Engine it was opened on.
    from types import SimpleNamespace
    from unittest.mock import Mock
    from oldman_server.native import host

    first, second = Mock(closed=False), Mock(closed=False)
    exchange = SimpleNamespace(native=SimpleNamespace(request_id=7, shard=0), max_body=64,
                               streaming="open", stage=None)
    request = SimpleNamespace(app=SimpleNamespace(config=SimpleNamespace(ACCESS_LOG=False)),
                              conn_info=None)
    previous, host._engine = host._engine, first
    try:
        tunnel = host._NativeTunnel(exchange, request, None)
        host._engine = second
        tunnel.abort()
    finally:
        host._engine = previous
    first.stream_abort.assert_called_once_with(7, 0)
    second.stream_abort.assert_not_called()
    first.closed = True
    tunnel.abort()  # a closed Engine is left alone
    first.stream_abort.assert_called_once()
