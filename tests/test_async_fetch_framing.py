"""Guard against a valid-sized payload with invalid HTTP message completion."""
import asyncio
from dataclasses import replace

import pytest
from oldman_server.native import engine as api
from test_http import config, request, wait_stat


@pytest.mark.asyncio
async def test_fetch_rejects_missing_terminal_chunk_and_partial_chunk():
    kind = "valid"
    async def origin(reader, writer):
        try:
            await reader.readuntil(b"\r\n\r\n")
            writer.write(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n2000\r\n")
            writer.write(b"x" * (100 if kind == "partial" else 8192))
            if kind != "partial": writer.write(b"\r\n")
            if kind == "valid": writer.write(b"0\r\n\r\n")
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
    server = await asyncio.start_server(origin, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    cfg = replace(config(upstream_ports=(port,)), worker_cpus=())
    async def business(req):
        result = await req.fetch()
        return api.HttpResponse(body=result)
    engine = await api.Engine.open(cfg, http_handler=business)
    try:
        for mode in ("reference", "python"):
            for kind, expected in [("valid", 200), ("missing_end", 502), ("partial", 502)]:
                reply = await request(cfg.server.http_port, f"/{mode}/fetch")
                assert reply[0] == expected, (mode, kind, reply[0])
        await wait_stat(engine, "active_requests", lambda n: n == 0)
    finally:
        await engine.aclose()
        server.close(); await server.wait_closed()
    assert engine.server_stats()["reserved_bytes"] == 0


@pytest.mark.asyncio
async def test_fetch_timeout_waiting_for_terminal_chunk_is_not_success():
    tasks = set()
    async def origin(reader, writer):
        task = asyncio.current_task(); tasks.add(task)
        try:
            await reader.readuntil(b"\r\n\r\n")
            writer.write(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2000\r\n" + b"x" * 8192 + b"\r\n")
            await writer.drain()
            await asyncio.sleep(.2)
        finally:
            writer.close(); await writer.wait_closed(); tasks.discard(task)
    server = await asyncio.start_server(origin, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    cfg = replace(config(upstream_ports=(port,), decision_timeout_ms=40), worker_cpus=())
    async def business(req):
        result = await req.fetch()
        return api.HttpResponse(body=result)
    engine = await api.Engine.open(cfg, http_handler=business)
    try:
        for mode in ("reference", "python"):
            assert (await request(cfg.server.http_port, f"/{mode}/fetch"))[0] == 504
        await wait_stat(engine, "active_requests", lambda n: n == 0)
    finally:
        await engine.aclose()
        server.close(); await server.wait_closed()
        if tasks: await asyncio.gather(*tasks)
    assert engine.server_stats()["reserved_bytes"] == 0
