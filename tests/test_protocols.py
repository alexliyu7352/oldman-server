"""Native sockets: binary framing, multiple flows, bounds, shutdown."""
import asyncio
import json
import os
from pathlib import Path
import random
import socket
import struct

import pytest
from oldman_server.native import engine as api
from test_http import free_port, wait_stat


def config(**kwargs):
    cpus = tuple(sorted(os.sched_getaffinity(0))[-4:])
    return api.Config(shard_cpus=cpus[:2], worker_cpus=cpus[2:],
                      server=api.ServerConfig(tcp_port=free_port(),
                                              udp_port=free_port(socket.SOCK_DGRAM), **kwargs))


@pytest.mark.asyncio
async def test_tcp_split_coalesced_binary_frames_and_shards():
    cfg = config()
    engine = await api.Engine.open(cfg)
    async def flow(seed):
        reader, writer = await asyncio.open_connection("127.0.0.1", cfg.server.tcp_port)
        bodies = [b"", b"\0\xff", random.Random(seed).randbytes(65536)]
        wire = b"".join(struct.pack("!I", len(b)) + b for b in bodies)
        try:
            for cut in (1, 2, 5):
                writer.write(wire[:cut])
                wire = wire[cut:]
                await writer.drain()
                await asyncio.sleep(.001)
            writer.write(wire)
            await writer.drain()
            for body in bodies:
                size = struct.unpack("!I", await reader.readexactly(4))[0]
                assert size == len(body)
                assert await reader.readexactly(size) == body
        finally:
            writer.close()
            await writer.wait_closed()
    try:
        await asyncio.wait_for(asyncio.gather(*(flow(i) for i in range(16))), 10)
        await wait_stat(engine, "tcp_connections", lambda n: n == 0)
        stats = engine.server_stats()
        assert stats["tcp_frames"] == 48
        assert all(s["tcp_frames"] > 0 for s in stats["shards"])
        assert stats["reserved_bytes"] == 0
        Path("results/phase3-tcp.json").write_text(json.dumps(stats, indent=2))
    finally:
        await engine.aclose()


@pytest.mark.asyncio
async def test_tcp_oversize_and_shutdown_partial_frame():
    cfg = config(max_body_bytes=1024)
    engine = await api.Engine.open(cfg)
    writers = []
    try:
        r, w = await asyncio.open_connection("127.0.0.1", cfg.server.tcp_port)
        writers.append(w)
        w.write(struct.pack("!I", 1025))
        await w.drain()
        assert await asyncio.wait_for(r.read(), 2) == b""
        r, w = await asyncio.open_connection("127.0.0.1", cfg.server.tcp_port)
        writers.append(w)
        w.write(struct.pack("!I", 1024) + b"x")
        await w.drain()
        await wait_stat(engine, "active_requests", lambda n: n == 1)
        await asyncio.wait_for(engine.aclose(), 5)
        stats = engine.server_stats()
        assert stats["reserved_bytes"] == stats["active_requests"] == stats["tcp_connections"] == 0
    finally:
        await engine.aclose()
        for w in writers:
            w.close()
            await w.wait_closed()


@pytest.mark.asyncio
async def test_tcp_slow_reader_holds_frame_until_shutdown():
    cfg = config()
    engine = await api.Engine.open(cfg)
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    sock.setblocking(False)
    loop = asyncio.get_running_loop()
    try:
        await loop.sock_connect(sock, ("127.0.0.1", cfg.server.tcp_port))
        body = b"x" * 1048576
        await loop.sock_sendall(sock, struct.pack("!I", len(body)) + body)
        await wait_stat(engine, "active_requests", lambda n: n == 1)
        await asyncio.sleep(.1)
        assert engine.server_stats()["active_requests"] == 1
        await asyncio.wait_for(engine.aclose(), 5)
        stats = engine.server_stats()
        assert stats["active_requests"] == stats["reserved_bytes"] == stats["tcp_connections"] == 0
    finally:
        sock.close()
        await engine.aclose()


@pytest.mark.asyncio
async def test_udp_many_flows_exact_datagrams_and_distribution():
    cfg = config()
    engine = await api.Engine.open(cfg)
    loop = asyncio.get_running_loop()
    async def flow(flow_id):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setblocking(False)
        sock.bind(("127.0.0.1", 0))
        sock.connect(("127.0.0.1", cfg.server.udp_port))
        received = []
        try:
            # A window of8 per flow,32 independent source ports. Identity and
            # sequence are in every datagram; compare wire bytes as well.
            for base in range(0, 64, 8):
                expected = [struct.pack("!II", flow_id, n) + bytes([n]) * 1016 for n in range(base, base + 8)]
                for body in expected:
                    await loop.sock_sendall(sock, body)
                replies = [await asyncio.wait_for(loop.sock_recv(sock, 65536), 3) for _ in expected]
                assert replies == expected
                received.extend(struct.unpack("!II", b[:8])[1] for b in replies)
            assert received == list(range(64))
            return len(received)
        finally:
            sock.close()
    try:
        counts = await asyncio.gather(*(flow(i) for i in range(32)))
        stats = engine.server_stats()
        assert stats["udp_received"] == stats["udp_sent"] == sum(counts) == 2048
        assert stats["udp_dropped"] == 0
        assert all(s["udp_received"] > 0 for s in stats["shards"])
        Path("results/phase3-udp.json").write_text(json.dumps({"flows": 32, "sent": 2048,
            "received": sum(counts), "duplicates": 0, "reordered": 0, "loss": 0, "stats": stats}, indent=2))
    finally:
        await asyncio.wait_for(engine.aclose(), 5)


@pytest.mark.asyncio
async def test_http_slow_reader_holds_budget_and_shutdown_releases(tmp_path):
    from test_http import config as http_config
    asset = tmp_path / "large.bin"
    with asset.open("wb") as f:
        f.truncate(32 * 1048576)
    cfg = http_config(file_path=str(asset))
    engine = await api.Engine.open(cfg)
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    sock.setblocking(False)
    loop = asyncio.get_running_loop()
    try:
        await loop.sock_connect(sock, ("127.0.0.1", cfg.server.http_port))
        await loop.sock_sendall(sock, b"GET /hybrid/file HTTP/1.1\r\nHost: local\r\n\r\n")
        await wait_stat(engine, "active_requests", lambda n: n == 1)
        await asyncio.sleep(.1)
        assert engine.server_stats()["active_requests"] == 1
        assert engine.server_stats()["reserved_bytes"] == 131072
        await asyncio.wait_for(engine.aclose(), 5)
        assert engine.server_stats()["active_requests"] == engine.server_stats()["reserved_bytes"] == 0
    finally:
        sock.close()
        await engine.aclose()
