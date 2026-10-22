"""Real loopback HTTP tests, one native runtime per test subprocess."""
import asyncio
import http.client
import os
from pathlib import Path
import random
import socket
import zlib

import pytest
from oldman_server.native import engine as api


def free_port(kind=socket.SOCK_STREAM):
    with socket.socket(socket.AF_INET, kind) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def config(**kwargs):
    assert hasattr(api, "ServerConfig"), "native HTTP configuration is missing"
    cpus = tuple(sorted(os.sched_getaffinity(0))[-4:])
    server = api.ServerConfig(http_port=free_port(), **kwargs)
    return api.Config(shard_cpus=cpus[:2], worker_cpus=cpus[2:], server=server)


async def request(port, path, body=None, connection=None):
    def send():
        c = connection or http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        try:
            c.request("POST" if body is not None else "GET", path, body=body)
            r = c.getresponse()
            return r.status, dict((k.lower(), v) for k, v in r.getheaders()), r.read()
        finally:
            if connection is None:
                c.close()
    return await asyncio.to_thread(send)


async def wait_stat(engine, key, predicate):
    async with asyncio.timeout(5):
        while not predicate(engine.server_stats()[key]):
            await asyncio.sleep(.001)


@pytest.mark.asyncio
async def test_http_native_hybrid_and_copy_preserve_bytes_and_origin():
    cfg = config()
    seen = []
    def decide(req):
        seen.append((req.route, req.body_size, req.body))
        return req.method == "POST" and req.route == "compress"
    engine = await api.Engine.open(cfg, decision_handler=decide)
    try:
        payload = random.Random(17).randbytes(65536)
        replies = await asyncio.gather(*(request(cfg.server.http_port, f"/{mode}/compress", payload)
                                        for mode in ("native", "hybrid", "copy") for _ in range(4)))
        worker_tids = {w["tid"] for w in engine.workers()}
        for status, headers, body in replies:
            assert status == 200
            assert body == zlib.compress(payload, 6)
            assert zlib.decompress(body) == payload
            assert int(headers["x-probe-pid"]) == os.getpid()
            assert int(headers["x-probe-worker"]) in worker_tids
        assert {int(h["x-probe-shard"]) for _, h, _ in replies} == {0, 1}
        assert len(seen) == 8
        assert sum(body is None for _, _, body in seen) == 4
        assert all(body in (None, payload) for _, _, body in seen)
        await wait_stat(engine, "active_requests", lambda n: n == 0)
        stats = engine.server_stats()
        assert stats["body_bytes_to_python"] == len(payload) * 4
        assert stats["body_bytes_from_python"] == len(payload) * 4
    finally:
        await engine.aclose()
    assert engine.server_stats()["active_requests"] == 0
    assert engine.server_stats()["reserved_bytes"] == 0


@pytest.mark.asyncio
async def test_http_keepalive_fixed_file_and_metadata_only(tmp_path):
    contents = random.Random(23).randbytes(1048576)
    file = tmp_path / "asset.bin"
    file.write_bytes(contents)
    cfg = config(file_path=str(file))
    seen = []
    def decide(req):
        seen.append(req)
        return req.method == "GET" and req.route in ("fixed", "file")
    engine = await api.Engine.open(cfg, decision_handler=decide)
    c = http.client.HTTPConnection("127.0.0.1", cfg.server.http_port, timeout=5)
    try:
        original_socket = None
        for mode in ("native", "hybrid"):
            status, _, body = await request(cfg.server.http_port, f"/{mode}/fixed?size=65536", connection=c)
            assert status == 200 and body == b"x" * 65536
            status, _, body = await request(cfg.server.http_port, f"/{mode}/file", connection=c)
            assert status == 200 and body == contents
            if original_socket is None:
                original_socket = c.sock
            assert c.sock is original_socket and c.sock is not None
        assert all(r.body is None for r in seen)
        assert len(seen) == 2
        assert engine.server_stats()["body_bytes_to_python"] == 0
    finally:
        c.close()
        await engine.aclose()


@pytest.mark.asyncio
async def test_http_bad_decision_is_explicit_and_native_path_survives():
    cfg = config()
    calls = 0
    async def unsupported():
        return True
    def decide(req):
        nonlocal calls
        calls += 1
        if calls == 1:
            return False
        if calls == 2:
            raise RuntimeError("intentional callback failure")
        if calls == 3:
            return unsupported()
        return True
    engine = await api.Engine.open(cfg, decision_handler=decide)
    try:
        statuses = [(await request(cfg.server.http_port, "/hybrid/fixed?size=1"))[0] for _ in range(4)]
        assert statuses == [403, 500, 500, 200]
        assert (await request(cfg.server.http_port, "/native/fixed?size=3"))[2] == b"xxx"
        await wait_stat(engine, "active_requests", lambda n: n == 0)
        assert engine.server_stats()["reserved_bytes"] == 0
    finally:
        await engine.aclose()


@pytest.mark.asyncio
async def test_http_decision_timeout_removes_queued_metadata_and_late_reply():
    cfg = config(decision_timeout_ms=30)
    engine = await api.Engine.open(cfg)
    loop = asyncio.get_running_loop()
    try:
        loop.remove_reader(engine.notification_fd)
        status, _, _ = await request(cfg.server.http_port, "/hybrid/fixed?size=1")
        assert status == 504
        assert engine.server_stats()["queued_decisions"] == 0
        assert engine.server_stats()["pending_decisions"] == 0
        assert engine.server_stats()["active_requests"] == 0
        # Arbitrary stale IDs cannot touch a destroyed promise.
        engine._native.resolve_request(12345678, 0, 1, b"")
    finally:
        loop.add_reader(engine.notification_fd, engine._drain)
        await engine.aclose()


@pytest.mark.asyncio
async def test_http_capacity_oversize_and_shutdown_of_partial_upload():
    cfg = config(max_requests=2, max_bytes=8192, max_body_bytes=1024, decision_timeout_ms=1000)
    engine = await api.Engine.open(cfg)
    loop = asyncio.get_running_loop()
    writers = []
    jobs = []
    try:
        assert (await request(cfg.server.http_port, "/native/compress", b"a" * 1025))[0] == 413
        assert (await request(cfg.server.http_port, "/native/fixed?size=1025"))[0] == 413
        loop.remove_reader(engine.notification_fd)
        jobs = [asyncio.create_task(request(cfg.server.http_port, "/hybrid/fixed?size=32")) for _ in range(8)]
        await wait_stat(engine, "active_requests", lambda n: n == 2)
        await asyncio.sleep(.05)
        done = [j for j in jobs if j.done()]
        assert done and all(j.result()[0] == 503 for j in done)
        loop.add_reader(engine.notification_fd, engine._drain)
        statuses = [r[0] for r in await asyncio.gather(*jobs)]
        assert statuses.count(200) == 2 and statuses.count(503) == 6
        await wait_stat(engine, "active_requests", lambda n: n == 0)
        reader, writer = await asyncio.open_connection("127.0.0.1", cfg.server.http_port)
        writers.append(writer)
        writer.write(b"POST /native/compress HTTP/1.1\r\nHost: local\r\nContent-Length: 1024\r\n\r\nx")
        await writer.drain()
        await wait_stat(engine, "active_requests", lambda n: n == 1)
        await asyncio.wait_for(engine.aclose(), 5)
        assert engine.server_stats()["active_requests"] == 0
        assert engine.server_stats()["reserved_bytes"] == 0
    finally:
        if engine._native.fd() >= 0:
            loop.add_reader(engine.notification_fd, engine._drain)
            await engine.aclose()
        for writer in writers:
            writer.close()
            await writer.wait_closed()


@pytest.mark.asyncio
async def test_http_port_collision_unwinds_startup():
    cfg = config()
    before = len(list(Path("/proc/self/fd").iterdir()))
    with socket.socket() as occupied:
        occupied.bind(("127.0.0.1", cfg.server.http_port))
        occupied.listen()
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(api.Engine.open(cfg), 10)
    # asyncio.to_thread may initialize process infrastructure; existing stage1
    # tests check exact FD cleanup. Here ensure no service listener or worker survives.
    assert len(list(Path("/proc/self/fd").iterdir())) <= before + 1


@pytest.mark.asyncio
async def test_http_chunked_body_explicitly_requires_content_length():
    cfg = config(max_body_bytes=1024)
    engine = await api.Engine.open(cfg)
    async def chunked(parts):
        def send():
            c = http.client.HTTPConnection("127.0.0.1", cfg.server.http_port, timeout=5)
            try:
                try:
                    c.request("POST", "/hybrid/compress", body=iter(parts), encode_chunked=True)
                except BrokenPipeError:
                    # A header-only rejection may close while chunks are still
                    # being sent. The response must still be read and checked.
                    pass
                r = c.getresponse()
                return r.status, r.read()
            finally:
                c.close()
        return await asyncio.to_thread(send)
    try:
        parts = [b"\0\xff" * 200, b"xyz" * 208]
        status, body = await chunked(parts)
        assert status == 411
        status, _ = await chunked(parts + [b"!"])
        assert status == 411
        await wait_stat(engine, "active_requests", lambda n: n == 0)
        assert engine.server_stats()["reserved_bytes"] == 0
        assert engine.server_stats()["decisions"] == engine.server_stats()["compute_ns"] == 0
    finally:
        await engine.aclose()


@pytest.mark.asyncio
async def test_http_half_closed_incomplete_body_never_executes():
    cfg = config()
    engine = await api.Engine.open(cfg)
    try:
        for mode in ("native", "hybrid", "copy"):
            for header, body, expected in ((b"Content-Length: 1024", b"x", 400),
                    (b"Transfer-Encoding: chunked", b"1\r\nx\r\n", 411)):
                reader, writer = await asyncio.open_connection("127.0.0.1", cfg.server.http_port)
                try:
                    writer.write(f"POST /{mode}/compress HTTP/1.1\r\nHost: local\r\n".encode()+header+b"\r\n\r\n"+body)
                    await writer.drain()
                    writer.write_eof()
                    status = await asyncio.wait_for(reader.readline(), 3)
                    assert int(status.split()[1]) == expected, status
                    await asyncio.wait_for(reader.read(), 3)
                finally:
                    writer.close()
                    await writer.wait_closed()
        await wait_stat(engine, "active_requests", lambda n: n == 0)
        stats = engine.server_stats()
        assert stats["reserved_bytes"] == stats["decisions"] == stats["compute_ns"] == 0
    finally:
        await engine.aclose()


@pytest.mark.asyncio
async def test_http_worker_queue_exhaustion_reports_capacity():
    from dataclasses import replace
    cfg = config()
    cfg = replace(cfg, worker_cpus=cfg.worker_cpus[:1], max_pending=1)
    engine = await api.Engine.open(cfg)
    blocked = asyncio.create_task(engine.blocking_wait(300))
    try:
        async with asyncio.timeout(3):
            while engine.stats()["pool_active"] != 1:
                await asyncio.sleep(.001)
        jobs = [asyncio.create_task(request(cfg.server.http_port, "/native/compress", b"abc")) for _ in range(8)]
        replies = await asyncio.gather(*jobs)
        assert sorted(r[0] for r in replies) == [200] + [503]*7
        await blocked
        await wait_stat(engine, "active_requests", lambda n: n == 0)
        assert engine.server_stats()["reserved_bytes"] == 0
    finally:
        await engine.aclose()
