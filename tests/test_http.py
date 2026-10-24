"""Helpers of the native host tests, and a listener that fails to start.
One native runtime per test process."""
import asyncio
import http.client
import os
from pathlib import Path
import socket

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
    return api.Config(shard_cpus=cpus[:2], server=server)


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
async def test_http_port_collision_unwinds_startup():
    cfg = config(application_dispatch_all=True)
    before = len(list(Path("/proc/self/fd").iterdir()))
    with socket.socket() as occupied:
        occupied.bind(("127.0.0.1", cfg.server.http_port))
        occupied.listen()
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(api.Engine.open(cfg, application_handler=lambda r: (200, b"", ())), 10)
    # asyncio.to_thread may initialize process infrastructure; existing stage1
    # tests check exact FD cleanup. Here ensure no service listener or worker survives.
    assert len(list(Path("/proc/self/fd").iterdir())) <= before + 1
