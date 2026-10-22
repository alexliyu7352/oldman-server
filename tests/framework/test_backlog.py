"""A listening socket queues a burst of connections larger than Sanic's
backlog of 100, as a proxy opening many upstream connections at once makes:
past the backlog, connect() on a unix socket fails (nginx answers 502)."""
import socket
import tempfile
from pathlib import Path

import pytest

from oldman_server.constants import DEFAULT_BACKLOG
from oldman_server.response import text

BURST = 300


@pytest.mark.skipif(
    int(Path("/proc/sys/net/core/somaxconn").read_text()) < BURST,
    reason="net.core.somaxconn caps the backlog below the burst",
)
def test_a_burst_of_connections_waits_in_the_backlog(app):
    # Short: unix socket paths (with the temporary name's suffix) are limited.
    directory = tempfile.TemporaryDirectory(dir="/tmp")
    path = f"{directory.name}/app.sock"

    @app.get("/")
    async def handler(request):
        return text("ok")

    @app.after_server_start
    async def burst(app):
        # Nothing awaits here, so the asyncio server accepts none of them
        # meanwhile (the native host's shards still do).
        sockets, refused = [], 0
        for _ in range(BURST):
            client = socket.socket(socket.AF_UNIX)
            client.setblocking(False)
            try:
                client.connect(path)
            except BlockingIOError:
                refused += 1
            sockets.append(client)
        for client in sockets:
            client.close()
        app.ctx.refused = refused
        app.stop()

    with directory:
        app.run(unix=path, single_process=True, access_log=False)
    assert app.ctx.refused == 0
    assert DEFAULT_BACKLOG > BURST
