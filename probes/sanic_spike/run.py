"""Helpers the comparison drivers and native tests share: an ASGI lifespan
driver, a free loopback port, and waiting for an Engine to go quiescent."""
import asyncio
from contextlib import asynccontextmanager, suppress
import socket


@asynccontextmanager
async def lifespan(app):
    incoming, outgoing = asyncio.Queue(), asyncio.Queue()
    task = asyncio.create_task(app({'type': 'lifespan', 'asgi': {'version': '3.0', 'spec_version': '2.0'}},
                                  incoming.get, outgoing.put))
    started = False
    try:
        await incoming.put({'type': 'lifespan.startup'})
        message = await asyncio.wait_for(outgoing.get(), 5)
        assert message['type'] == 'lifespan.startup.complete', message
        started = True
        yield
    finally:
        try:
            if started:
                await incoming.put({'type': 'lifespan.shutdown'})
                message = await asyncio.wait_for(outgoing.get(), 5)
                assert message['type'] == 'lifespan.shutdown.complete', message
                await asyncio.wait_for(task, 5)
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


async def quiescent(engine):
    async with asyncio.timeout(5):
        while True:
            stats = engine.server_stats()
            if all(stats[key] == 0 for key in ('active_requests', 'reserved_bytes',
                                             'pending_application_nodes', 'python_application_active',
                                             'pending_decisions', 'queued_decisions')):
                return stats
            await asyncio.sleep(.001)
