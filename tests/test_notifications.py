import asyncio
from dataclasses import replace
import pytest
from oldman_server.native import engine as api
from test_http import config, request, wait_stat


@pytest.mark.asyncio
async def test_coalesced_notifications_rearm_partial_http_and_completion_batches():
    assert "coalesce_notifications" in api.ServerConfig.__dataclass_fields__, "coalescing is missing"
    cfg = replace(config(coalesce_notifications=True), batch_decisions=True, completion_batch=3)
    engine = await api.Engine.open(cfg)
    loop = asyncio.get_running_loop()
    try:
        # Both queues must make progress even when one callback cannot drain all work.
        for _ in range(4):
            loop.remove_reader(engine.notification_fd)
            jobs = [asyncio.create_task(request(cfg.server.http_port, "/hybrid/fixed?size=8")) for _ in range(12)]
            echoes = [asyncio.create_task(engine.echo(bytes([i]), delay_ms=1)) for i in range(8)]
            await wait_stat(engine, "queued_decisions", lambda n:n == 12)
            engine._drain()
            loop.add_reader(engine.notification_fd, engine._drain)
            assert all(r[0] == 200 and r[2] == b"x"*8 for r in await asyncio.gather(*jobs))
            assert [r.payload for r in await asyncio.gather(*echoes)] == [bytes([i]) for i in range(8)]
        await wait_stat(engine, "active_requests", lambda n:n == 0)
        assert engine.server_stats()["python_decisions"] == 48
        assert engine.server_stats()["notification_writes"] < 48+32
    finally:
        loop.add_reader(engine.notification_fd, engine._drain)
        await engine.aclose()
    assert engine.server_stats()["pending_decisions"] == engine.server_stats()["queued_decisions"] == 0
