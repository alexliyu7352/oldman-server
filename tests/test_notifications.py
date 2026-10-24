import asyncio
from dataclasses import replace
import pytest
from oldman_server.native import engine as api
from test_http import config, request, wait_stat


@pytest.mark.asyncio
async def test_coalesced_notifications_rearm_partial_request_batches():
    assert "coalesce_notifications" in api.ServerConfig.__dataclass_fields__, "coalescing is missing"
    cfg = replace(config(coalesce_notifications=True, application_dispatch_all=True), completion_batch=3)
    engine = await api.Engine.open(cfg, application_handler=lambda req: (200, b"x" * 8, ()))
    loop = asyncio.get_running_loop()
    try:
        # The queue must make progress even when one callback cannot drain all of it.
        for _ in range(4):
            # The previous round's connections have closed and their events are drained.
            await wait_stat(engine, "connections_open", lambda n: n == 0)
            await wait_stat(engine, "queued_decisions", lambda n: n == 0)
            loop.remove_reader(engine.notification_fd)
            jobs = [asyncio.create_task(request(cfg.server.http_port, "/fixed")) for _ in range(12)]
            await wait_stat(engine, "queued_decisions", lambda n: n == 12)
            engine._drain()
            loop.add_reader(engine.notification_fd, engine._drain)
            assert all(r[0] == 200 and r[2] == b"x" * 8 for r in await asyncio.gather(*jobs))
        await wait_stat(engine, "active_requests", lambda n: n == 0)
        assert engine.server_stats()["python_application_finished"] == 48
        # 48 requests and 48 connection closes: fewer wakes than events.
        assert engine.server_stats()["notification_writes"] < 96
    finally:
        loop.add_reader(engine.notification_fd, engine._drain)
        await engine.aclose()
    assert engine.server_stats()["pending_decisions"] == engine.server_stats()["queued_decisions"] == 0
