"""Owned connection metadata; each lifecycle runs in its own process."""
import asyncio

import pytest
from oldman_server.native import engine as api
from test_http import config
from test_framework_dispatch import assert_released


def test_synthetic_request_metadata_is_unknown_and_readonly():
    req = api.ApplicationRequest("GET", b"/", (), b"")
    assert req.http_version == ""
    assert req.client is None and req.server is None
    for name in ("http_version", "client", "server"):
        with pytest.raises(AttributeError):
            setattr(req, name, None)


@pytest.mark.asyncio
async def test_real_request_metadata_survives_await_and_runtime_close():
    assert hasattr(api.ApplicationRequest, "http_version")
    cfg = config(application_dispatch_all=True)
    kept, expected = [], []
    async def handler(req):
        await asyncio.sleep(0)
        kept.append(req)
        return 200, b"ok", ()
    engine = await api.Engine.open(cfg, application_handler=handler)
    try:
        for version in ("1.0", "1.1"):
            reader, writer = await asyncio.open_connection("127.0.0.1", cfg.server.http_port)
            expected.append((version, writer.get_extra_info("sockname"), writer.get_extra_info("peername")))
            try:
                writer.write(f"GET /metadata HTTP/{version}\r\nHost: local\r\nConnection: close\r\n\r\n".encode())
                await writer.drain()
                response = await asyncio.wait_for(reader.read(), 3)
                assert response.split(b"\r\n", 1)[0].split()[1] == b"200", response
            finally:
                writer.close()
                await writer.wait_closed()
    finally:
        await engine.aclose()
    assert [(req.http_version, req.client, req.server) for req in kept] == expected
    assert_released(engine)
