"""build/http_load, the load generator of the comparison, against an aiohttp server."""
import asyncio
import json
import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.asyncio
@pytest.mark.parametrize("rate,seconds,offered", [("1000", ".12", 120), ("33868.8", ".234375", 7938)])
async def test_scheduled_client_exposes_overload_and_checks_pid_set(tmp_path, rate, seconds, offered):
    from aiohttp import web
    async def delayed(request):
        await asyncio.sleep(.02)
        return web.Response(body=b"expected", headers={"X-Probe-Pid": str(os.getpid())})
    app = web.Application()
    app.router.add_get("/", delayed)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    expected = tmp_path / "expected.bin"
    expected.write_bytes(b"expected")
    cpus = ",".join(map(str, sorted(os.sched_getaffinity(0))[:2]))
    async def run(pids):
        proc = await asyncio.create_subprocess_exec(str(ROOT / "build/http_load"),
            "--smp", "2", "--cpuset", cpus, "--port", str(port), "--target-pids", pids, "--path", "/", "--expected", str(expected),
            "--rate", rate, "--concurrency", "2", "--warmup", ".02", "--seconds", seconds,
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await asyncio.wait_for(proc.communicate(b"start\n"), 10)
        rows = [json.loads(line) for line in stdout.decode().splitlines() if line.startswith("{")]
        assert rows, (stdout, stderr)
        return proc.returncode, rows[-1]
    try:
        code, row = await run(f"{os.getpid()},9999999")
        assert code == 0 and row["integrity_errors"] == 0
        # Second case scales264.6RPS*30s by an exact power of2 to reproduce
        # the same floating endpoint in a short regression.
        assert row["scheduled"] == offered and row["dropped"] > 50
        assert row["requests"] + row["errors"] + row["dropped"] == row["scheduled"]
        assert row["p99_us"] >= 19000 and row["new_connections"] == 0
        assert row["server_pids"] == {str(os.getpid()): row["requests"]}
        code, row = await run("9999999")
        assert code == 1 and row["integrity_errors"] > 0 and row["requests"] == 0
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_scheduled_client_uses_free_connection_before_busy_lane(tmp_path):
    from aiohttp import web
    delays = {}
    async def variable(request):
        peer = request.transport.get_extra_info("peername")
        if peer not in delays: delays[peer] = .08 if not delays else .001
        await asyncio.sleep(delays[peer])
        return web.Response(body=b"ok", headers={"X-Probe-Pid": str(os.getpid())})
    app = web.Application(); app.router.add_get("/", variable)
    runner = web.AppRunner(app); await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0); await site.start()
    port = site._server.sockets[0].getsockname()[1]
    expected = tmp_path/"expected.bin"; expected.write_bytes(b"ok")
    cpu = str(min(os.sched_getaffinity(0)))
    try:
        proc = await asyncio.create_subprocess_exec(str(ROOT/"build/http_load"),
            "--smp", "1", "--cpuset", cpu, "--port", str(port), "--target-pid", str(os.getpid()), "--path", "/", "--expected", str(expected),
            "--rate", "50", "--concurrency", "2", "--warmup", ".15", "--seconds", ".4",
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        out, err = await asyncio.wait_for(proc.communicate(b"start\n"), 5)
        row = [json.loads(line) for line in out.decode().splitlines() if line.startswith("{")][-1]
        assert proc.returncode == 0, err
        assert row["scheduled"] == 20 and row["dropped"] == 0, row
        assert row["new_connections"] == 0 and len(delays) == 2
        assert row["requests"] == 20 and row["p99_us"] < 120000
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_closed_loop_reads_chunked_bodies_on_kept_connections(tmp_path):
    from aiohttp import web
    async def chunked(request):
        response = web.StreamResponse(headers={"X-Probe-Pid": str(os.getpid()), "X-Probe-Worker": "3"})
        response.enable_chunked_encoding()
        await response.prepare(request)
        for part in (b"chun", b"ked ", b"body"):
            await response.write(part)
        await response.write_eof()
        return response
    app = web.Application(); app.router.add_post("/", chunked)
    runner = web.AppRunner(app); await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0); await site.start()
    port = site._server.sockets[0].getsockname()[1]
    expected = tmp_path/"expected.bin"; expected.write_bytes(b"chunked body")
    payload = tmp_path/"input.bin"; payload.write_bytes(b"x" * 100)
    cpu = str(min(os.sched_getaffinity(0)))
    try:
        proc = await asyncio.create_subprocess_exec(str(ROOT/"build/http_load"),
            "--smp", "1", "--cpuset", cpu, "--port", str(port), "--target-pid", str(os.getpid()),
            "--path", "/", "--method", "POST", "--input", str(payload), "--expected", str(expected),
            "--concurrency", "2", "--warmup", ".05", "--seconds", ".2",
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        out, err = await asyncio.wait_for(proc.communicate(b"start\n"), 5)
        row = [json.loads(line) for line in out.decode().splitlines() if line.startswith("{")][-1]
        assert proc.returncode == 0, err
        assert row["requests"] > 0 and row["errors"] == 0 and row["integrity_errors"] == 0, row
        assert row["response_bytes"] == row["requests"] * len(b"chunked body")
        assert row["total_connections"] == 2 and row["new_connections"] == 0
        assert row["server_workers"] == {"3": row["requests"]}
    finally:
        await runner.cleanup()
