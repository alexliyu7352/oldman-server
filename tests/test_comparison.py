import contextlib
import asyncio
import http.client
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import zlib

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from run_http_benchmarks import read_json, send


@contextlib.contextmanager
def service(mode, requests=96, byte_limit=256*1048576):
    cpus = sorted(os.sched_getaffinity(0))[:6]
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    p = subprocess.Popen(["taskset", "-c", ",".join(map(str, cpus)), sys.executable,
        str(ROOT / "scripts/comparison_server.py"), "--mode", mode, "--port", str(port),
        "--requests", str(requests), "--bytes", str(byte_limit)], stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)
    try:
        ready = read_json(p)
        assert ready["ready"]
        yield p, port, ready
        send(p, "quit")
        closed = read_json(p)
        assert closed["closed"] and all(x["active_requests"] == 0 for x in closed["workers"])
        assert all(x["reserved_bytes"] == 0 for x in closed["workers"])
        assert p.wait(10) == 0, p.stderr.read().decode()
    finally:
        if p.poll() is None:
            p.kill()
            p.wait()


@pytest.mark.parametrize("mode", ["direct", "pool", "multiprocess", "seastar", "seastar-shared"])
def test_comparison_equal_bytes_budget_and_process_topology(mode):
    with service(mode) as (p, port, ready):
        assert len(ready["pids"]) == (6 if mode == "multiprocess" else 1)
        assert len(ready["service_pids"]) == (7 if mode == "multiprocess" else 1)
        assert sum(w["request_limit"] for w in ready["workers"]) == 96
        assert sum(w["byte_limit"] for w in ready["workers"]) == 256*1048576
        send(p, "begin"); assert read_json(p)["begun"]
        body = bytes(range(256))*4096
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        for _ in range(2):
            conn.request("POST", "/hybrid/compress", body)
            response = conn.getresponse()
            assert response.status == 200
            assert int(response.getheader("X-Probe-Pid")) in ready["pids"]
            assert response.getheader("Transfer-Encoding") == "chunked"
            assert response.read() == zlib.compress(body, 6)
        conn.close()
        send(p, "end"); end = read_json(p)
        assert sum(w["delta"]["responses"] for w in end["workers"]) == 2
        assert all(w["after"]["active_requests"] == 0 for w in end["workers"])
        for worker in end["workers"]:
            assert all(set(t["affinity"]) <= set(ready["server_cpus"]) for t in worker["threads"])
        if mode == "pool":
            assert sorted(w["affinity"] for w in ready["workers"][0]["compute_workers"]) == [[i] for i in ready["server_cpus"][1:]]


def test_comparison_budget_rejects_and_releases():
    from comparison_server import Budget
    b = Budget(1, 100)
    assert b.acquire(80)
    assert not b.acquire(1)
    assert not b.acquire(101)
    b.release(80)
    assert b.active == 0 and b.reserved == 0
    assert b.acquire(100)
    b.release(100)


def test_python_adapter_body_limits_and_partial_shutdown():
    with service("pool") as (p, port, ready):
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        conn.putrequest("POST", "/hybrid/compress")
        conn.putheader("Content-Length", "1048577")
        conn.endheaders()
        response = conn.getresponse()
        assert response.status == 413
        response.read(); conn.close()
        partial = socket.create_connection(("127.0.0.1", port))
        partial.sendall(b"POST /hybrid/compress HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\n\r\nx")
        # service shutdown must cancel an unfinished upload, not wait forever.
    partial.close()


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
            "--smp", "2", "--cpuset", cpus, "--reactor-backend", "epoll", "--overprovisioned",
            "--default-log-level", "error", "--enable-glibc-exception-scaling-workaround", "0",
            "--port", str(port), "--target-pids", pids, "--path", "/", "--expected", str(expected),
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


def test_comparison_rejects_corrupt_results_and_changed_manifest():
    from run_comparison import validate_manifest, validate_row
    for field in ("fingerprints", "python", "kernel", "zlib", "native_zlib", "native_libraries"):
        with pytest.raises(ValueError, match=field):
            validate_manifest({field: "old"}, {field: "new"})
    with pytest.raises(ValueError):
        validate_row({"key": "x", "returncodes": [0, 0], "client": {"integrity_errors": 1}}, "x")


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
            "--smp", "1", "--cpuset", cpu, "--reactor-backend", "epoll", "--overprovisioned",
            "--default-log-level", "error", "--enable-glibc-exception-scaling-workaround", "0",
            "--port", str(port), "--target-pid", str(os.getpid()), "--path", "/", "--expected", str(expected),
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
