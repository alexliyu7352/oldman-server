"""Policy execution is native per request; Python updates are acknowledged on all shards."""
import asyncio
import http.client
import json
import os
from pathlib import Path
import sys
import time
import zlib
from dataclasses import replace

import pytest
from oldman_server.native import engine as api
from test_http import config, request, wait_stat


@pytest.mark.asyncio
async def test_native_policy_updates_all_shards_and_preserves_dynamic_fallback(tmp_path):
    assert hasattr(api, "NativePolicy"), "native policy is missing"
    payload = bytes(range(256))*256
    asset = tmp_path / "asset.bin"
    asset.write_bytes(payload)
    cfg = replace(config(file_path=str(asset), coalesce_notifications=True), batch_decisions=True)
    seen = []
    engine = await api.Engine.open(cfg, decision_handler=lambda req: seen.append(req) or True)
    peers = [http.client.HTTPConnection("127.0.0.1", cfg.server.http_port, timeout=5) for _ in range(6)]
    try:
        assert (await request(cfg.server.http_port, "/policy/fixed?size=16"))[0] == 403
        v1 = await engine.install_policy(api.NativePolicy(allow_fixed=True, max_payload_bytes=32))
        replies = await asyncio.gather(*(request(cfg.server.http_port, "/policy/fixed?size=16", connection=p) for p in peers))
        assert {h["x-probe-shard"] for _, h, _ in replies} == {"0", "1"}
        assert all(s == 200 and b == b"x"*16 and int(h["x-probe-policy"]) == v1
                   and int(h["x-probe-pid"]) == os.getpid() for s, h, b in replies)
        assert (await request(cfg.server.http_port, "/policy/fixed?size=33"))[0] == 413
        assert (await request(cfg.server.http_port, "/policy/file"))[0] == 403
        v2 = await engine.install_policy(api.NativePolicy(allow_file=True, max_payload_bytes=len(payload)))
        denied = await asyncio.gather(*(request(cfg.server.http_port, "/policy/fixed?size=16", connection=p) for p in peers))
        assert all(s == 403 and int(h["x-probe-policy"]) == v2 for s, h, _ in denied)
        files = await asyncio.gather(*(request(cfg.server.http_port, "/policy/file", connection=p) for p in peers))
        assert all(s == 200 and b == payload and int(h["x-probe-policy"]) == v2 for s, h, b in files)
        assert not seen and engine.server_stats()["python_decisions"] == 0
        assert (await request(cfg.server.http_port, "/hybrid/fixed?size=16"))[0] == 200
        assert len(seen) == 1
        assert engine.server_stats()["native_policy_decisions"] == 21
        assert engine.server_stats()["native_policy_denied"] == 9
        v3 = await engine.install_policy(api.NativePolicy(allow_compress=True, max_payload_bytes=len(payload)))
        status, headers, compressed = await request(cfg.server.http_port, "/policy/compress", body=payload)
        assert status == 200 and zlib.decompress(compressed) == payload
        assert int(headers["x-probe-policy"]) == v3
        assert int(headers["x-probe-worker"]) in {w["tid"] for w in engine.workers()}
        assert engine.server_stats()["python_decisions"] == 1
        await wait_stat(engine, "active_requests", lambda n:n == 0)
    finally:
        for p in peers: p.close()
        await engine.aclose()
    assert engine.stats()["pending_requests"] == 0


@pytest.mark.asyncio
async def test_native_policy_concurrent_updates_cancellation_and_close():
    assert hasattr(api, "NativePolicy"), "native policy is missing"
    cfg = config(coalesce_notifications=True)
    engine = await api.Engine.open(cfg)
    updates = [asyncio.create_task(engine.install_policy(api.NativePolicy(allow_fixed=bool(i%2)))) for i in range(12)]
    await asyncio.sleep(0)
    updates[3].cancel()
    answers = await asyncio.gather(*updates, return_exceptions=True)
    assert isinstance(answers[3], asyncio.CancelledError)
    assert all(isinstance(v, int) for i,v in enumerate(answers) if i != 3)
    final = await engine.install_policy(api.NativePolicy(allow_fixed=True))
    status, headers, body = await request(cfg.server.http_port, "/policy/fixed?size=7")
    assert status == 200 and body == b"x"*7 and int(headers["x-probe-policy"]) == final
    await engine.aclose()
    assert engine.stats()["pending_requests"] == engine.stats()["pending_bytes"] == 0
    with pytest.raises(RuntimeError, match="closing"):
        await engine.install_policy(api.NativePolicy())


@pytest.mark.asyncio
async def test_native_http_completes_while_python_holds_the_gil(tmp_path):
    """Independent C++ client measures service while this interpreter cannot drain callbacks."""
    cfg = config(coalesce_notifications=True)
    engine = await api.Engine.open(cfg)
    client = None
    available = sorted(os.sched_getaffinity(0))
    old_interval = sys.getswitchinterval()
    try:
        version = await engine.install_policy(api.NativePolicy(allow_fixed=True))
        expected = tmp_path / "expected.bin"
        expected.write_bytes(b"x"*1024)
        cpus = ",".join(map(str, available[:2]))
        client = await asyncio.create_subprocess_exec("taskset", "-c", cpus,
            str(Path("build/http_load").resolve()), "--smp", "2", "--cpuset", cpus,
            "--reactor-backend", "epoll", "--overprovisioned", "--default-log-level", "error",
            "--enable-glibc-exception-scaling-workaround", "0", "--port", str(cfg.server.http_port),
            "--target-pid", str(os.getpid()), "--path", "/policy/fixed?size=1024",
            "--policy-version", str(version), "--expected", str(expected),
            "--concurrency", "16", "--warmup", "0", "--seconds", ".1",
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        async with asyncio.timeout(10):
            while True:
                line = await client.stdout.readline()
                assert line, "client exited before readiness"
                if line.strip() == b"MEASURE_READY": break
        before = engine.server_stats()["responses"]
        os.sched_setaffinity(0, {available[2]})
        sys.setswitchinterval(10.0)
        client.stdin.write(b"start\n")
        await client.stdin.drain()
        start = time.monotonic()
        # No await and no GIL-releasing operation during the measurement window.
        while time.monotonic() - start < .4:
            pass
        held_seconds = time.monotonic() - start
        during = engine.server_stats()["responses"] - before
        sys.setswitchinterval(old_interval)
        stdout, stderr = await asyncio.wait_for(client.communicate(), 10)
        result = next(json.loads(line) for line in stdout.decode().splitlines() if line.startswith("{"))
        assert client.returncode == 0 and result["errors"] == 0, (result, stderr)
        assert during == result["requests"] and during > 100
        assert engine.server_stats()["python_decisions"] == 0
        evidence = dict(gil_hold_seconds=held_seconds, completed_while_gil_held=during,
                        python_decisions=0, server_pid=os.getpid(), client=result, shards=engine.shards())
        Path("results/final-gil-independent.json").write_text(json.dumps(evidence, indent=2)+"\n")
    finally:
        sys.setswitchinterval(old_interval)
        os.sched_setaffinity(0, available)
        if client is not None and client.returncode is None:
            client.kill()
            await client.wait()
        await engine.aclose()
