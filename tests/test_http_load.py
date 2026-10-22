import asyncio
import json
import os
from pathlib import Path
import random
import zlib

import pytest
from oldman_server.native import engine as api
from test_http import config


@pytest.mark.asyncio
async def test_load_client_detects_wrong_body_and_reuses_connections(tmp_path):
    executable = Path("build/http_load").resolve()
    assert executable.exists(), "native HTTP load generator is missing"
    cfg = config()
    engine = await api.Engine.open(cfg)
    payload = random.Random(92).randbytes(1024)
    input_file, expected = tmp_path / "input.bin", tmp_path / "expected.bin"
    input_file.write_bytes(payload)
    expected.write_bytes(zlib.compress(payload, 6))
    async def run(policy_version=None, result_tag=None):
        cpus = ",".join(map(str, sorted(os.sched_getaffinity(0))[:2]))
        proc = await asyncio.create_subprocess_exec(str(executable), "--smp", "2", "--cpuset", cpus,
            "--reactor-backend", "epoll", "--overprovisioned", "--default-log-level", "error",
            "--enable-glibc-exception-scaling-workaround", "0", "--port", str(cfg.server.http_port),
            "--target-pid", str(os.getpid()), "--path", "/hybrid/compress", "--method", "POST",
            "--input", str(input_file), "--expected", str(expected), "--concurrency", "8",
            "--warmup", ".05", "--seconds", ".1",
            *([] if policy_version is None else ["--policy-version", str(policy_version)]), stdin=asyncio.subprocess.PIPE,
            *([] if result_tag is None else ["--result-tag", result_tag]),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await asyncio.wait_for(proc.communicate(b"start\n"), 10)
        records = [json.loads(line) for line in stdout.decode().splitlines() if line.startswith("{")]
        assert records, (stdout, stderr)
        return proc.returncode, records[-1]
    try:
        code, result = await run()
        assert code == 0 and result["requests"] > 0 and result["errors"] == 0
        assert result["new_connections"] == 0, result
        assert result["total_connections"] == 8
        assert len(result["server_shards"]) == 2
        code, result = await run(0)
        assert code == 0 and result["requests"] > 0 and result["errors"] == 0
        code, result = await run(1)
        assert code == 1 and result["integrity_errors"] > 0 and result["requests"] == 0
        code, result = await run(result_tag="missing-business-result")
        assert code == 1 and result["integrity_errors"] > 0 and result["requests"] == 0
        expected.write_bytes(b"wrong bytes")
        code, result = await run()
        assert code == 1 and result["errors"] > 0 and result["requests"] == 0
    finally:
        await engine.aclose()
