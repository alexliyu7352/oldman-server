"""Native operation contracts. Run lifecycle cases in fresh processes."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import random
import threading
import zlib

import pytest

from oldman_server.native import _native as native
from oldman_server.native.engine import BusyError, Config, Engine


def layout():
    available = sorted(os.sched_getaffinity(0))
    assert len(available) >= 4
    return tuple(available[-4:-2]), tuple(available[-2:])


async def wait_stat(engine, key, predicate):
    async with asyncio.timeout(10):
        while not predicate(engine.stats()[key]):
            await asyncio.sleep(.001)


def test_shared_compression_handles_binary_empty_and_chunk_edges():
    # Catches truncation, bad finish/flush handling, or changing stream parameters.
    assert hasattr(native, "native_compress"), "shared native compressor is missing"
    rng = random.Random(17)
    for size in (0, 1, 32767, 32768, 32769, 65536, 1048576):
        payload = rng.randbytes(size)
        output, timing = native.native_compress(payload, 6)
        assert zlib.decompress(output) == payload
        assert output == zlib.compress(payload, 6)
        assert timing["work_end_ns"] >= timing["work_start_ns"]
        assert timing["algorithm_ns"] > 0
        assert timing["work_tid"] == threading.get_native_id()
    for level in (-2, 10):
        with pytest.raises(ValueError):
            native.native_compress(b"x", level)


def test_native_binding_releases_gil_for_standard_executor():
    # If the binding keeps the GIL, two real sleep intervals cannot overlap.
    assert hasattr(native, "native_wait"), "GIL-releasing native wait is missing"
    barrier = threading.Barrier(2)
    def run():
        barrier.wait()
        return native.native_wait(100)[1]
    with ThreadPoolExecutor(2) as pool:
        a, b = [job.result() for job in [pool.submit(run), pool.submit(run)]]
    overlap = min(a["work_end_ns"], b["work_end_ns"]) - max(a["work_start_ns"], b["work_start_ns"])
    assert overlap > 20_000_000


@pytest.mark.asyncio
async def test_pool_compression_returns_via_origin_shard():
    # Wrong-shard promise delivery and accidental reactor execution are both bugs.
    shards, workers = layout()
    engine = await Engine.open(Config(shard_cpus=shards, worker_cpus=workers))
    try:
        payload = random.Random(21).randbytes(131072)
        expected, _ = native.native_compress(payload, 6)
        out = await asyncio.gather(*(engine.compress(payload) for _ in range(8)))
        shard_tids = {s["shard"]: s["tid"] for s in engine.shards()}
        worker_tids = {w["tid"] for w in engine.workers()}
        assert {tuple(w["affinity"]) for w in engine.workers()} == {(c,) for c in workers}
        assert {r.shard for r in out} == {0, 1}
        assert {r.metrics["work_tid"] for r in out} == worker_tids
        for result in out:
            assert result.payload == expected
            assert zlib.decompress(result.payload) == payload
            assert result.server_pid == os.getpid()
            assert result.worker_tid == shard_tids[result.shard]
            assert result.metrics["shard_tid"] == shard_tids[result.shard]
            assert result.metrics["work_tid"] not in shard_tids.values()
            assert result.metrics["return_to_shard_ns"] >= 0
        assert engine.stats()["pending_bytes"] == 0
    finally:
        await engine.aclose()


@pytest.mark.asyncio
async def test_pool_cancel_preserves_capacity_and_close_joins_workers():
    # Logical cancel must not permit unbounded queued native work or early teardown.
    shards, workers = layout()
    engine = await Engine.open(Config(shard_cpus=shards[:1], worker_cpus=workers[:1], max_pending=3))
    tids = {w["tid"] for w in engine.workers()}
    jobs = [asyncio.create_task(engine.blocking_wait(100)) for _ in range(3)]
    await wait_stat(engine, "pending_requests", lambda n: n == 3)
    for job in jobs:
        job.cancel()
    await asyncio.gather(*jobs, return_exceptions=True)
    with pytest.raises(BusyError):
        await engine.blocking_wait(20)
    assert engine.stats()["pool_queued"] + engine.stats()["pool_active"] <= 3
    await asyncio.wait_for(engine.aclose(), 10)
    assert engine.stats()["pending_requests"] == 0
    assert engine.stats()["pool_live"] == 0
    assert engine.stats()["pool_queued"] == 0
    assert engine.stats()["pool_active"] == 0
    assert all(not Path(f"/proc/self/task/{tid}").exists() for tid in tids)


@pytest.mark.asyncio
async def test_compression_reserves_output_until_completion_drained():
    # An incompressible result is larger than input; charging its result length
    # instead of the reservation would underflow or free capacity too early.
    shards, workers = layout()
    engine = await Engine.open(Config(shard_cpus=shards[:1], worker_cpus=workers[:1], max_pending_bytes=2100))
    loop = asyncio.get_running_loop()
    task = None
    try:
        loop.remove_reader(engine.notification_fd)
        payload = random.Random(3).randbytes(1024)
        task = asyncio.create_task(engine.compress(payload))
        await wait_stat(engine, "queued_completions", lambda n: n == 1)
        reserved = engine.stats()["pending_bytes"]
        assert 2048 < reserved <= 2100
        with pytest.raises(BusyError):
            await engine.compress(payload)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert engine.stats()["pending_bytes"] == reserved
    finally:
        loop.add_reader(engine.notification_fd, engine._drain)
        await engine.aclose()
    assert engine.stats()["pending_bytes"] == 0


@pytest.mark.asyncio
async def test_cooperative_compression_allows_same_shard_echo():
    # Removing the yield would run all 8 MiB before a queued echo can finish.
    shards, _ = layout()
    engine = await Engine.open(Config(shard_cpus=shards[:1]))
    try:
        payload = random.Random(9).randbytes(8 * 1024 * 1024)
        long = asyncio.create_task(engine.compress(payload, mode="cooperative"))
        await wait_stat(engine, "pending_requests", lambda n: n == 1)
        echo = await engine.echo(b"reactor-progress")
        assert echo.payload == b"reactor-progress"
        assert not long.done()
        result = await long
        assert zlib.decompress(result.payload) == payload
        assert result.payload == native.native_compress(payload, 6)[0]
        assert result.metrics["cooperative_yields"] > 0
        assert result.metrics["work_tid"] == engine.shards()[0]["tid"]
        assert result.metrics["shard_tid"] == result.metrics["work_tid"]
    finally:
        await engine.aclose()


@pytest.mark.asyncio
async def test_blocking_worker_allows_python_and_reactor_progress():
    # A mistaken native sleep on the reactor or loop stalls the unrelated echoes.
    shards, workers = layout()
    engine = await Engine.open(Config(shard_cpus=shards[:1], worker_cpus=workers[:1]))
    try:
        blocked = asyncio.create_task(engine.blocking_wait(200))
        await wait_stat(engine, "pool_active", lambda n: n == 1)
        for _ in range(5):
            await asyncio.sleep(.001)
            assert (await engine.echo(b"alive")).payload == b"alive"
        assert not blocked.done()
        result = await blocked
        assert result.metrics["work_end_ns"] - result.metrics["work_start_ns"] >= 190_000_000
        assert result.metrics["work_tid"] == engine.workers()[0]["tid"]
    finally:
        await engine.aclose()


@pytest.mark.asyncio
async def test_invalid_work_rejected_without_capacity_loss():
    shards, workers = layout()
    with pytest.raises(ValueError):
        await Engine.open(Config(shard_cpus=shards, worker_cpus=shards))
    with pytest.raises(ValueError):
        await Engine.open(Config(shard_cpus=shards, worker_cpus=(workers[0], workers[0])))
    engine = await Engine.open(Config(shard_cpus=shards[:1]))
    try:
        for kwargs in ({"mode": "oops"}, {"level": 10}, {"mode": "pool"}):
            with pytest.raises(ValueError):
                await engine.compress(b"hello", **kwargs)
        with pytest.raises(ValueError):
            await engine.blocking_wait(-1)
        assert engine.stats()["pending_requests"] == 0
        assert engine.stats()["pending_bytes"] == 0
        assert (await engine.compress(b"fine", mode="cooperative")).payload == zlib.compress(b"fine", 6)
    finally:
        await engine.aclose()
