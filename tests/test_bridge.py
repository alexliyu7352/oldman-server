"""Real integration tests; each case runs in a fresh process (see run_tests.py)."""
import asyncio
import ctypes
import gc
import os
import signal
import threading

import pytest

from oldman_server.native.engine import BusyError, Config, Engine


def cpus():
    return tuple(sorted(os.sched_getaffinity(0))[-2:])


async def wait_stat(engine, key, predicate, seconds=5):
    async with asyncio.timeout(seconds):
        while not predicate(engine.stats()[key]):
            await asyncio.sleep(0.001)


@pytest.mark.asyncio
async def test_roundtrip_same_process():
    old_signal = signal.getsignal(signal.SIGINT)
    engine = await Engine.open(Config(shard_cpus=cpus()))
    try:
        jobs = [engine.echo(i.to_bytes(8, "little")) for i in range(1000)]
        out = await asyncio.wait_for(asyncio.gather(*jobs), 30)
        assert [x.payload for x in out] == [i.to_bytes(8, "little") for i in range(1000)]
        assert all(x.server_pid == os.getpid() for x in out)
        assert all(x.worker_tid != threading.get_native_id() for x in out)
        assert {x.shard for x in out} == {0, 1}
        assert engine.stats()["pending_requests"] == 0
        assert engine.stats()["pending_bytes"] == 0
        assert signal.getsignal(signal.SIGINT) == old_signal
    finally:
        await asyncio.wait_for(engine.aclose(), 30)
    assert engine.stats()["live_workers"] == 0


@pytest.mark.asyncio
async def test_input_lifetime():
    engine = await Engine.open(Config(shard_cpus=cpus()))
    try:
        payload = bytes(range(256)) * 4096
        task = asyncio.create_task(engine.echo(payload, delay_ms=50))
        del payload
        await wait_stat(engine, "pending_requests", lambda n: n == 1)
        gc.collect()
        churn = [bytes([i]) * 1048576 for i in range(8)]
        result = await asyncio.wait_for(task, 30)
        assert result.payload == bytes(range(256)) * 4096
        assert len(churn) == 8
    finally:
        await engine.aclose()


@pytest.mark.asyncio
async def test_saturation_recovers():
    engine = await Engine.open(Config(shard_cpus=cpus(), max_pending=8))
    try:
        tasks = [asyncio.create_task(engine.echo(b"x", delay_ms=150)) for _ in range(8)]
        await wait_stat(engine, "pending_requests", lambda n: n == 8)
        with pytest.raises(BusyError):
            await engine.echo(b"rejected")
        assert len(await asyncio.gather(*tasks)) == 8
        assert (await engine.echo(b"accepted")).payload == b"accepted"
    finally:
        await engine.aclose()


@pytest.mark.asyncio
async def test_byte_budget_recovers():
    engine = await Engine.open(Config(shard_cpus=cpus(), max_pending_bytes=16))
    try:
        task = asyncio.create_task(engine.echo(b"1234567890abcdef", delay_ms=100))
        await wait_stat(engine, "pending_bytes", lambda n: n == 16)
        with pytest.raises(BusyError):
            await engine.echo(b"x")
        await task
        assert (await engine.echo(b"x")).payload == b"x"
    finally:
        await engine.aclose()


@pytest.mark.asyncio
async def test_canceled_timer_retains_capacity_until_native_done():
    engine = await Engine.open(Config(shard_cpus=cpus(), max_pending=1))
    try:
        task = asyncio.create_task(engine.echo(b"still-owned", delay_ms=120))
        await wait_stat(engine, "pending_requests", lambda n: n == 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert engine.stats()["pending_bytes"] == 11
        with pytest.raises(BusyError):
            await engine.echo(b"too-early")
        await wait_stat(engine, "pending_requests", lambda n: n == 0)
        assert engine.stats()["pending_bytes"] == 0
        assert (await engine.echo(b"again")).payload == b"again"
    finally:
        await engine.aclose()


@pytest.mark.asyncio
async def test_cancel_after_completion_queued():
    engine = await Engine.open(Config(shard_cpus=cpus()))
    try:
        loop = asyncio.get_running_loop()
        loop.remove_reader(engine.notification_fd)
        task = asyncio.create_task(engine.echo(b"race", delay_ms=20))
        await wait_stat(engine, "queued_completions", lambda n: n == 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        loop.add_reader(engine.notification_fd, engine._drain)
        await wait_stat(engine, "pending_requests", lambda n: n == 0)
        assert engine.stats()["pending_bytes"] == 0
    finally:
        asyncio.get_running_loop().add_reader(engine.notification_fd, engine._drain)
        await engine.aclose()


@pytest.mark.asyncio
async def test_close_drains_inflight_and_rejects_new_work():
    engine = await Engine.open(Config(shard_cpus=cpus()))
    tasks = [asyncio.create_task(engine.echo(b"close", delay_ms=70)) for _ in range(64)]
    await wait_stat(engine, "pending_requests", lambda n: n == 64)
    await asyncio.wait_for(engine.aclose(), 10)
    out = await asyncio.gather(*tasks)
    assert len(out) == 64
    assert engine.stats()["pending_requests"] == 0
    assert engine.stats()["pending_bytes"] == 0
    assert engine.stats()["live_workers"] == 0
    with pytest.raises(RuntimeError):
        await engine.echo(b"closed")
    await engine.aclose()


@pytest.mark.asyncio
async def test_timer_does_not_block_python_or_other_native_tasks():
    engine = await Engine.open(Config(shard_cpus=cpus()))
    try:
        delayed = asyncio.create_task(engine.echo(b"slow", delay_ms=200))
        await asyncio.sleep(0)
        for _ in range(20):
            assert (await engine.echo(b"fast")).payload == b"fast"
        assert not delayed.done()
        await delayed
    finally:
        await engine.aclose()


def c_signal_handler(signum):
    # Linux sigaction has the handler union as its first machine word.
    libc = ctypes.CDLL(None, use_errno=True)
    action = ctypes.create_string_buffer(256)
    assert libc.sigaction(signum, None, action) == 0
    return ctypes.c_void_p.from_buffer(action).value


@pytest.mark.asyncio
async def test_signal_handlers_restored_after_shutdown():
    watched = (signal.SIGINT, signal.SIGTERM, signal.SIGABRT, signal.SIGSEGV, signal.SIGILL, signal.SIGFPE)
    before = {n: c_signal_handler(n) for n in watched}
    engine = await Engine.open(Config(shard_cpus=cpus()))
    assert c_signal_handler(signal.SIGINT) == before[signal.SIGINT]
    assert c_signal_handler(signal.SIGTERM) == before[signal.SIGTERM]
    await engine.aclose()
    assert {n: c_signal_handler(n) for n in watched} == before


@pytest.mark.asyncio
async def test_canceled_close_still_finishes_cleanup():
    engine = await Engine.open(Config(shard_cpus=cpus()))
    task = asyncio.create_task(engine.echo(b"owned", delay_ms=100))
    await wait_stat(engine, "pending_requests", lambda n: n == 1)
    close = asyncio.create_task(engine.aclose())
    await asyncio.sleep(0)
    close.cancel()
    with pytest.raises(asyncio.CancelledError):
        await close
    assert (await task).payload == b"owned"
    await asyncio.wait_for(engine.aclose(), 5)
    assert engine.stats()["live_workers"] == 0


@pytest.mark.asyncio
async def test_shutdown_releases_native_threads_and_fds():
    fds_before = len(os.listdir("/proc/self/fd"))
    threads_before = {int(t) for t in os.listdir("/proc/self/task")}
    engine = await Engine.open(Config(shard_cpus=cpus()))
    tids = {s["tid"] for s in engine.shards()}
    assert len(tids) == 2
    assert [s["affinity"] for s in engine.shards()] == [[c] for c in cpus()]
    assert all(s["backend"] == "epoll" for s in engine.shards())
    await engine.echo(b"fd-check")
    await engine.aclose()
    assert not tids & {int(t) for t in os.listdir("/proc/self/task")}
    leftover = {int(t) for t in os.listdir("/proc/self/task")} - threads_before
    assert leftover <= {t.native_id for t in threading.enumerate()}
    assert len(os.listdir("/proc/self/fd")) == fds_before


@pytest.mark.asyncio
async def test_copy_baseline_preserves_binary_payload():
    from oldman_server.native import _native as _seastar_probe
    assert _seastar_probe.copy_roundtrip(b"\x00\xffprobe\x80") == b"\x00\xffprobe\x80"


@pytest.mark.asyncio
async def test_completed_queue_stays_bounded_and_batch_notifications_rearm():
    engine = await Engine.open(Config(shard_cpus=cpus(), max_pending=8, completion_batch=3))
    loop = asyncio.get_running_loop()
    try:
        loop.remove_reader(engine.notification_fd)
        tasks = [asyncio.create_task(engine.echo(b"queued")) for _ in range(8)]
        await wait_stat(engine, "queued_completions", lambda n: n == 8)
        with pytest.raises(BusyError):
            await engine.echo(b"overflow")
        assert engine.stats()["pending_requests"] == 8
        loop.add_reader(engine.notification_fd, engine._drain)
        assert len(await asyncio.wait_for(asyncio.gather(*tasks), 5)) == 8
        assert engine.stats()["pending_bytes"] == 0
    finally:
        loop.add_reader(engine.notification_fd, engine._drain)
        await engine.aclose()


@pytest.mark.asyncio
async def test_cancel_during_open_cleans_up():
    before = len(os.listdir("/proc/self/fd"))
    opening = asyncio.create_task(Engine.open(Config(shard_cpus=cpus())))
    await asyncio.sleep(0)
    opening.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(opening, 10)
    assert len(os.listdir("/proc/self/fd")) == before


@pytest.mark.asyncio
async def test_cancel_after_future_set_before_task_resumes():
    engine = await Engine.open(Config(shard_cpus=cpus()))
    loop = asyncio.get_running_loop()
    try:
        loop.remove_reader(engine.notification_fd)
        task = asyncio.create_task(engine.echo(b"completed"))
        await wait_stat(engine, "queued_completions", lambda n: n == 1)
        engine._drain()
        assert engine.stats()["pending_requests"] == 0
        assert not task.done()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert engine.stats()["pending_bytes"] == 0
    finally:
        loop.add_reader(engine.notification_fd, engine._drain)
        await engine.aclose()


@pytest.mark.asyncio
async def test_shutdown_preserves_handler_installed_by_application():
    original = signal.getsignal(signal.SIGABRT)
    engine = await Engine.open(Config(shard_cpus=cpus()))
    try:
        handler = lambda *_: None
        signal.signal(signal.SIGABRT, handler)
        installed = c_signal_handler(signal.SIGABRT)
        await engine.aclose()
        assert c_signal_handler(signal.SIGABRT) == installed
        assert signal.getsignal(signal.SIGABRT) is handler
    finally:
        await engine.aclose()
        signal.signal(signal.SIGABRT, original)


async def held_open(monkeypatch):
    """Hold only eventfd delivery; initialization and native threads remain real."""
    loop = asyncio.get_running_loop()
    original_add = loop.add_reader
    captured = loop.create_future()
    def add_reader(fd, callback, *args):
        if getattr(callback, "__name__", "") == "_drain" and not captured.done():
            captured.set_result((fd, callback))
        else:
            original_add(fd, callback, *args)
    monkeypatch.setattr(loop, "add_reader", add_reader)
    opening = asyncio.create_task(Engine.open(Config(shard_cpus=cpus())))
    fd, callback = await captured
    return opening, callback.__self__, fd, callback, original_add


async def release_held_open(opening, engine, fd, callback, original_add):
    # Ensure a failed regression itself cannot strand native threads.
    if engine._native.fd() >= 0:
        original_add(fd, callback)
    try:
        await opening
    except BaseException:
        pass
    if engine._native.fd() >= 0:
        await wait_stat(engine, "state", lambda n: n != 0)
        await engine.aclose()
    if engine._ready.done() and not engine._ready.cancelled():
        engine._ready.exception()


@pytest.mark.asyncio
async def test_repeated_startup_cancellation_keeps_cleanup_owned(monkeypatch):
    opening, engine, fd, callback, original_add = await held_open(monkeypatch)
    try:
        opening.cancel()
        await asyncio.sleep(0)
        opening.cancel()
        await asyncio.sleep(0)
        assert not opening.done(), "canceled open returned while native cleanup was still unowned"
        original_add(fd, callback)
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(opening, 10)
        assert engine._native.fd() == -1
        assert engine.stats()["live_workers"] == 0
    finally:
        await release_held_open(opening, engine, fd, callback, original_add)


@pytest.mark.asyncio
async def test_canceled_startup_failure_also_releases_resources(monkeypatch):
    monkeypatch.setenv("SEASTAR_OPTIONS", "--probe-intentionally-invalid-option")
    opening, engine, fd, callback, original_add = await held_open(monkeypatch)
    try:
        opening.cancel()
        await asyncio.sleep(0)
        original_add(fd, callback)
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(opening, 10)
        assert engine._native.fd() == -1
        assert engine.stats()["live_workers"] == 0
    finally:
        await release_held_open(opening, engine, fd, callback, original_add)


@pytest.mark.asyncio
async def test_reader_registration_failure_releases_runtime(monkeypatch):
    loop = asyncio.get_running_loop()
    before = len(os.listdir("/proc/self/fd"))
    original_add = loop.add_reader
    captured = []
    def fail_registration(fd, callback, *args):
        if getattr(callback, "__name__", "") == "_drain":
            captured.append(callback.__self__)
            raise RuntimeError("injected registration failure")
        original_add(fd, callback, *args)
    monkeypatch.setattr(loop, "add_reader", fail_registration)
    try:
        with pytest.raises(RuntimeError, match="injected registration failure"):
            await Engine.open(Config(shard_cpus=cpus()))
        assert len(os.listdir("/proc/self/fd")) == before
        assert captured[0]._native.fd() == -1
    finally:
        monkeypatch.setattr(loop, "add_reader", original_add)
        if captured and captured[0]._native.fd() >= 0:
            engine = captured[0]
            original_add(engine.notification_fd, engine._drain)
            await wait_stat(engine, "state", lambda n: n != 0)
            await engine.aclose()
