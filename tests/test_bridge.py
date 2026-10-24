"""The Engine's own lifecycle: startup, cancellation, shutdown, signals and
resources. Each case runs in a fresh process (one native runtime per process)."""
import asyncio
import ctypes
import os
import signal
import threading

import pytest

from oldman_server.native.engine import Config, Engine, ServerConfig
from test_http import free_port, request


def cpus():
    return tuple(sorted(os.sched_getaffinity(0))[-2:])


def open_engine():
    """An Engine serving a trivial framework handler on a free port."""
    server = ServerConfig(http_port=free_port(), application_dispatch_all=True)
    return Engine.open(Config(shard_cpus=cpus(), server=server),
                       application_handler=lambda request: (200, b"", ()))


async def wait_stat(engine, key, predicate, seconds=5):
    async with asyncio.timeout(seconds):
        while not predicate(engine.stats()[key]):
            await asyncio.sleep(0.001)


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
    engine = await open_engine()
    assert c_signal_handler(signal.SIGINT) == before[signal.SIGINT]
    assert c_signal_handler(signal.SIGTERM) == before[signal.SIGTERM]
    await engine.aclose()
    assert {n: c_signal_handler(n) for n in watched} == before


@pytest.mark.asyncio
async def test_canceled_close_still_finishes_cleanup():
    engine = await open_engine()
    close = asyncio.create_task(engine.aclose())
    await asyncio.sleep(0)
    close.cancel()
    with pytest.raises(asyncio.CancelledError):
        await close
    await asyncio.wait_for(engine.aclose(), 5)
    assert engine.stats()["live_workers"] == 0


@pytest.mark.asyncio
async def test_shutdown_releases_native_threads_and_fds():
    fds_before = len(os.listdir("/proc/self/fd"))
    threads_before = {int(t) for t in os.listdir("/proc/self/task")}
    engine = await open_engine()
    tids = {s["tid"] for s in engine.shards()}
    assert len(tids) == 2
    assert [s["affinity"] for s in engine.shards()] == [[c] for c in cpus()]
    assert all(s["backend"] == "epoll" for s in engine.shards())
    assert (await request(engine.config.server.http_port, "/"))[0] == 200
    await engine.aclose()
    assert not tids & {int(t) for t in os.listdir("/proc/self/task")}
    leftover = {int(t) for t in os.listdir("/proc/self/task")} - threads_before
    assert leftover <= {t.native_id for t in threading.enumerate()}
    assert len(os.listdir("/proc/self/fd")) == fds_before


@pytest.mark.asyncio
async def test_cancel_during_open_cleans_up():
    before = len(os.listdir("/proc/self/fd"))
    opening = asyncio.create_task(open_engine())
    await asyncio.sleep(0)
    opening.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(opening, 10)
    assert len(os.listdir("/proc/self/fd")) == before


@pytest.mark.asyncio
async def test_shutdown_preserves_handler_installed_by_application():
    original = signal.getsignal(signal.SIGABRT)
    engine = await open_engine()
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
    opening = asyncio.create_task(open_engine())
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
            await open_engine()
        assert len(os.listdir("/proc/self/fd")) == before
        assert captured[0]._native.fd() == -1
    finally:
        monkeypatch.setattr(loop, "add_reader", original_add)
        if captured and captured[0]._native.fd() >= 0:
            engine = captured[0]
            original_add(engine.notification_fd, engine._drain)
            await wait_stat(engine, "state", lambda n: n != 0)
            await engine.aclose()
