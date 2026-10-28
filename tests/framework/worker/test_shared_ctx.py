# 18
# 21-29
# 26
# 36-37
# 42
# 55
# 38->

import logging
import sys

from ctypes import c_int32
from multiprocessing import Pipe, Queue, Value
from os import environ
from typing import Any

import pytest

from oldman_server.compat import use_context
from oldman_server.types.shared_ctx import SharedContext


@pytest.mark.parametrize(
    "item,okay",
    (
        (Pipe(), True),
        (Value("i", 0), True),
        (Queue(), True),
        (c_int32(1), True),
        (1, False),
        ("thing", False),
        (object(), False),
    ),
)
def test_set_items(item: Any, okay: bool, caplog):
    ctx = SharedContext()

    with caplog.at_level(logging.INFO):
        ctx.item = item

    assert ctx.is_locked is False
    assert len(caplog.record_tuples) == 0 if okay else 1
    if not okay:
        assert caplog.record_tuples[0][0] == "oldman_server.error"
        assert caplog.record_tuples[0][1] == logging.WARNING
        assert "Unsafe object" in caplog.record_tuples[0][2]


@pytest.mark.parametrize(
    "item",
    (
        Pipe(),
        Value("i", 0),
        Queue(),
        c_int32(1),
        1,
        "thing",
        object(),
    ),
)
def test_set_items_in_worker(item: Any, caplog):
    ctx = SharedContext()

    environ["OLDMAN_SERVER_WORKER_NAME"] = "foo"
    with caplog.at_level(logging.INFO):
        ctx.item = item
    del environ["OLDMAN_SERVER_WORKER_NAME"]

    assert ctx.is_locked is False
    assert len(caplog.record_tuples) == 0


def test_lock():
    ctx = SharedContext()

    assert ctx.is_locked is False

    ctx.lock()

    assert ctx.is_locked is True

    message = "Cannot set item on locked SharedContext object"
    with pytest.raises(RuntimeError, match=message):
        ctx.item = 1


def test_unlock():
    ctx = SharedContext()
    ctx.lock()
    ctx.unlock()

    assert ctx.is_locked is False
    ctx.item = Value("i", 1)


@pytest.mark.skipif(
    sys.platform not in ("linux", "darwin"),
    reason="This test requires fork context",
)
def test_shared_ctx_is_locked_in_workers(app, port):
    """What main_process_start put in shared_ctx is shared by the workers,
    and a worker cannot add to it (the lock never took effect before: it
    was decided before the worker knew it was one)."""

    @app.main_process_start
    async def setup(app):
        app.shared_ctx.count = Value("i", 0)
        app.shared_ctx.results = Queue()

    @app.after_server_start
    async def check(app):
        try:
            app.shared_ctx.late = 1
            raised = False
        except RuntimeError:
            raised = True
        with app.shared_ctx.count.get_lock():
            app.shared_ctx.count.value += 1
            seen = app.shared_ctx.count.value
        app.shared_ctx.results.put((app.shared_ctx.is_locked, raised))
        if seen == 2:
            app.stop()

    with use_context("fork"):
        app.run(port=port, workers=2)

    results = [app.shared_ctx.results.get(timeout=5) for _ in range(2)]
    assert results == [(True, True), (True, True)]
    assert app.shared_ctx.count.value == 2


def test_shared_ctx_is_locked_while_a_single_process_serves(app, port):
    seen = []

    @app.after_server_start
    async def check(app):
        try:
            app.shared_ctx.late = 1
            seen.append("set")
        except RuntimeError:
            seen.append("locked")
        app.stop()

    app.run(port=port, single_process=True)

    assert seen == ["locked"]
    # Open again for the next run's main_process_start.
    assert app.shared_ctx.is_locked is False
    app.shared_ctx.after = Value("i", 1)
