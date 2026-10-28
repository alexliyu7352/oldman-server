"""Worker health: the worker manager restarts a server worker whose event loop
has stopped responding (WORKER_HEALTH_TIMEOUT).

The main process gives the server workers a table in shared memory with one
slot per worker number (oldman_server.native.cpus.worker_number). Each worker
writes time.monotonic() to its slot from a task on its own event loop: a loop
blocked by code that does not yield stops the writes, which is what this
detects. CLOCK_MONOTONIC is the same clock in every process on Linux, and wall
clock changes do not move it. The manager reads the table between monitor
messages (WorkerManager._check_health).

Plain Python, like oldman_server.native.accept: the main process creates the
table without the native extension, and asyncio workers use it too.
"""
from __future__ import annotations

import asyncio
import ctypes
import faulthandler
import os
import signal

from contextlib import suppress
from time import monotonic, sleep
from typing import Any

from oldman_server.native.cpus import worker_number


# Worker numbers below this are watched (numbers stay 0..n-1 through scaling).
SLOTS = 1024
# The signal on which a watched worker writes the stacks of its threads to
# its stderr (faulthandler), sent before the manager kills a silent worker.
# None where there is none (Windows): the worker is killed without it.
DUMP_SIGNAL = getattr(signal, "SIGUSR2", None)
# How long the manager gives that write before the kill.
DUMP_WAIT = 0.2


def timeout(config) -> float | None:
    """WORKER_HEALTH_TIMEOUT in seconds, or None when it is off (unset or 0).
    Anything else than a number of seconds of at least 1 is refused: a worker
    writes every interval() and the timeout must span at least two writes."""
    value = config.WORKER_HEALTH_TIMEOUT
    if value is None or value is False or value == 0:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not value >= 1:
        raise ValueError(
            "WORKER_HEALTH_TIMEOUT is a number of seconds of at least 1, or "
            f"unset (0) to watch no worker: {value!r}"
        )
    return float(value)


def interval(seconds: float) -> float:
    """How often a worker writes its heartbeat for a `seconds` timeout."""
    return max(0.5, seconds / 4)


def table(context) -> Any:
    """A zeroed table in shared memory for the worker processes `context`
    starts (they receive it as they start)."""
    return context.RawArray(ctypes.c_double, SLOTS)


class Heartbeat:
    """The writes of one worker process, from a task on its event loop."""

    def __init__(self, loop, slots, slot: int, seconds: float):
        self._slots = slots
        self._slot = slot
        self._every = interval(seconds)
        self._loop = loop
        self.beat()
        self._task = loop.create_task(self._run(), name="oldman-server worker heartbeat")

    @classmethod
    def start(cls, loop, app) -> Heartbeat | None:
        """The heartbeat of this server worker process, or None when health
        is not watched (off, outside a worker process, or a worker number past
        the table)."""
        slots = app.state.worker_health
        seconds = app.state.worker_health_timeout
        number = worker_number()
        if slots is None or not seconds or number is None or number >= SLOTS:
            return None
        if DUMP_SIGNAL is not None:
            # Written by the C handler even while the event loop is blocked:
            # what it was blocked in reaches the logs before the kill.
            faulthandler.register(DUMP_SIGNAL, all_threads=True, chain=False)
        return cls(loop, slots, number, seconds)

    def beat(self) -> None:
        self._slots[self._slot] = monotonic()

    async def _run(self) -> None:
        with suppress(asyncio.CancelledError):
            while True:
                await asyncio.sleep(self._every)
                self.beat()

    def stop(self) -> None:
        """End the task quietly (as the worker's loop closes): cancelled and
        let finish, so that it is neither logged nor destroyed pending."""
        task = self._task
        if task.done():
            return
        task.cancel()
        loop = self._loop
        if not loop.is_closed() and not loop.is_running():
            loop.run_until_complete(asyncio.gather(task, return_exceptions=True))


class Watch:
    """The manager's side: which acknowledged server workers have been silent
    past the timeout. A process counts from when the manager saw it
    acknowledge its start, so a slot's earlier writes (by the process a
    restart replaced) never count against its successor."""

    def __init__(self, slots, seconds: float):
        self.slots = slots
        self.seconds = seconds
        self._acked_at: dict[int, float] = {}

    def silent_for(self, pid: int, slot: int, now: float) -> float:
        acked = self._acked_at.setdefault(pid, now)
        return now - max(self.slots[slot], acked)

    def forget(self, pid: int) -> None:
        self._acked_at.pop(pid, None)

    @staticmethod
    def dump_stack(pid: int) -> bool:
        """Have the worker write its threads' stacks to its stderr, and give
        it DUMP_WAIT to do so; whether the signal went out."""
        if DUMP_SIGNAL is None:
            return False
        try:
            os.kill(pid, DUMP_SIGNAL)
        except ProcessLookupError:
            return False
        sleep(DUMP_WAIT)
        return True
