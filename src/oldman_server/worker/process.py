from __future__ import annotations

import os

from collections.abc import MutableMapping
from contextlib import suppress
from datetime import datetime, timezone
from functools import partial
from inspect import signature
from multiprocessing.context import BaseContext
from signal import SIGINT
from threading import Thread
from time import monotonic, sleep
from typing import Any

from oldman_server.compat import OS_IS_WINDOWS
from oldman_server.log import Colors, logger
from oldman_server.worker.constants import ProcessState, RestartOrder


SIGKILL: int
if not OS_IS_WINDOWS:
    from signal import SIGKILL
else:
    SIGKILL = SIGINT  # os.kill terminates the process there


def get_now():
    now = datetime.now(tz=timezone.utc)
    return now


class WorkerProcess:
    """A worker process."""

    THRESHOLD = 300  # == 30 seconds
    SERVER_LABEL = "Server"
    SERVER_IDENTIFIER = "Srv"

    def __init__(
        self,
        factory,
        name,
        ident,
        target,
        kwargs,
        worker_state,
        restartable: bool = False,
        slot: int | None = None,
    ):
        self.state = ProcessState.IDLE
        # A server worker's number among the current server workers
        # (OLDMAN_SERVER_WORKER_SLOT; oldman_server.native.cpus.worker_number).
        self.slot = slot
        self.factory = factory
        self.name = name
        self.ident = ident
        self.target = target
        self.kwargs = kwargs
        self.worker_state = worker_state
        self.restartable = restartable
        if self.name not in self.worker_state:
            self.worker_state[self.name] = {
                "server": self.SERVER_LABEL in self.name
            }
        # The processes zero-downtime restarts replaced, each kept until its
        # replacement acknowledges; terminate() stops those still there.
        self._previous: set[Any] = set()
        # Those terminate() stopped: exit() waits for them too.
        self._stopped_previous: list[Any] = []
        self.spawn()

    def set_state(self, state: ProcessState, force=False):
        if not force and state < self.state:
            raise Exception("...")
        self.state = state
        self.worker_state[self.name] = {
            **self.worker_state[self.name],
            "state": self.state.name,
        }

    def start(self):
        os.environ["OLDMAN_SERVER_WORKER_NAME"] = self.name
        os.environ["OLDMAN_SERVER_WORKER_IDENTIFIER"] = self.ident
        if self.slot is not None:
            os.environ["OLDMAN_SERVER_WORKER_SLOT"] = str(self.slot)
        logger.debug(
            f"{Colors.BLUE}Starting a process: {Colors.BOLD}"
            f"{Colors.PINK}%s{Colors.END}",
            self.name,
        )
        self.set_state(ProcessState.STARTING)
        self._current_process.start()
        self.set_state(ProcessState.STARTED)
        if not self.worker_state[self.name].get("starts"):
            self.worker_state[self.name] = {
                **self.worker_state[self.name],
                "pid": self.pid,
                "start_at": get_now(),
                "starts": 1,
            }
        del os.environ["OLDMAN_SERVER_WORKER_NAME"]
        del os.environ["OLDMAN_SERVER_WORKER_IDENTIFIER"]
        os.environ.pop("OLDMAN_SERVER_WORKER_SLOT", None)

    def join(self):
        self.set_state(ProcessState.JOINED)
        self._current_process.join()

    def stopped(self) -> bool:
        """Whether the process, and the previous ones terminate() stopped,
        have ended."""
        return not self.is_alive() and not any(
            previous.is_alive() for previous in self._stopped_previous
        )

    def kill(self):
        """SIGKILL the process and the previous ones terminate() stopped."""
        for process in (self._current_process, *self._stopped_previous):
            with suppress(AttributeError, ProcessLookupError, TypeError):
                if process.is_alive():
                    os.kill(process.pid, SIGKILL)

    def exit(self, timeout: float = 10.0):
        """Wait up to `timeout` seconds for the process (and the previous ones
        terminate() stopped) to end, then drop its shared state."""
        deadline = monotonic() + timeout
        while not self.stopped() and monotonic() < deadline:
            sleep(0.1)

        if not self.is_alive():
            try:
                del self.worker_state[self.name]
            except (
                BrokenPipeError,
                ConnectionRefusedError,
                ConnectionResetError,
                EOFError,
            ):
                logger.debug("Monitor process has already exited.")
            except KeyError:
                logger.debug("Could not find worker state to delete.")

    def terminate(self):
        """Stop the process (SIGINT: it stops gracefully), and any previous
        process a zero-downtime restart still keeps."""
        while self._previous:
            try:
                previous = self._previous.pop()
            except KeyError:  # the restart's own thread took the last one
                break
            with suppress(AttributeError, ProcessLookupError):
                previous.terminate()
            self._stopped_previous.append(previous)
        if self.state is not ProcessState.TERMINATED:
            logger.debug(
                f"{Colors.BLUE}Terminating a process: "
                f"{Colors.BOLD}{Colors.PINK}"
                f"%s {Colors.BLUE}[%s]{Colors.END}",
                self.name,
                self.pid,
            )
            try:
                self.set_state(ProcessState.TERMINATED, force=True)
            except (BrokenPipeError, ConnectionResetError, EOFError):
                pass
            try:
                os.kill(self.pid, SIGINT)
            except (KeyError, AttributeError, ProcessLookupError):
                ...

    def restart(self, restart_order=RestartOrder.SHUTDOWN_FIRST, **kwargs):
        logger.debug(
            f"{Colors.BLUE}Restarting a process: {Colors.BOLD}{Colors.PINK}"
            f"%s {Colors.BLUE}[%s]{Colors.END}",
            self.name,
            self.pid,
        )
        self.set_state(ProcessState.RESTARTING, force=True)
        if restart_order is RestartOrder.SHUTDOWN_FIRST:
            self._terminate_now()
        else:
            previous = self._current_process
            self._previous.add(previous)
        if self._add_config():
            self.kwargs.update(
                {"config": {k.upper(): v for k, v in kwargs.items()}}
            )
        try:
            self.spawn()
            self.start()
        except AttributeError:
            raise RuntimeError("Restart failed")

        if restart_order is RestartOrder.STARTUP_FIRST:
            self._terminate_soon(previous)

        self.worker_state[self.name] = {
            **self.worker_state[self.name],
            "pid": self.pid,
            "starts": self.worker_state[self.name]["starts"] + 1,
            "restart_at": get_now(),
        }

    def is_alive(self):
        try:
            return self._current_process.is_alive()
        except AssertionError:
            return False

    def spawn(self):
        if self.state not in (ProcessState.IDLE, ProcessState.RESTARTING):
            raise Exception("Cannot spawn a worker process until it is idle.")
        self._current_process = self.factory(
            name=self.name,
            target=self.target,
            kwargs=self.kwargs,
            daemon=True,
        )

    @property
    def pid(self):
        return self._current_process.pid

    @property
    def exitcode(self):
        return self._current_process.exitcode

    def _terminate_now(self):
        if not self._current_process.is_alive():
            return
        logger.debug(
            f"{Colors.BLUE}Begin restart termination: "
            f"{Colors.BOLD}{Colors.PINK}"
            f"%s {Colors.BLUE}[%s]{Colors.END}",
            self.name,
            self._current_process.pid,
        )
        self._current_process.terminate()

    def _terminate_soon(self, previous):
        logger.debug(
            f"{Colors.BLUE}Begin restart termination: "
            f"{Colors.BOLD}{Colors.PINK}"
            f"%s {Colors.BLUE}[%s]{Colors.END}",
            self.name,
            self._current_process.pid,
        )
        termination_thread = Thread(
            target=partial(self._wait_to_terminate, previous)
        )
        termination_thread.start()

    def _wait_to_terminate(self, previous):
        """Stop `previous` once its replacement acknowledges, unless
        terminate() stopped it first (the manager is shutting down)."""
        logger.debug(
            f"{Colors.BLUE}Waiting for process to be acked: "
            f"{Colors.BOLD}{Colors.PINK}"
            f"%s {Colors.BLUE}[%s]{Colors.END}",
            self.name,
            previous.pid,
        )
        misses = 0
        while self.state is not ProcessState.ACKED:
            if previous not in self._previous:
                return
            sleep(0.1)
            misses += 1
            if misses > self.THRESHOLD:
                raise TimeoutError(
                    f"Worker {self.name} failed to come ack within "
                    f"{self.THRESHOLD / 10} seconds"
                )
        try:
            self._previous.remove(previous)
        except KeyError:  # terminate() took it meanwhile
            return
        logger.debug(
            f"{Colors.BLUE}Process acked. Terminating: "
            f"{Colors.BOLD}{Colors.PINK}"
            f"%s {Colors.BLUE}[%s]{Colors.END}",
            self.name,
            previous.pid,
        )
        previous.terminate()

    def _add_config(self) -> bool:
        sig = signature(self.target)
        if "config" in sig.parameters or any(
            param.kind == param.VAR_KEYWORD
            for param in sig.parameters.values()
        ):
            return True
        return False


class Worker:
    # Not containing SERVER_LABEL: worker kinds are told apart by substring.
    WORKER_PREFIX = "Oldman"

    def __init__(
        self,
        ident: str,
        name: str,
        serve,
        server_settings,
        context: BaseContext,
        worker_state: MutableMapping[str, Any],
        num: int = 1,
        restartable: bool = False,
        tracked: bool = True,
        auto_start: bool = True,
        slot: int | None = None,
    ):
        self.ident = ident
        self.slot = slot
        self.name = name
        self.num = num
        self.context = context
        self.serve = serve
        self.server_settings = server_settings
        self.worker_state = worker_state
        self.processes: set[WorkerProcess] = set()
        self.restartable = restartable
        self.tracked = tracked
        self.auto_start = auto_start
        for _ in range(num):
            self.create_process()

    def create_process(self) -> WorkerProcess:
        process = WorkerProcess(
            # Need to ignore this typing error - The problem is the
            # BaseContext itself has no Process. But, all of its
            # implementations do. We can safely ignore as it is a typing
            # issue in the standard lib.
            factory=self.context.Process,  # type: ignore
            name="-".join(
                [self.WORKER_PREFIX, self.name, str(len(self.processes))]
            ),
            ident=self.ident,
            target=self.serve,
            kwargs={**self.server_settings},
            worker_state=self.worker_state,
            restartable=self.restartable,
            slot=self.slot,
        )
        self.processes.add(process)
        return process

    def has_alive_processes(self) -> bool:
        return any(process.is_alive() for process in self.processes)
