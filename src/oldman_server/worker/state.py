from __future__ import annotations

import os
import sys

from collections.abc import ItemsView, Iterator, KeysView, Mapping, ValuesView
from collections.abc import Mapping as MappingType
from multiprocessing.managers import SyncManager
from signal import SIG_IGN, SIGINT, SIGTERM, signal
from typing import Any


# prctl(2) option: the signal this process gets when its parent dies.
PR_SET_PDEATHSIG = 1


def start_shared_state() -> SyncManager:
    """Start the process that holds the workers' shared state (the worker
    manager's and the workers' state dict)."""
    manager = SyncManager()
    manager.start(_shared_state_process)
    return manager


def _shared_state_process() -> None:
    """In the shared state's process: the main process stops it (shutdown()),
    once the service has stopped; a stop signal meant for the service does
    not. Ctrl+C reaches every process of the group and a systemd stop
    (KillMode=control-group) every process of the unit: ended by it, this
    process took the state with it while the main process and the workers
    were still stopping, and each of their state calls failed. It ends with
    the main process all the same (Linux), not left behind if that is
    killed."""
    signal(SIGINT, SIG_IGN)
    signal(SIGTERM, SIG_IGN)
    if not sys.platform.startswith("linux"):
        return
    import ctypes
    import multiprocessing

    from signal import SIGKILL

    parent = multiprocessing.parent_process()
    libc = ctypes.CDLL(None, use_errno=True)
    if parent is None or libc.prctl(PR_SET_PDEATHSIG, SIGKILL) != 0:
        return
    if os.getppid() != parent.pid:
        # The parent died before the request took effect.
        os._exit(1)


class WorkerState(Mapping):
    RESTRICTED = (
        "health",
        "pid",
        "requests",
        "restart_at",
        "server",
        "start_at",
        "starts",
        "state",
    )

    def __init__(self, state: dict[str, Any], current: str) -> None:
        self._name = current
        self._state = state

    def __getitem__(self, key: str) -> Any:
        return self._state[self._name][key]

    def __setitem__(self, key: str, value: Any) -> None:
        if key in self.RESTRICTED:
            self._write_error([key])
        self._state[self._name] = {
            **self._state[self._name],
            key: value,
        }

    def __delitem__(self, key: str) -> None:
        if key in self.RESTRICTED:
            self._write_error([key])
        self._state[self._name] = {
            k: v for k, v in self._state[self._name].items() if k != key
        }

    def __iter__(self) -> Iterator[Any]:
        return iter(self._state[self._name])

    def __len__(self) -> int:
        return len(self._state[self._name])

    def __repr__(self) -> str:
        return repr(self._state[self._name])

    def __eq__(self, other: object) -> bool:
        return self._state[self._name] == other

    def keys(self) -> KeysView[str]:
        return self._state[self._name].keys()

    def values(self) -> ValuesView[Any]:
        return self._state[self._name].values()

    def items(self) -> ItemsView[str, Any]:
        return self._state[self._name].items()

    def update(self, mapping: MappingType[str, Any]) -> None:
        if any(k in self.RESTRICTED for k in mapping.keys()):
            self._write_error(
                [k for k in mapping.keys() if k in self.RESTRICTED]
            )
        self._state[self._name] = {
            **self._state[self._name],
            **mapping,
        }

    def pop(self) -> None:
        raise NotImplementedError

    def full(self) -> dict[str, Any]:
        return dict(self._state)

    def _write_error(self, keys: list[str]) -> None:
        raise LookupError(
            f"Cannot set restricted key{'s' if len(keys) > 1 else ''} on "
            f"WorkerState: {', '.join(keys)}"
        )
