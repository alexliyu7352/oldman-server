import asyncio
import os
import platform
import signal
import sys

from collections.abc import Awaitable
from contextlib import contextmanager
from enum import Enum
from typing import Any, Literal

from multidict import CIMultiDict, MultiMapping  # type: ignore

from oldman_server.helpers import Default
from oldman_server.log import error_logger


StartMethod = (
    Default | Literal["fork"] | Literal["forkserver"] | Literal["spawn"]
)

# The asyncio server also runs on Windows (and macOS), for development and
# small local services; the native host is Linux only (oldman_server.native).
OS_IS_WINDOWS = os.name == "nt"
PYPY_IMPLEMENTATION = platform.python_implementation() == "PyPy"
UVLOOP_INSTALLED = False
PYTHON_314_OR_LATER = sys.version_info >= (3, 14)

try:
    import uvloop  # type: ignore # noqa

    UVLOOP_INSTALLED = True
except ImportError:
    pass

# Python 3.11 changed the way Enum formatting works for mixed-in types.
if sys.version_info < (3, 11, 0):

    class StrEnum(str, Enum):
        pass

else:
    from enum import StrEnum  # type: ignore # noqa


class UpperStrEnum(StrEnum):
    """Base class for string enums that are case insensitive."""

    def _generate_next_value_(name, start, count, last_values):
        return name.upper()

    def __eq__(self, value: object) -> bool:
        value = str(value).upper()
        return super().__eq__(value)

    def __hash__(self) -> int:
        return hash(self.value)

    def __str__(self) -> str:
        return self.value


@contextmanager
def use_context(method: StartMethod):
    from oldman_server import OldmanServer

    orig = OldmanServer.start_method
    OldmanServer.start_method = method
    try:
        yield
    finally:
        OldmanServer.start_method = orig


def enable_windows_color_support():
    import ctypes

    kernel = ctypes.windll.kernel32
    kernel.SetConsoleMode(kernel.GetStdHandle(-11), 7)


def pypy_os_module_patch() -> None:
    """
    The PyPy os module is missing the 'readlink' function, which causes issues
    withaiofiles. This workaround replaces the missing 'readlink' function
    with 'os.path.realpath', which serves the same purpose.
    """
    if hasattr(os, "readlink"):
        error_logger.debug(
            "PyPy: Skipping patching of the os module as it appears the "
            "'readlink' function has been added."
        )
        return

    module = sys.modules["os"]
    module.readlink = os.path.realpath  # type: ignore


class Header(CIMultiDict):
    """Container used for both request and response headers.
    It is a subclass of  [CIMultiDict](https://multidict.readthedocs.io/en/stable/multidict.html#cimultidictproxy)

    It allows for multiple values for a single key in keeping with the HTTP
    spec. Also, all keys are *case in-sensitive*.

    Please checkout [the MultiDict documentation](https://multidict.readthedocs.io/en/stable/multidict.html#multidict)
    for more details about how to use the object. In general, it should work
    very similar to a regular dictionary.
    """  # noqa: E501

    def __getattr__(self, key: str) -> str:
        if key.startswith("_"):
            return self.__getattribute__(key)
        key = key.rstrip("_").replace("_", "-")
        return ",".join(self.getall(key, []))

    def get_all(self, key: str):
        """Convenience method mapped to ``getall()``."""
        return self.getall(key, [])


_MISSING: Any = object()


class NativeHeaders:
    """request.headers of a native-host request: reads the native request's
    headers on demand instead of building a Header (CIMultiDict) for every
    request that looks at one.

    Lookups (`[]`, `get`, `getone`, `getall`, `in`, attribute access) query
    the native request directly. Anything else (iteration, `items()`,
    `len()`, `==`, `copy()`, changes) works on a Header built from all the
    headers once, which every later operation then uses, so the results are
    those of a Header. It is a `multidict.MultiMapping` (and a `Mapping`),
    not a `CIMultiDict`.
    """

    __slots__ = ("_header", "_native")

    def __init__(self, native: Any) -> None:
        self._native = native
        self._header: Header | None = None

    def _materialize(self) -> Header:
        if self._header is None:
            self._header = Header(self._native.sanic_headers())
        return self._header

    # ---- Lookups ----

    @staticmethod
    def _check(key: Any) -> None:
        if not isinstance(key, str):
            raise TypeError(
                "CIMultiDict keys should be either str or subclasses of str"
            )

    def _first(self, key: str) -> Any:
        self._check(key)
        try:
            return self._native.sanic_header(key, _MISSING)
        except UnicodeEncodeError:  # not ASCII: no such header
            return _MISSING

    def __getitem__(self, key: str) -> str:
        if self._header is not None:
            return self._header[key]
        value = self._first(key)
        if value is _MISSING:
            raise KeyError(key)
        return value

    def get(self, key: str, default: Any = None) -> Any:
        if self._header is not None:
            return self._header.get(key, default)
        value = self._first(key)
        return default if value is _MISSING else value

    def getone(self, key: str, default: Any = _MISSING) -> Any:
        if self._header is not None:
            return (self._header.getone(key) if default is _MISSING
                    else self._header.getone(key, default))
        value = self._first(key)
        if value is not _MISSING:
            return value
        if default is _MISSING:
            raise KeyError(key)
        return default

    def getall(self, key: str, default: Any = _MISSING) -> Any:
        if self._header is not None:
            return (self._header.getall(key) if default is _MISSING
                    else self._header.getall(key, default))
        self._check(key)
        try:
            values = self._native.sanic_header_all(key)
        except UnicodeEncodeError:
            values = []
        if values:
            return values
        if default is _MISSING:
            raise KeyError(key)
        return default

    def get_all(self, key: str) -> list[str]:
        """Convenience method mapped to ``getall()``."""
        return self.getall(key, [])

    def __contains__(self, key: object) -> bool:
        if self._header is not None:
            return key in self._header
        return isinstance(key, str) and self._first(key) is not _MISSING

    def __getattr__(self, key: str) -> str:
        # Header's attribute access: headers.content_type and the like.
        if key.startswith("_"):
            raise AttributeError(key)
        return ",".join(self.getall(key.rstrip("_").replace("_", "-"), []))

    # ---- Everything else, on a Header ----

    def __iter__(self):
        return iter(self._materialize())

    def __len__(self) -> int:
        return len(self._materialize())

    def __bool__(self) -> bool:
        return bool(self._materialize())

    def __eq__(self, other: object) -> bool:
        if isinstance(other, NativeHeaders):
            other = other._materialize()
        return self._materialize() == other

    __hash__ = None  # type: ignore[assignment]

    def __repr__(self) -> str:
        return repr(self._materialize())

    def keys(self):
        return self._materialize().keys()

    def values(self):
        return self._materialize().values()

    def items(self):
        return self._materialize().items()

    def copy(self):
        return self._materialize().copy()

    def __setitem__(self, key: str, value: str) -> None:
        self._materialize()[key] = value

    def __delitem__(self, key: str) -> None:
        del self._materialize()[key]

    def add(self, key: str, value: str) -> None:
        self._materialize().add(key, value)

    def extend(self, *args: Any, **kwargs: Any) -> None:
        self._materialize().extend(*args, **kwargs)

    def update(self, *args: Any, **kwargs: Any) -> None:
        self._materialize().update(*args, **kwargs)

    def setdefault(self, key: str, default: Any = None) -> Any:
        return self._materialize().setdefault(key, default)

    def pop(self, *args: Any) -> Any:
        return self._materialize().pop(*args)

    def popone(self, *args: Any) -> Any:
        return self._materialize().popone(*args)

    def popall(self, *args: Any) -> Any:
        return self._materialize().popall(*args)

    def popitem(self) -> tuple[str, str]:
        return self._materialize().popitem()

    def clear(self) -> None:
        self._materialize().clear()


MultiMapping.register(NativeHeaders)


use_trio = sys.argv[0].endswith("hypercorn") and "trio" in sys.argv

if use_trio:  # pragma: no cover
    import trio  # type: ignore

    def stat_async(path) -> Awaitable[os.stat_result]:
        return trio.Path(path).stat()

    open_async = trio.open_file
    CancelledErrors = tuple([asyncio.CancelledError, trio.Cancelled])
else:
    if PYPY_IMPLEMENTATION:
        pypy_os_module_patch()

    from aiofiles import open as aio_open  # type: ignore
    from aiofiles.os import stat as stat_async  # type: ignore  # noqa: F401

    async def open_async(file, mode="r", **kwargs):
        return aio_open(file, mode, **kwargs)

    CancelledErrors = tuple([asyncio.CancelledError])


# While the native host serves (NATIVE_SERVER), its file operations run on the
# native shards: an object with serving() (true on the loop it serves),
# stat(path) and read(path, offset, length) returning awaitables.
native_files: Any = None

_stat_async = stat_async


async def stat_async(path) -> os.stat_result:  # type: ignore[no-redef]
    files = native_files
    if files is not None and files.serving():
        return await files.stat(path)
    return await _stat_async(path)


async def read_file_async(path, offset: int = 0, size: int | None = None) -> bytes:
    """open(path, "rb"), seek to `offset` and read `size` bytes (all by default)."""
    files = native_files
    if files is not None and files.serving():
        return await files.read(path, offset, -1 if size is None else size)
    async with await open_async(path, mode="rb") as f:
        if offset:
            await f.seek(offset)
        return await (f.read() if size is None else f.read(size))


def ctrlc_workaround_for_windows(app):
    async def stay_active(app):
        """Asyncio wakeups to allow receiving SIGINT in Python"""
        while not die:
            # If someone else stopped the app, just exit
            if app.state.is_stopping:
                return
            # Windows Python blocks signal handlers while the event loop is
            # waiting for I/O. Frequent wakeups keep interrupts flowing.
            await asyncio.sleep(0.1)
        # Can't be called from signal handler, so call it from here
        app.stop()

    def ctrlc_handler(sig, frame):
        nonlocal die
        if die:
            raise KeyboardInterrupt("Non-graceful Ctrl+C")
        die = True

    die = False
    signal.signal(signal.SIGINT, ctrlc_handler)
    app.add_task(stay_active)


def clear_function_annotate(*funcs):
    """Clear __annotate__ on functions for Python 3.14+ pickle compatibility.

    In Python 3.14, PEP 649 adds __annotate__ to functions with annotations.
    When methods are used in functools.partial and pickled, the __annotate__
    function can cause PicklingError because pickle cannot locate it by name.

    This function sets __annotate__ to None on the given functions to avoid
    pickle issues.
    """
    if PYTHON_314_OR_LATER:
        for func in funcs:
            if hasattr(func, "__annotate__") and func.__annotate__ is not None:
                func.__annotate__ = None
