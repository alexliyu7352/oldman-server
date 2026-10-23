"""One ordinary CPython asyncio loop, real Seastar shards, no worker PyObjects.

Use `await engine.aclose()` before loop exit. A canceled Python request retains
its native capacity until completion; canceling does not interrupt native work.
"""
import asyncio
import atexit
from collections import deque
from dataclasses import dataclass
import logging
import os
import threading
import time
import inspect
from types import CoroutineType

from . import _native


def _settle(future, result, error):
    if not future.done():
        if error is None:
            future.set_result(result)
        else:
            future.set_exception(error)


async def _off_loop(func, *args):
    """func(*args), a blocking native lifecycle call, on its own daemon thread.
    Not the loop's default executor: a signal can interrupt its submit after
    the worker thread started but before the executor recorded it, and that
    non-daemon thread then keeps the process from exiting (a Sanic worker
    told to stop while it starts, say)."""
    loop = asyncio.get_running_loop()
    future = loop.create_future()

    def run():
        try:
            result, error = func(*args), None
        except BaseException as caught:
            result, error = None, caught
        try:
            loop.call_soon_threadsafe(_settle, future, result, error)
        except RuntimeError:
            pass  # the loop closed meanwhile: nobody awaits the result

    threading.Thread(target=run, name="native-lifecycle", daemon=True).start()
    return await future


@dataclass(frozen=True)
class ServerConfig:
    http_port: int = 0
    tcp_port: int = 0
    udp_port: int = 0
    max_requests: int = 256
    max_bytes: int = 64 * 1024 * 1024
    max_body_bytes: int = 1024 * 1024
    decision_timeout_ms: int = 1000
    file_path: str = ""
    coalesce_notifications: bool = False
    cooperative_compression: bool = False
    inline_compression: bool = False
    upstream_ports: tuple[int, ...] = ()
    application_routes: tuple[tuple[str, str], ...] = ()
    generate_date_header: bool = False
    max_response_bytes: int = 65536
    native_json: bool = False
    application_dispatch_all: bool = False
    # Diagnostic headers on the framework host: "all", "pid" or "none".
    probe_headers: str = "all"
    # Framework host (application_dispatch_all) HTTP/1.1 settings; 0 disables
    # a timeout.
    http_host: str = "127.0.0.1"
    # Instead of http_host:http_port: a listening socket's fd (TCP or AF_UNIX;
    # the caller keeps it open while serving) and, for AF_UNIX, the path
    # requests report as the server address.
    listen_fd: int = -1
    unix_path: str = ""
    # Other worker processes accept on listen_fd too: shard 0 accepts on its
    # own, so that stopping does not shut the socket down for them.
    listen_shared: bool = False
    # Worker processes sharing listen_fd take turns accepting
    # (oldman_server.native.accept): the address of the shared table of
    # accept_slots lines, and this process's line (-1: not taking part).
    accept_counts: int = 0
    accept_slots: int = 0
    accept_slot: int = -1
    # This process's own listening socket besides listen_fd
    # (NATIVE_WORKER_LISTEN) and, for AF_UNIX, the path its requests report.
    worker_listen_fd: int = -1
    worker_unix_path: str = ""
    max_header_bytes: int = 8192
    keep_alive: bool = True
    # A request body still arriving goes to Python at most this far ahead of
    # what its handler has read.
    body_window_bytes: int = 65536
    keep_alive_timeout_ms: int = 0
    request_timeout_ms: int = 0
    # A write the peer does not take within this time closes the connection.
    write_timeout_ms: int = 0
    # Protocol errors are rendered by application_error_handler and request body
    # errors raised from Engine.receive, instead of plain native error responses.
    framework_errors: bool = False


@dataclass(frozen=True)
class NativePolicy:
    """Rules executed in C++; this does not compile or run a Python callback."""
    allow_fixed: bool = False
    allow_file: bool = False
    allow_compress: bool = False
    max_payload_bytes: int = 1024 * 1024


@dataclass(frozen=True)
class Request:
    id: int
    shard: int
    method: str
    route: str
    body_size: int
    body: bytes | None = None
    parameter: str = ""
    parent: int = 0


# Python owns this extension object; its plain native data outlives the request
# independently of the server. Header/body Python objects are built on access.
ApplicationRequest = _native.ApplicationRequest


@dataclass(frozen=True)
class Config:
    shard_cpus: tuple[int, ...]
    max_pending: int = 1024
    max_pending_bytes: int = 64 * 1024 * 1024
    completion_batch: int = 64
    worker_cpus: tuple[int, ...] = ()
    server: ServerConfig | None = None
    batch_decisions: bool = False
    share_worker_cpus: bool = False
    retire_http_contexts: bool = False
    # Seastar's reactor backend: "epoll", "linux-aio" or "io_uring"; "" lets
    # Seastar take the first available of io_uring, linux-aio and epoll.
    reactor_backend: str = "epoll"


@dataclass(frozen=True)
class Result:
    payload: bytes
    server_pid: int
    worker_tid: int
    shard: int
    metrics: dict | None = None


class BusyError(Exception):
    pass


class NativeOperationError(RuntimeError):
    def __init__(self, status):
        self.status = status
        super().__init__(f"native HTTP operation failed with status {status}")


@dataclass(frozen=True)
class NativeBody:
    """An opaque response body owned by its native HTTP request, not Python bytes."""
    size: int
    status: int = 200


@dataclass(frozen=True)
class HttpResponse:
    status: int = 200
    body: NativeBody | None = None
    tag: str = ""


class HttpRequest:
    """Experimental one-operation HTTP context; native storage lasts until response."""
    def __init__(self, engine, event):
        self._engine, self._event = engine, event
        self.method, self.body_size, self.parameter = event.method, event.body_size, event.parameter
        self._phase = event
        self._used = self._done = False
        self._future = self._body = None
        self._native_finished = False
        self._terminal_code = 503
        self._task = None

    async def compress(self, *, level=6):
        if self._done or self._used:
            raise RuntimeError("request compression can be awaited only once while active")
        if type(level) is not int or not 0 <= level <= 9:
            raise ValueError("level must be an integer in 0..9")
        if self._event.route != "python_compress":
            raise ValueError("request is not a compression operation")
        if not self._engine.config.worker_cpus and not (
                self._engine.config.server.cooperative_compression or self._engine.config.server.inline_compression):
            raise ValueError("worker pool is not configured")
        self._used = True
        self._future = self._engine._loop.create_future()
        self._engine._resolve_http(self._event.id, self._event.shard, 100 + level, b"")
        return await self._future

    async def fetch(self, *, upstream=0):
        if self._done or self._used:
            raise RuntimeError("request operation can be awaited only once while active")
        if self._event.route != "python_fetch":
            raise ValueError("request is not an upstream operation")
        if type(upstream) is not int or not 0 <= upstream < len(self._engine.config.server.upstream_ports):
            raise ValueError("upstream must identify a configured origin")
        self._used = True
        self._future = self._engine._loop.create_future()
        self._engine._resolve_http(self._event.id, self._event.shard, 1000 + upstream, b"")
        return await self._future


# Seastar starts once per process. Engines opened with persistent=True share one
# runtime and start/stop only its network server: (runtime, shard CPUs, worker CPUs).
_persistent = None
# A process forked after Seastar started (Sanic's "fork" start method, say)
# has the runtime's memory but not its threads, and Seastar does not start
# twice in a process: such a child cannot serve natively.
_started = False
_forked_after_start = False

# How long aclose waits for cancelled handlers. One that ignores its
# cancellation is left running, as Sanic leaves it when it closes the loop.
_CANCELLED_WAIT = 1.0


def _after_fork_in_child():
    global _persistent, _forked_after_start
    if _started:
        _persistent = None
        _forked_after_start = True


os.register_at_fork(after_in_child=_after_fork_in_child)


def startable():
    """Whether this process can start the native runtime: it is not a child
    forked after the runtime started in its parent."""
    return not _forked_after_start


def _new_runtime(*args):
    global _started
    if _forked_after_start:
        raise RuntimeError("the native host cannot start in a process forked after it "
                           "started; use the spawn start method")
    _started = True
    return _native.Runtime(*args)


def _persistent_runtime(config):
    """The process's persistent runtime, created on first use: (runtime, created)."""
    global _persistent
    if _persistent is None:
        runtime = _new_runtime(list(config.shard_cpus), config.max_pending,
                               config.max_pending_bytes, list(config.worker_cpus),
                               _native.ServerConfig(), config.reactor_backend)
        _persistent = (runtime, tuple(config.shard_cpus), tuple(config.worker_cpus),
                       config.reactor_backend)
        atexit.register(_stop_persistent_runtime)
        return runtime, True
    runtime, shards, workers, backend = _persistent
    if (shards, workers) != (tuple(config.shard_cpus), tuple(config.worker_cpus)):
        raise ValueError("the persistent native runtime already runs on other CPUs")
    if backend != config.reactor_backend:
        raise ValueError(f"the persistent native runtime already runs the {backend or 'default'} reactor backend")
    return runtime, False


def _stop_persistent_runtime():
    if _persistent is None:
        return  # a forked child's: not its runtime
    runtime = _persistent[0]
    while runtime.stats()["state"] == 0:
        time.sleep(.001)
    if runtime.stats()["state"] == 1:
        runtime.stop()
    runtime.join()
    runtime.close_fd()


class _ApplicationStream:
    """A streamed application response: bytes queued but not yet written, and
    a file range the shard is writing into it (future, path)."""
    __slots__ = ("unacked", "waiter", "file")

    def __init__(self):
        self.unacked = 0
        self.waiter = None
        self.file = None


class _Inbound:
    """The rest of a request body still arriving as application_data events."""
    __slots__ = ("shard", "chunks", "ended", "error", "waiter")

    def __init__(self, shard):
        self.shard = shard
        self.chunks = deque()
        self.ended = False
        self.error = None
        self.waiter = None


# In _Inbound.chunks: the body declared more than max_body_bytes from here on.
_OVER_LIMIT = object()


class BodyError(Exception):
    """The request body cannot be read: the host rejected it (status, message)."""

    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


# Decision kinds of the framework host (native/server.hh DecisionKind). A
# STREAM_START with status 101 makes the connection a tunnel (WebSocket).
STREAM_START, STREAM_DATA, STREAM_END, STREAM_ABORT = 1, 2, 3, 4
TUNNEL_PAUSE, TUNNEL_RESUME = 5, 6
STREAM_FILE = 7
BODY_READ = 8  # the handler read this many bytes of a request body still arriving
FILE_STAT, FILE_READ, FILE_OPEN = 0, 1, 2  # native file operations


def _file_path(path):
    """The bytes of a path a shard opens. Its C string would end at a NUL byte
    (secret.key\\0.txt reading secret.key): refused, as os.stat and open do."""
    encoded = os.fsencode(path)
    if b"\0" in encoded:
        raise ValueError("embedded null byte")
    return encoded


def _stat_result(values):
    """os.stat_result as os.stat builds it, from a native file_stat's numbers."""
    mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime, blksize, blocks, rdev = values
    (a, a_ns), (m, m_ns), (c, c_ns) = (divmod(t, 1_000_000_000) for t in (atime, mtime, ctime))
    return os.stat_result((mode, ino, dev, nlink, uid, gid, size, a, m, c), {
        "st_atime": a + a_ns * 1e-9, "st_mtime": m + m_ns * 1e-9, "st_ctime": c + c_ns * 1e-9,
        "st_atime_ns": atime, "st_mtime_ns": mtime, "st_ctime_ns": ctime,
        "st_blksize": blksize, "st_blocks": blocks, "st_rdev": rdev})
# Streamed bytes Python may queue before waiting for the shard to write them.
STREAM_WINDOW = 65536


class Engine:
    @classmethod
    async def open(cls, config: Config, *, decision_handler=None, http_handler=None,
                   application_handler=None, application_native_results=False,
                   application_bytes_headers=False, persistent=False,
                   application_error_handler=None, application_connections=None,
                   opened=None):
        """persistent: share the process's Seastar runtime across Engines; this
        Engine starts and stops only the network server, so the process can
        serve again later (requires an HTTP ServerConfig).

        opened(engine): called with the Engine before its server accepts
        connections (with a persistent runtime: once the runtime runs, so the
        caller may narrow its own thread's CPUs). The server dispatches
        requests while open() still awaits its start, so a caller that reaches
        the Engine from its handlers learns it here.

        application_error_handler(request): renders a protocol error the host
        detected (request.error is "<status>:<message>") like application_handler;
        without it the host's plain error response is used.
        application_connections: a dict of the open connections by request.connection
            (a state with connection_lost(), or None or False while none exists;
            False: its peer left).
        A connection maps to None until the handler stores its state there; on
        close the entry is removed and the state's connection_lost() called."""
        if (not config.shard_cpus or len(set(config.shard_cpus)) != len(config.shard_cpus)
                or not set(config.shard_cpus) <= os.sched_getaffinity(0)):
            raise ValueError("shard_cpus must be distinct available CPUs")
        if min(config.max_pending, config.max_pending_bytes, config.completion_batch) <= 0:
            raise ValueError("bounds must be positive")
        if (len(set(config.worker_cpus)) != len(config.worker_cpus)
                or not set(config.worker_cpus) <= os.sched_getaffinity(0)
                or (not config.share_worker_cpus and set(config.worker_cpus) & set(config.shard_cpus))):
            raise ValueError("worker_cpus must be distinct available CPUs separate from shards")
        if config.server:
            s = config.server
            if s.cooperative_compression and s.inline_compression:
                raise ValueError("choose one HTTP compression execution mode")
            if type(s.listen_fd) is not int or s.listen_fd < -1 or type(s.unix_path) is not str:
                raise ValueError("listen_fd must be an fd or -1 and unix_path a str")
            if s.listen_fd >= 0 and not s.application_dispatch_all:
                raise ValueError("a listening socket requires framework dispatch")
            if (type(s.worker_listen_fd) is not int or s.worker_listen_fd < -1
                    or type(s.worker_unix_path) is not str
                    or (s.worker_listen_fd >= 0 and not s.application_dispatch_all)):
                raise ValueError("worker_listen_fd must be an fd or -1, with framework dispatch")
            if (any(type(v) is not int for v in (s.accept_counts, s.accept_slots, s.accept_slot))
                    or s.accept_counts < 0 or not -1 <= s.accept_slot < max(s.accept_slots, 0)
                    or ((s.accept_counts or s.listen_shared) and s.listen_fd < 0)):
                raise ValueError("accept turns need a shared listen_fd and a slot of the table")
            if not any((s.http_port, s.tcp_port, s.udp_port, s.listen_fd >= 0, s.worker_listen_fd >= 0)):
                raise ValueError("at least one server port is required")
            if any(not isinstance(p, int) or not 0 <= p <= 65535 for p in (s.http_port, s.tcp_port, s.udp_port)):
                raise ValueError("ports must be integers in 0..65535")
            if s.http_port and s.http_port == s.tcp_port:
                raise ValueError("HTTP and framed TCP require separate ports")
            # Without framework_errors the host buffers whole request bodies.
            if (min(s.max_requests, s.max_bytes, s.max_body_bytes, s.decision_timeout_ms) <= 0
                    or s.max_body_bytes > (1 << 62 if s.framework_errors else 128 * 1024 * 1024)
                    or s.decision_timeout_ms > 86_400_000):
                raise ValueError("invalid server bounds")
            if type(s.body_window_bytes) is not int or not 1 <= s.body_window_bytes <= 1 << 30:
                raise ValueError("body_window_bytes must be within 1..1 GiB")
            if s.file_path and not os.path.isfile(s.file_path):
                raise ValueError("file_path must identify a regular file")
            if len(s.upstream_ports) > 16 or any(type(p) is not int or not 1 <= p <= 65535 for p in s.upstream_ports):
                raise ValueError("configure at most 16 valid upstream ports")
            if type(s.max_response_bytes) is not int or not 0 < s.max_response_bytes <= 128 * 1024 * 1024:
                raise ValueError("invalid response byte limit")
            if s.application_routes and not s.http_port:
                raise ValueError("application routes require an HTTP port")
            if type(s.application_dispatch_all) is not bool:
                raise ValueError("application_dispatch_all must be bool")
            if s.probe_headers not in ("all", "pid", "none"):
                raise ValueError("probe_headers must be 'all', 'pid' or 'none'")
            if type(s.http_host) is not str or not s.http_host:
                raise ValueError("http_host must be a non-empty address")
            if type(s.max_header_bytes) is not int or not 1 <= s.max_header_bytes <= 65536:
                raise ValueError("max_header_bytes must be within 1..65536")
            if type(s.keep_alive) is not bool or type(s.framework_errors) is not bool:
                raise ValueError("keep_alive and framework_errors must be bool")
            if any(type(v) is not int or not 0 <= v <= 86_400_000
                   for v in (s.keep_alive_timeout_ms, s.request_timeout_ms, s.write_timeout_ms)):
                raise ValueError("timeouts must be milliseconds within 0..86400000")
            if s.application_dispatch_all:
                if (not s.http_port and s.listen_fd < 0) or s.application_routes or s.native_json:
                    raise ValueError("framework dispatch requires HTTP without exact routes or native JSON")
                if application_handler is None:
                    raise ValueError("framework dispatch requires an application_handler")
        if decision_handler is not None and not callable(decision_handler):
            raise TypeError("decision_handler must be callable")
        if http_handler is not None and not callable(http_handler):
            raise TypeError("http_handler must be callable")
        if application_handler is not None and not callable(application_handler):
            raise TypeError("application_handler must be callable")
        if application_error_handler is not None and not callable(application_error_handler):
            raise TypeError("application_error_handler must be callable")
        if application_native_results and config.server is None:
            raise ValueError("native application results require an HTTP ServerConfig")
        if type(application_bytes_headers) is not bool:
            raise TypeError("application_bytes_headers must be bool")
        if application_bytes_headers and config.server is None:
            raise ValueError("bytes application headers require an HTTP ServerConfig")
        if persistent and config.server is None:
            raise ValueError("a persistent Engine requires an HTTP ServerConfig")
        self = cls()
        self.config = config
        self._persistent = persistent
        self._loop = asyncio.get_running_loop()
        self._pending = {}
        self._id = 0
        self._closed = False
        self._close_task = None
        self._ready = self._loop.create_future()
        self._stopped = self._loop.create_future()
        self._empty = asyncio.Event()
        self._empty.set()
        self._native = None
        self._files = {}  # native file operations: id -> (future, operation, path)
        self._file_id = 0
        self._server_generation = 0  # the persistent runtime's server this Engine started
        self._abandoned = False  # its server stopped by abandon(); the runtime may serve another
        self._draining = False
        self._reader_registered = False
        self._decision_handler = decision_handler
        self._http_handler = http_handler
        self._application_handler = application_handler
        self._application_error_handler = application_error_handler
        self.application_connections = (application_connections
                                        if application_connections is not None else {})
        self._application_timeouts = set()
        # Requests whose peer left (FIN or RST) while they ran: cancelled,
        # as Sanic's connection_lost cancels the handler.
        self._application_lost = set()
        self._application_native_results = application_native_results
        self._application_bytes_headers = application_bytes_headers
        self._application_tasks = {}
        self._application_returns = (
            _native.ApplicationResponseBatch(config.server.max_response_bytes,
                                                    bytes_headers=application_bytes_headers)
            if application_native_results else [])
        self._application_flush_scheduled = False
        self._application_terminal = set()
        self._application_streams = {}
        self._application_inbound = {}
        self._application_tunnels = {}
        self._http_tasks = set()
        self._http_waiting = {}
        self._http_returns = []
        self._http_flush_scheduled = False
        self._decision_stats = dict(python_decisions=0, python_decision_cpu_ns=0,
                                    python_decision_wall_ns=0, python_decision_errors=0,
                                    python_http_started=0, python_http_finished=0,
                                    python_http_resumed=0, python_http_errors=0,
                                    python_application_started=0, python_application_finished=0,
                                    python_application_errors=0)
        self._opened = opened
        # A separate task owns startup and its error cleanup. Caller cancellation
        # never cancels this task or transfers an unowned runtime to the loop.
        startup = self._loop.create_task(self._start())
        try:
            return await asyncio.shield(startup)
        except asyncio.CancelledError:
            cleanup = self._loop.create_task(self._cancel_start(startup))
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    # Additional cancel() calls must not interrupt resource cleanup.
                    continue
            cleanup.result()
            raise

    async def _start(self):
        c = self.config
        server = _native.ServerConfig()
        if c.server:
            for key, value in vars(c.server).items():
                setattr(server, key, value)
        if self._persistent:
            return await self._start_persistent(server)
        if self._opened is not None:
            self._opened(self)
        try:
            self._native = _new_runtime(list(c.shard_cpus), c.max_pending, c.max_pending_bytes, list(c.worker_cpus), server,
                                        c.reactor_backend)
            self.notification_fd = self._native.fd()
            self._loop.add_reader(self.notification_fd, self._drain)
            self._reader_registered = True
            await self._ready
            return self
        except Exception:
            # Registration can fail, so cleanup cannot require a working reader.
            if self._native is not None:
                if self._reader_registered:
                    self._loop.remove_reader(self.notification_fd)
                    self._reader_registered = False
                while self._native.stats()["state"] == 0:
                    await asyncio.sleep(.001)
                if self._native.stats()["state"] == 1:
                    self._native.stop()
                while self._native.stats()["state"] < 3:
                    await asyncio.sleep(.001)
                await _off_loop(self._native.join)
                self._native.close_fd()
            raise

    async def _start_persistent(self, server):
        runtime, created = _persistent_runtime(self.config)
        self._native = runtime
        self.notification_fd = runtime.fd()
        self._loop.add_reader(self.notification_fd, self._drain)
        self._reader_registered = True
        try:
            if created:
                await self._ready  # Seastar reports running through the notification fd
            if self._opened is not None:
                self._opened(self)
            self._server_generation = await _off_loop(runtime.start_server, server)
        except BaseException:
            self._loop.remove_reader(self.notification_fd)
            self._reader_registered = False
            raise
        if not self._ready.done():
            self._ready.set_result(None)
        return self

    async def _cancel_start(self, startup):
        try:
            await startup
        except Exception:
            return  # _start already joined and released a failed runtime.
        await self.aclose()

    async def echo(self, payload: bytes, *, delay_ms: int = 0) -> Result:
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("Engine belongs to a different event loop")
        if self._closed:
            raise RuntimeError("Engine is closing or closed")
        if not isinstance(payload, bytes):
            raise TypeError("first probe accepts immutable bytes only")
        if not isinstance(delay_ms, int) or not 0 <= delay_ms <= 60_000:
            raise ValueError("delay_ms must be an integer in 0..60000")
        # Avoid making an oversized native copy just to reject it.
        if len(payload) > self.config.max_pending_bytes:
            raise BusyError("native byte budget exceeded")
        self._id += 1
        request_id = self._id
        fut = self._loop.create_future()
        if not self._native.submit(request_id, payload, delay_ms):
            raise BusyError("native request or byte budget exhausted")
        self._pending[request_id] = fut
        self._empty.clear()
        del payload  # native owns its input; no borrowed Python object
        return await fut

    async def install_policy(self, policy: NativePolicy) -> int:
        """Await installation on all shards. In-flight requests keep their old version.

        Canceling the await does not roll back an admitted update; shutdown waits
        for it, just like other native operations. /hybrid retains Python callbacks.
        """
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("Engine belongs to a different event loop")
        if self._closed:
            raise RuntimeError("Engine is closing or closed")
        if not self.config.server:
            raise RuntimeError("server is not configured")
        if not isinstance(policy, NativePolicy):
            raise TypeError("policy must be NativePolicy")
        if type(policy.max_payload_bytes) is not int or not 0 < policy.max_payload_bytes <= 128*1024*1024:
            raise ValueError("invalid policy payload bound")
        native = _native.NativePolicy()
        for key, value in vars(policy).items():
            if key != "max_payload_bytes" and type(value) is not bool:
                raise TypeError("policy permissions must be bool")
            setattr(native, key, value)
        self._id += 1
        request_id = self._id
        fut = self._loop.create_future()
        if not self._native.submit_policy(request_id, native):
            raise BusyError("native request budget exhausted")
        self._pending[request_id] = fut
        self._empty.clear()
        result = await fut
        return int(result.payload)

    async def compress(self, payload: bytes, *, mode: str = "pool", level: int = 6) -> Result:
        if mode not in ("pool", "cooperative"):
            raise ValueError("mode must be pool or cooperative")
        if not isinstance(level, int) or not -1 <= level <= 9:
            raise ValueError("zlib level must be -1..9")
        return await self._work(payload, 1 if mode == "pool" else 2, level)

    async def blocking_wait(self, milliseconds: int = 20) -> Result:
        if not isinstance(milliseconds, int) or not 0 <= milliseconds <= 60000:
            raise ValueError("wait must be an integer in 0..60000 ms")
        return await self._work(b"", 3, milliseconds)

    async def _work(self, payload, operation, parameter):
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("Engine belongs to a different event loop")
        if self._closed:
            raise RuntimeError("Engine is closing or closed")
        if not isinstance(payload, bytes):
            raise TypeError("probe accepts immutable bytes only")
        if operation != 2 and not self.config.worker_cpus:
            raise ValueError("worker pool is not configured")
        reserve = _native.compression_reservation(len(payload)) if operation != 3 else 0
        if reserve > self.config.max_pending_bytes:
            raise BusyError("native input/output byte budget exceeded")
        self._id += 1
        request_id = self._id
        fut = self._loop.create_future()
        if not self._native.submit_work(request_id, payload, operation, parameter):
            raise BusyError("native request or byte budget exhausted")
        self._pending[request_id] = fut
        self._empty.clear()
        del payload
        result = await fut
        result.metrics["python_resume_ns"] = time.monotonic_ns() - result.metrics["python_delivered_ns"]
        return result

    def _resolve_http(self, request_id, shard, code, body, retire=False):
        if not self.config.batch_decisions:
            self._native.resolve_request(request_id, shard, code, body, retire)
            return
        item = (request_id, shard, code, body)
        self._http_returns.append(item + (True,) if retire else item)
        if not self._http_flush_scheduled:
            self._http_flush_scheduled = True
            self._loop.call_soon(self._flush_http)

    def _flush_http(self):
        self._http_flush_scheduled = False
        if self._http_returns:
            batch, self._http_returns = self._http_returns, []
            self._native.resolve_requests(batch)

    def _queue_application(self, request_id, shard, status, body=b"", headers=()):
        self._queue_application_result(request_id, shard, (status, body, headers))

    def _queue_application_result(self, request_id, shard, result):
        if request_id in self._application_terminal:
            return
        try:
            if self._application_native_results:
                if not self._application_returns.append_result(request_id, shard, result):
                    self._decision_stats["python_application_errors"] += 1
            else:
                self._application_returns.append((request_id, shard, *result))
        finally:
            # The native boundary queues an empty 500 even when a serializer
            # raises BaseException. It must be flushed before that error escapes.
            if self._application_returns and not self._application_flush_scheduled:
                self._application_flush_scheduled = True
                self._loop.call_soon(self._flush_application)

    async def receive(self, request_id, limited=True):
        """The next piece of a request body that is still arriving, or None at its
        end. limited: a chunked body past max_body_bytes raises BodyError 413
        (otherwise, for a stream handler, it is read on)."""
        inbound = self._application_inbound.get(request_id)
        if inbound is None:
            return None
        while True:
            while not inbound.chunks and not inbound.ended and inbound.error is None:
                inbound.waiter = self._loop.create_future()
                try:
                    await inbound.waiter
                finally:
                    inbound.waiter = None
            if not inbound.chunks or inbound.chunks[0] is not _OVER_LIMIT:
                break
            inbound.chunks.popleft()
            if limited:
                raise BodyError(413, "Request body exceeds the size limit")
        if inbound.chunks:
            data = inbound.chunks.popleft()
            # The shard sends at most body_window_bytes ahead of what was read.
            if self._application_native_results:
                self._application_returns.append_body_read(request_id, inbound.shard, len(data))
                if not self._application_flush_scheduled:
                    self._application_flush_scheduled = True
                    self._loop.call_soon(self._flush_application)
            else:
                self._queue_stream_item((request_id, inbound.shard, 0, b"", (), BODY_READ, len(data)))
            return data
        if inbound.error is not None:
            raise BodyError(*inbound.error)
        return None

    def timed_out(self, request_id):
        """Whether the host reported the response deadline of this request."""
        return request_id in self._application_timeouts

    def lost(self, request_id):
        """Whether the host reported that this request's peer left."""
        return request_id in self._application_lost

    async def stream_send(self, request_id, shard, kind, body, status=0, headers=(), length=-1):
        """Queue one piece of a streamed application response.

        STREAM_START carries the status, encoded headers and the application's
        Content-Length (or -1); later pieces carry data. Waits while more than
        STREAM_WINDOW bytes are queued but not yet written by the shard.
        """
        if self._application_native_results:
            raise RuntimeError("streamed responses require the checked response path")
        stream = self._application_streams.get(request_id)
        if stream is None:
            stream = self._application_streams[request_id] = _ApplicationStream()
        self._queue_stream_item((request_id, shard, status, body, headers, kind, length)
                                if kind == STREAM_START else (request_id, shard, 0, body, (), kind))
        stream.unacked += len(body)
        if kind == STREAM_END:
            del self._application_streams[request_id]
            return
        while stream.unacked > STREAM_WINDOW:
            stream.waiter = self._loop.create_future()
            try:
                await stream.waiter
            finally:
                stream.waiter = None

    def stream_write(self, request_id, shard, body):
        """Queue stream data without waiting for the window (a transport's
        synchronous write); stream_send's waits still count it."""
        stream = self._application_streams.get(request_id)
        if stream is None:
            return
        self._queue_stream_item((request_id, shard, 0, body, (), STREAM_DATA))
        stream.unacked += len(body)

    def stream_end(self, request_id, shard):
        """End a stream without waiting (a transport's synchronous close)."""
        if self._application_streams.pop(request_id, None) is not None:
            self._queue_stream_item((request_id, shard, 0, b"", (), STREAM_END))

    def open_tunnel(self, request_id, protocol):
        """Deliver the peer's bytes after a 101 to `protocol` (data_received,
        eof_received, connection_lost), from the next drained event on."""
        self._application_tunnels[request_id] = protocol

    def pause_tunnel(self, request_id, shard, paused):
        """Stop (or restart) the shard reading the tunnel's peer."""
        if request_id in self._application_tunnels:
            self._queue_stream_item((request_id, shard, 0, b"", (),
                                     TUNNEL_PAUSE if paused else TUNNEL_RESUME))

    def _tunnel_lost(self, request_id):
        protocol = self._application_tunnels.pop(request_id, None)
        if protocol is not None:
            self._application_streams.pop(request_id, None)
            protocol.connection_lost(None)

    def stream_file(self, request_id, shard, path, offset, length, chunk_size):
        """Write `length` bytes of the file at `path` from `offset` into an open
        stream (STREAM_FILE): the shard reads them and frames each chunk_size
        piece as a separate write. Returns a future of the bytes written; OSError
        when the shard cannot read the file."""
        stream = self._application_streams.get(request_id)
        if stream is None or stream.file is not None:
            raise RuntimeError("the stream is not open for a file")
        path = os.fspath(path)
        encoded = _file_path(path)
        future = self._loop.create_future()
        stream.file = future, path
        body = b"%d %d %d %s" % (offset, length, chunk_size, encoded)
        self._queue_stream_item((request_id, shard, 0, body, (), STREAM_FILE))
        return future

    def file_stat(self, path):
        """os.stat(path), run on a native shard: a future of an os.stat_result."""
        return self._file_operation(FILE_STAT, path, 0, -1)

    def file_read(self, path, offset=0, length=-1):
        """`length` bytes of the file at `path` from `offset` (-1: to the end),
        read on a native shard: a future of bytes. OSError as open() and read()
        raise it (a directory: IsADirectoryError)."""
        return self._file_operation(FILE_READ, path, offset, length)

    def file_open_stat(self, path):
        """open(path, "rb") and os.fstat of it, on a native shard: a future of an
        os.stat_result; OSError as open() raises it."""
        return self._file_operation(FILE_OPEN, path, 0, 0)

    def _file_operation(self, operation, path, offset, length):
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("Engine belongs to a different event loop")
        if self._closed:
            raise RuntimeError("Engine is closing or closed")
        path = os.fspath(path)
        encoded = _file_path(path)
        self._file_id += 1
        future = self._loop.create_future()
        self._native.submit_file(self._file_id, operation, encoded, offset, length)
        self._files[self._file_id] = future, operation, path
        return future

    def _drain_files(self):
        for file_id, value, error in self._native.drain_files(self.config.completion_batch):
            future, operation, path = self._files.pop(file_id)
            if future.done():
                continue
            if error:
                future.set_exception(OSError(error, os.strerror(error), path))
            else:
                future.set_result(value if operation == FILE_READ else _stat_result(value))

    def stream_abort(self, request_id, shard):
        """End a streamed response that cannot be completed; the shard closes
        the connection without terminating the response."""
        self._application_streams.pop(request_id, None)
        self._queue_stream_item((request_id, shard, 0, b"", (), STREAM_ABORT))

    def _queue_stream_item(self, item):
        self._application_returns.append(item)
        if not self._application_flush_scheduled:
            self._application_flush_scheduled = True
            self._loop.call_soon(self._flush_application)

    def _flush_application(self):
        self._application_flush_scheduled = False
        if self._application_returns:
            if self._application_native_results:
                self._application_returns.flush(self._native)
            else:
                batch, self._application_returns = self._application_returns, []
                # User-field validation is per response at the native boundary.
                # One bad reply becomes an empty 500 without dropping its siblings.
                # The framework host writes HTTP/1.1 itself, including 204/304.
                invalid = self._native.resolve_http_responses_checked(
                    batch, self.config.server.max_response_bytes,
                    bytes_headers=self._application_bytes_headers,
                    no_body_statuses=self.config.server.application_dispatch_all)
                self._decision_stats["python_application_errors"] += invalid

    async def _serve_application(self, request_id, shard, request, handler):
        """Queue a complete owned response while the Task still owns its wrapper."""
        result = (500, b"", ())
        try:
            # The deadline is owned natively: the shard starts decision_timeout_ms
            # when it publishes the request (before this Task exists), then sends
            # application_timeout, which cancels this Task for the handler to
            # answer; without an answer it replies 503 and sends application_done.
            answer = handler(request)
            if type(answer) is CoroutineType or inspect.isawaitable(answer):
                answer = await answer
            if answer is None or self._application_native_results:
                result = answer  # None: the handler streamed its response
            elif len(answer) == 3:
                # Preserve the legacy Engine handler's tuple-unpacking API.
                status, body, headers = answer
                result = (status, body, headers)
            else:  # plus the decision kind and a HEAD response's Content-Length
                status, body, headers, kind, length = answer
                result = (status, body, headers, kind, length)
        except asyncio.CancelledError:
            # The response deadline (application_timeout) or a cancellation.
            result = (504 if request_id in self._application_timeouts else 503, b"", ())
        except TimeoutError:  # Raised by the handler itself.
            result = (504, b"", ())
        except Exception:
            self._decision_stats["python_application_errors"] += 1
            result = (500, b"", ())
        except BaseException:
            self._decision_stats["python_application_errors"] += 1
            raise
        finally:
            # The coroutine owns its bookkeeping, so no per-Task done callback is
            # scheduled. A Task cancelled before its first step never runs this;
            # _reap_unstarted_application covers that case where it is cancelled.
            self._application_tasks.pop(request_id, None)
            if self._application_inbound:
                self._application_inbound.pop(request_id, None)
            if self._application_timeouts:
                self._application_timeouts.discard(request_id)
            if self._application_lost:
                self._application_lost.discard(request_id)
            try:
                if result is not None:
                    self._queue_application_result(request_id, shard, result)
            except BaseException:
                # A serializer may raise BaseException after the native batch
                # already queued an empty 500 for this request.
                self._decision_stats["python_application_errors"] += 1
                raise
            finally:
                self._application_terminal.discard(request_id)
                self._decision_stats["python_application_finished"] += 1

    def _lose_application(self, request_id):
        entry = self._application_tasks.get(request_id)
        if entry is not None:
            self._application_lost.add(request_id)
            entry[0].cancel()
            self._reap_unstarted_application(request_id)

    def _reap_unstarted_application(self, request_id):
        """Finish a registered Task whose coroutine body never ran (its finally won't)."""
        entry = self._application_tasks.get(request_id)
        if entry is None:
            return
        task, shard = entry
        if not task.done() and inspect.getcoroutinestate(task.get_coro()) != inspect.CORO_CREATED:
            return  # Started: its own finally completes the bookkeeping.
        del self._application_tasks[request_id]
        self._decision_stats["python_application_finished"] += 1
        if request_id in self._application_terminal:
            self._application_terminal.discard(request_id)
        else:
            self._decision_stats["python_application_errors"] += 1
            self._queue_application(request_id, shard, 503)

    def _application_event(self, values):
        request = None
        handler = self._application_handler
        if len(values) == 3:
            request_id, shard, request = values
        elif len(values) == 4:
            request_id, shard, request, marker = values
            if marker == "more":  # the body is still arriving
                self._application_inbound[request_id] = _Inbound(shard)
            else:  # a protocol error, without a request
                handler = self._application_error_handler
                if handler is None:
                    self._queue_application(request_id, shard, int(request.error.split(":", 1)[0]))
                    return
        else:
            request_id, shard, method, route, size, body, parameter, parent = values[:8]
            if route == "application_data":
                tunnel = self._application_tunnels.get(parent)
                if tunnel is not None:
                    if body:
                        tunnel.data_received(body)
                    if parameter == "end":
                        tunnel.eof_received()
                    elif parameter == "lost":
                        self._tunnel_lost(parent)
                    return
                inbound = self._application_inbound.get(parent)
                if inbound is not None:
                    if body:
                        inbound.chunks.append(body)
                    if parameter == "end":
                        inbound.ended = True
                    elif parameter == "over":
                        inbound.chunks.append(_OVER_LIMIT)
                    elif parameter.startswith("error:"):
                        status, message = parameter[6:].split(":", 1)
                        inbound.error = (int(status), message)
                    waiter = inbound.waiter
                    if waiter is not None and not waiter.done():
                        waiter.set_result(None)
                return
            if route == "application_written":
                stream = self._application_streams.get(parent)
                if stream is not None:
                    stream.unacked -= size
                    waiter = stream.waiter
                    if waiter is not None and stream.unacked <= STREAM_WINDOW and not waiter.done():
                        waiter.set_result(None)
                return
            if route == "application_file":
                stream = self._application_streams.get(parent)
                if stream is not None and stream.file is not None:
                    (future, path), stream.file = stream.file, None
                    if not future.done():
                        if parameter.startswith("error:"):
                            code = int(parameter[6:].split(":", 1)[0])
                            future.set_exception(OSError(code, os.strerror(code), path))
                        else:
                            future.set_result(size)
                return
            if route == "application_lost":
                # The peer left; size is the connection's key. Its state is
                # marked lost (False: created later, as lost) before the
                # cancellation, as Sanic's connection_lost does.
                connections = self.application_connections
                state = connections.get(size)
                if state:
                    state.connection_lost()
                elif size in connections:
                    connections[size] = False
                if parent in self._application_tasks:
                    # Cancelled after the handler's first step, as Sanic's
                    # connection_lost lands on a running handler: a request that
                    # arrived with the peer's FIN (both in one drain) is still
                    # started, and answered with RequestCancelled.
                    self._loop.call_soon(self._lose_application, parent)
                return
            if route == "application_timeout":
                entry = self._application_tasks.get(parent)
                if entry is not None:
                    self._application_timeouts.add(parent)
                    entry[0].cancel()
                    self._reap_unstarted_application(parent)
                return
            if route == "application_done":
                if self._application_tunnels:
                    self._tunnel_lost(parent)
                entry = self._application_tasks.get(parent)
                if entry is not None:
                    self._application_terminal.add(parent)
                    entry[0].cancel()
                    self._reap_unstarted_application(parent)
                return
        if self._closed or handler is None:
            if self._application_inbound:
                self._application_inbound.pop(request_id, None)
            self._queue_application(request_id, shard, 503)
            return
        try:
            if request is None:  # Legacy tuple input remains accepted internally.
                _, _, _, _, _, body, _, _, target, raw_headers = values
                request = ApplicationRequest(method, target, raw_headers, body or b"")
            coroutine = self._serve_application(request_id, shard, request, handler)
            try:
                task = self._loop.create_task(coroutine)
            except BaseException:
                coroutine.close()
                raise
            self._decision_stats["python_application_started"] += 1
            # Eager task factories may finish the coroutine, and its bookkeeping,
            # before create_task returns; only a pending Task is registered.
            if not task.done():
                self._application_tasks[request_id] = (task, shard)
                if task.cancelling():
                    # A task factory cancelled it before its first step: its body
                    # will never run, so answer as soon as the cancellation lands.
                    task.add_done_callback(
                        lambda _, rid=request_id: self._reap_unstarted_application(rid))
        except Exception:
            if self._application_inbound:
                self._application_inbound.pop(request_id, None)
            self._decision_stats["python_application_errors"] += 1
            self._queue_application(request_id, shard, 500)

    async def _serve_http(self, context):
        event = context._event
        code, tag = 500, b""
        self._decision_stats["python_http_started"] += 1
        try:
            async with asyncio.timeout(self.config.server.decision_timeout_ms / 1000):
                answer = await self._http_handler(context)
                if not isinstance(answer, HttpResponse):
                    raise TypeError("http_handler must return HttpResponse")
                if type(answer.status) is not int or not (answer.status == 200 or 400 <= answer.status <= 599):
                    raise ValueError("unsupported HTTP status")
                if answer.status == 200 and (context._body is None or answer.body is not context._body):
                    raise ValueError("success must return this request's awaited native body")
                tag = answer.tag.encode("ascii")
                if len(tag) > 128 or b"\r" in tag or b"\n" in tag:
                    raise ValueError("invalid result tag")
                code = answer.status
        except asyncio.CancelledError:
            code = context._terminal_code
        except TimeoutError:
            code = 504
        except NativeOperationError as error:
            code = error.status
        except Exception:
            self._decision_stats["python_http_errors"] += 1
        finally:
            context._done = True
            context._terminal_code = code
            # Only retire when no native await can still require completion.
            # A handler that timed out around shield(fetch()) must retain its
            # correlation until native_done settles that independently-held Task.
            retire = self.config.retire_http_contexts and (not context._used or (
                context._phase.route == "python_result" and context._future.done()))
            if context._native_finished or retire:
                self._http_waiting.pop(event.id, None)
            phase = context._phase
            self._resolve_http(phase.id, phase.shard, code, tag, retire)
            self._decision_stats["python_http_finished"] += 1

    def _http_event(self, event):
        if event.route == "python_done":
            context = self._http_waiting.pop(event.parent, None)
            if context is not None:
                context._native_finished = True
                status = event.body_size or context._terminal_code
                context._terminal_code = status
                if context._future is not None and not context._future.done():
                    context._future.set_exception(NativeOperationError(status))
                elif not context._done:
                    context._task.cancel()
        elif event.route == "python_result":
            context = self._http_waiting.get(event.parent)
            if context is None or context._done or context._future.done():
                code = context._terminal_code if context is not None else 503
                self._resolve_http(event.id, event.shard, code, b"")
            else:
                context._phase = event
                context._body = NativeBody(event.body_size, int(event.parameter or "200"))
                self._decision_stats["python_http_resumed"] += 1
                context._future.set_result(context._body)
        elif self._closed or self._http_handler is None:
            self._resolve_http(event.id, event.shard, 503, b"")
        else:
            context = HttpRequest(self, event)
            self._http_waiting[event.id] = context
            task = self._loop.create_task(self._serve_http(context))
            context._task = task
            self._http_tasks.add(task)
            task.add_done_callback(self._http_tasks.discard)

    def _drain(self):
        for request_id, payload, pid, tid, shard, error, metrics in self._native.drain(self.config.completion_batch):
            start = time.monotonic_ns()
            fut = self._pending.pop(request_id)
            if not fut.done():
                if error:
                    fut.set_exception(RuntimeError(error))
                else:
                    fut.set_result(Result(payload, pid, tid, shard, metrics))
            metrics["python_delivered_ns"] = time.monotonic_ns()
            metrics["python_callback_ns"] = metrics["python_delivered_ns"] - start
        if self._files:
            self._drain_files()
        if self.config.server:
            decisions = [] if self.config.batch_decisions else None
            for values in self._native.drain_requests_owned(self.config.completion_batch,
                                                            self.application_connections):
                if (len(values) <= 4 or (len(values) == 10 and values[3] == "application")
                        or values[3] in ("application_done", "application_written", "application_data",
                                         "application_timeout", "application_file",
                                         "application_lost")):
                    self._application_event(values)
                    continue
                request = Request(*values)
                if request.route in ("python_compress", "python_fetch", "python_result", "python_done"):
                    self._http_event(request)
                    continue
                start, cpu = time.monotonic_ns(), time.thread_time_ns()
                code = 3
                if not self._closed:
                    try:
                        if self._decision_handler is None:
                            answer = (request.route == "file" and request.method == "GET") or (
                                request.body_size <= self.config.server.max_body_bytes and
                                ((request.route == "fixed" and request.method == "GET") or
                                 (request.route == "compress" and request.method == "POST")))
                        else:
                            answer = self._decision_handler(request)
                        if inspect.isawaitable(answer):
                            if inspect.iscoroutine(answer):
                                answer.close()
                            raise TypeError("this probe requires a synchronous decision callback")
                        if not isinstance(answer, bool):
                            raise TypeError("decision callback must return bool")
                        code = int(answer)
                    except Exception:
                        code = 2
                        self._decision_stats["python_decision_errors"] += 1
                self._decision_stats["python_decisions"] += 1
                self._decision_stats["python_decision_cpu_ns"] += time.thread_time_ns() - cpu
                self._decision_stats["python_decision_wall_ns"] += time.monotonic_ns() - start
                if decisions is None:
                    self._native.resolve_request(request.id, request.shard, code, request.body or b"")
                else:
                    decisions.append((request.id, request.shard, code, request.body or b""))
            if decisions:
                self._native.resolve_requests(decisions)
        if not self._pending:
            self._empty.set()
        status = self._native.stats()
        if status["state"] == 1 and not self._ready.done():
            self._ready.set_result(None)
        if status["state"] == 4 and not self._ready.done():
            self._ready.set_exception(RuntimeError(status["error"]))
        if status["state"] >= 3 and not self._stopped.done():
            self._stopped.set_result(None)

    def stats(self):
        return self._native.stats()

    def shards(self):
        return self._native.shards()

    def workers(self):
        return self._native.workers()

    def server_stats(self):
        stats = (self._native.server_stats() | self._decision_stats |
                 {"python_application_active": len(self._application_tasks)})
        if self._application_native_results:
            stats.update(("native_response_" + key, value)
                         for key, value in self._application_returns.stats().items())
        return stats

    def _cancel_applications(self):
        tasks = tuple(task for task, _ in self._application_tasks.values())
        for task in tasks:
            task.cancel()
        return tasks

    def _reap_applications(self):
        for request_id in tuple(self._application_tasks):
            self._reap_unstarted_application(request_id)

    async def _finish_cancelled(self, tasks):
        """Wait up to _CANCELLED_WAIT for cancelled Tasks. A handler that ignores
        its cancellation is left running; its late answer is dropped."""
        if not tasks:
            return
        await asyncio.wait((asyncio.gather(*tasks, return_exceptions=True),),
                           timeout=_CANCELLED_WAIT)
        stuck = 0
        for request_id, (task, _) in tuple(self._application_tasks.items()):
            if task in tasks and not task.done():
                del self._application_tasks[request_id]
                self._application_terminal.add(request_id)
                stuck += 1
        for task in tasks:
            if not task.done() and task in self._http_tasks:
                self._http_tasks.discard(task)
                stuck += 1
        if stuck:
            logging.getLogger("oldman_server.error").warning(
                "%d request handler(s) kept running after being cancelled; "
                "the server stops without them", stuck)

    async def _close(self):
        await self._finish_cancelled(self._cancel_applications())
        self._reap_applications()
        self._flush_application()
        tasks = tuple(self._http_tasks)
        for task in tasks:
            task.cancel()
        await self._finish_cancelled(tasks)
        self._flush_http()
        await self._empty.wait()
        # After abandon() the runtime (its notification fd, queue and connection
        # registry) may serve another Engine: this one's server already stopped.
        if not self._abandoned:
            if self._persistent:
                # Stop only this Engine's network server; the shared runtime keeps running.
                await _off_loop(self._native.stop_server, self._server_generation)
            else:
                self._native.stop()
                await self._stopped
            while self._native.server_stats().get("queued_decisions", 0):
                self._drain()
                await self._finish_cancelled(self._cancel_applications())
                self._reap_applications()
                self._flush_application()
            self._loop.remove_reader(self.notification_fd)
            self._reader_registered = False
            self._release_connections()
        # The server stopped: every tunnel is gone; file results are no longer drained.
        for request_id in tuple(self._application_tunnels):
            self._tunnel_lost(request_id)
        for future, _, _ in self._files.values():
            if not future.done():
                future.set_exception(RuntimeError("the native host stopped"))
        self._files.clear()
        if not self._persistent:
            await _off_loop(self._native.join)
            self._native.close_fd()

    def abandon(self):
        """Stop a persistent Engine's network server without its event loop, when
        aclose can no longer run (e.g. the loop was interrupted). Python tasks of
        this Engine are not awaited; the shared runtime stays usable."""
        if self._close_task is not None and self._close_task.done():
            return
        self._closed = True
        if self._reader_registered:
            try:
                self._loop.remove_reader(self.notification_fd)
            except Exception:
                pass  # the loop may already be closed
            self._reader_registered = False
        if self._persistent:
            self._native.stop_server(self._server_generation)
            self._abandoned = True
            self._release_connections()

    def _release_connections(self):
        """The server stopped: every connection is gone."""
        for state in tuple(self.application_connections.values()):
            if state:  # None or False: no state was created for it
                state.connection_lost()
        self.application_connections.clear()

    @property
    def closed(self):
        return self._close_task is not None and self._close_task.done()

    async def drain_applications(self, timeout):
        """A graceful stop, as Sanic's within GRACEFUL_SHUTDOWN_TIMEOUT: the
        server accepts no more connections and closes idle ones (the others
        after their response); requests keep being served for up to `timeout`
        seconds, until no request runs and no connection is open (a response
        is still being written). aclose then cancels and cuts off the rest."""
        deadline = self._loop.time() + timeout
        if not self._draining and not self._closed and not self._abandoned:
            self._draining = True
            await _off_loop(self._native.drain_server, self._server_generation)
        while (self._application_tasks or self._native.server_stats().get("connections_open", 0)) \
                and self._loop.time() < deadline:
            await asyncio.sleep(0.01)

    async def aclose(self):
        if self._close_task is None:
            self._closed = True
            self._close_task = self._loop.create_task(self._close())
        await asyncio.shield(self._close_task)
