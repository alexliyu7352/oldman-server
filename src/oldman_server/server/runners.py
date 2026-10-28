from __future__ import annotations

from typing import TYPE_CHECKING

from oldman_server.compat import OS_IS_WINDOWS, ctrlc_workaround_for_windows
from oldman_server.config import Config
from oldman_server.constants import DEFAULT_BACKLOG
from oldman_server.exceptions import ServerError


if TYPE_CHECKING:
    from oldman_server.app import OldmanServer

import asyncio
import os
import socket
import sys

from functools import partial
from signal import SIG_IGN, SIGINT, SIGTERM
from signal import signal as signal_func

from oldman_server.log import error_logger, logger, server_logger
from oldman_server.logging.setup import setup_logging
from oldman_server.models.server_types import Signal
from oldman_server.native import native_server
from oldman_server.native.accept import turn as accept_turn
from oldman_server.worker.health import Heartbeat
from oldman_server.native.cpus import worker_group
from oldman_server.native.listen import worker_address
from oldman_server.server.async_server import AsyncioServer
from oldman_server.server.protocols.http_protocol import HttpProtocol
from oldman_server.server.socket import (
    bind_socket,
    bind_unix_socket,
    remove_unix_socket,
    unix_socket_mode,
    worker_listener,
)


def serve(
    host,
    port,
    app: OldmanServer,
    sock: socket.socket | None = None,
    unix: str | None = None,
    reuse_port: bool = False,
    loop=None,
    protocol: type[asyncio.Protocol] = HttpProtocol,
    backlog: int = DEFAULT_BACKLOG,
    register_sys_signals: bool = True,
    run_multiple: bool = False,
    run_async: bool = False,
    connections=None,
    signal=Signal(),
    state=None,
    asyncio_server_kwargs=None,
):
    """Start asynchronous HTTP Server on an individual process.

    :param host: Address to host on
    :param port: Port to host on
    :param before_start: function to be executed before the server starts
                         listening. Takes arguments `app` instance and `loop`
    :param after_start: function to be executed after the server starts
                        listening. Takes  arguments `app` instance and `loop`
    :param before_stop: function to be executed when a stop signal is
                        received before it is respected. Takes arguments
                        `app` instance and `loop`
    :param after_stop: function to be executed when a stop signal is
                       received after it is respected. Takes arguments
                       `app` instance and `loop`
    :param sock: Socket for the server to accept connections from
    :param unix: Unix socket to listen on instead of TCP port
    :param reuse_port: `True` for multiple workers
    :param loop: asyncio compatible event loop
    :param run_async: bool: Do not create a new event loop for the server,
                      and return an AsyncServer object rather than running it
    :param asyncio_server_kwargs: key-value args for asyncio/uvloop
                                  create_server method
    :return: Nothing

    Args:
        host (str): Address to host on
        port (int): Port to host on
        app (OldmanServer): Sanic app instance
        sock (Optional[socket.socket], optional): Socket for the server to
            accept connections from. Defaults to `None`.
        unix (Optional[str], optional): Unix socket to listen on instead of
            TCP port. Defaults to `None`.
        reuse_port (bool, optional): `True` for multiple workers. Defaults
            to `False`.
        loop: asyncio compatible event loop. Defaults
            to `None`.
        protocol (Type[asyncio.Protocol], optional): Protocol to use. Defaults
            to `HttpProtocol`.
        backlog (int, optional): The maximum number of queued connections
            passed to socket.listen(). Defaults to `100`.
        register_sys_signals (bool, optional): Register SIGINT and SIGTERM.
            Defaults to `True`.
        run_multiple (bool, optional): Run multiple workers. Defaults
            to `False`.
        run_async (bool, optional): Return an AsyncServer object.
            Defaults to `False`.
        connections: Connections. Defaults to `None`.
        signal (Signal, optional): Signal. Defaults to `Signal()`.
        state: State. Defaults to `None`.
        asyncio_server_kwargs (Optional[Dict[str, Union[int, float]]], optional):
            key-value args for asyncio/uvloop create_server method. Defaults
            to `None`.

    Returns:
        AsyncioServer: AsyncioServer object if `run_async` is `True`.
    """  # noqa: E501
    if not run_async and not loop:
        # create new event_loop after fork
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    setup_logging(app.debug, app.config.NO_COLOR, app.config.LOG_EXTRA)

    if app.debug:
        loop.set_debug(app.debug)

    return _serve_http_1(
        host,
        port,
        app,
        sock,
        unix,
        reuse_port,
        loop,
        protocol,
        backlog,
        register_sys_signals,
        run_multiple,
        run_async,
        connections,
        signal,
        state,
        asyncio_server_kwargs,
    )


class _Stop:
    """SIGINT and SIGTERM in a serving process: app.stop() once the server
    serves, so the stop phases run.

    A Python-level handler, not the loop's add_signal_handler: uvloop drops a
    signal that arrives while it is not running (between the startup steps'
    run_until_complete calls and run_forever), and the process then never
    stopped.

    A signal during startup cancels the startup rather than waiting for it:
    a start that hangs (a database connect, say) would otherwise hold the
    stop until something killed the process, with no cleanup at all. The
    startup listener running (before_server_start, after_server_start) is
    cancelled at its next await and the steps after it do not run; the stop
    listeners paired with what did run still run (_run_startup_step and its
    callers). The internal steps between the listeners (the app's startup,
    the server start) are short and are not cancelled: the stop takes effect
    when they end. Code that does not await cannot be cancelled and ends
    first.

    A later signal stops at once only in a process of its own (`escalates`):
    during a startup listener, by raising KeyboardInterrupt there, which ends
    even code that does not await. One user action reaches a worker the main
    process manages twice: Ctrl+C sends SIGINT to the whole process group, a
    systemd stop SIGTERM to every process of the unit, and the main process
    then stops each worker too. There the main process escalates (its own
    second signal, or its stop timeout, kills the workers), and a worker
    stops once.
    """

    def __init__(self, app: OldmanServer, loop: asyncio.AbstractEventLoop,
                 escalates: bool = True):
        self.app, self.loop = app, loop
        self.escalates = escalates
        # requested: a stop, from a signal or app.stop(); signalled: from a
        # signal, which cancels the startup.
        self.requested = self.serving = self.signalled = False
        # The startup listener step running (a Task), which a stop cancels;
        # cancelled: a stop did; forced: a second signal ended it at once.
        self.startup: asyncio.Task | None = None
        self.cancelled = self.forced = False

    def __call__(self, signum=None, frame=None) -> None:
        if self.signalled and not self.escalates:
            return
        first = not self.signalled
        self.requested = self.signalled = True
        if self.serving:
            self.loop.call_soon_threadsafe(
                partial(self.app.stop, terminate=False)
            )
        elif first:
            self.loop.call_soon_threadsafe(self._cancel_startup)
        elif self.startup is not None:
            self.forced = True
            raise KeyboardInterrupt

    def _cancel_startup(self) -> None:
        if self.startup is not None and not self.startup.done():
            self.cancelled = True
            self.startup.cancel()


def _setup_system_signals(
    app: OldmanServer,
    run_multiple: bool,
    register_sys_signals: bool,
    loop: asyncio.AbstractEventLoop,
) -> _Stop:  # no cov
    stop = _Stop(app, loop, escalates=not run_multiple)
    os.environ["OLDMAN_SERVER_WORKER_PROCESS"] = "true"
    # Register signals for graceful termination
    if OS_IS_WINDOWS:
        # Its selector loop does not wake for a signal: frequent wakeups.
        if register_sys_signals:
            ctrlc_workaround_for_windows(app)
        return stop
    for _signal in [SIGINT, SIGTERM]:
        signal_func(_signal, stop if register_sys_signals else SIG_IGN)
    if register_sys_signals and run_multiple:
        _stop_with_parent(stop)
    return stop


# prctl(2) option: the signal this process gets when its parent dies.
PR_SET_PDEATHSIG = 1


def _stop_with_parent(stop: _Stop) -> None:
    """A worker process stops (SIGTERM, handled as app.stop()) when the main
    process that started it dies, even by SIGKILL; otherwise it would keep
    serving, and holding its socket, with nothing managing it. Linux only."""
    import ctypes
    import multiprocessing

    parent = multiprocessing.parent_process()
    if parent is None or not sys.platform.startswith("linux"):
        return
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(PR_SET_PDEATHSIG, SIGTERM) != 0:
        return
    if os.getppid() != parent.pid:
        # The parent died before the request took effect.
        stop(SIGTERM)


def _run_shutdown_coro(loop, coro):
    """Run a lifecycle coroutine to its end, on a loop that may have a stop
    pending.

    loop.stop() called while the loop is not running (a second app.stop(),
    a signal during select) ends the next run after one iteration, before
    the coroutine is done; uvloop reports that as a RuntimeError. The same
    task then runs on: the coroutine is never started a second time, so its
    own errors, RuntimeError included, propagate.
    """
    # Clear asyncio's stopped state if accessible
    if hasattr(loop, "_stopping"):
        loop._stopping = False
    task = loop.create_task(coro())
    for _ in range(3):
        try:
            loop.run_until_complete(task)
            return
        except (RuntimeError, KeyboardInterrupt):
            # KeyboardInterrupt: signal arrived during select (asyncio).
            if task.done():
                raise
            if hasattr(loop, "_stopping"):
                loop._stopping = False
    # The loop is unusable: leave the task.


def _run_starting(loop, coro, stop):
    """Run a startup step to its end. app.stop() meanwhile (from a listener;
    the loop stops after its current iteration) does not abort the step,
    which would look like a failed start: the server stops once started."""
    task = loop.create_task(coro)
    while True:
        try:
            return loop.run_until_complete(task)
        except RuntimeError:
            if task.done():
                raise
            stop.requested = True


def _run_startup_step(loop, coro, stop) -> bool:
    """Run a startup listener step (before_server_start, after_server_start)
    as the task a stop signal cancels (_Stop). True if it ran to its end,
    False if a stop signal ended it. app.stop() from a listener (the loop
    stops after its current iteration) does not end the step: the server
    stops once started, as in _run_starting."""
    task = loop.create_task(coro)
    if stop is None:
        loop.run_until_complete(task)
        return True
    stop.startup = task
    try:
        while True:
            try:
                loop.run_until_complete(task)
                return True
            except RuntimeError:
                if task.done():
                    raise
                stop.requested = True
    except asyncio.CancelledError:
        if stop.cancelled and task.cancelled():
            return False
        raise
    except KeyboardInterrupt:
        # A second signal in a process of its own, raised by _Stop.
        if not stop.forced:
            raise
        if not task.done():
            task.cancel()
        return False
    finally:
        stop.startup = None


def _startup_interrupted(stop) -> None:
    """A stop signal ended the start: a stop, not a failed start (a worker
    exits as a stopped one, which the main process does not count as a
    failure to start)."""
    stop.app.state.is_stopping = True
    logger.info("Startup interrupted by a stop signal")


def _release_unstarted(loop, app):
    """after_server_stop for a server that did not start (its start failed, or
    a stop signal ended it): it releases what before_server_start set up. Its
    own failure is logged; a start failure is the one that propagates."""
    try:
        _run_shutdown_coro(
            loop, partial(app._server_event, "shutdown", "after")
        )
    except Exception:
        error_logger.exception(
            "Exception in after_server_stop of a server that did not start"
        )


def _run_before_start(loop, app, stop) -> bool:
    """before_server_start. True when the start goes on; False when a stop
    signal ended it (after_server_stop has run if before_server_start began).
    When it fails, after_server_stop still runs and the failure propagates."""
    if stop.signalled:
        _startup_interrupted(stop)
        return False
    try:
        completed = _run_startup_step(
            loop, app._server_event("init", "before"), stop
        )
    except BaseException:
        _release_unstarted(loop, app)
        raise
    if completed and not stop.signalled:
        return True
    _startup_interrupted(stop)
    _release_unstarted(loop, app)
    return False


async def _after_start(app):
    await app._server_event("init", "after")
    app.set_serving(True)


def _run_server_forever(loop, start, before_stop, after_stop, cleanup, unix, pid,
                        stop=None, heartbeat=None):
    """Start (after_server_start: `start()` gives its coroutine), serve until
    stopped, then stop: before_server_stop, the server's cleanup,
    after_server_stop.

    A failing start stops the server the same way. A stop signal received
    while starting (`stop`, from _setup_system_signals) cancels
    after_server_start at its next await, or skips it if it has not begun;
    before_server_stop runs only if after_server_start began. Every stop
    step runs even if an earlier one failed; the first failure then
    propagates.
    """
    errors = []
    began = True
    if stop is not None:
        stop.app.state.is_stopping = False
    try:
        if stop is not None and stop.signalled:
            began = False
            _startup_interrupted(stop)
        elif not _run_startup_step(loop, start(), stop) or (
            stop is not None and stop.signalled
        ):
            _startup_interrupted(stop)
        else:
            server_logger.info("Worker ready [%s]", pid)
            if stop is not None:
                stop.serving = True
            if stop is None or not stop.requested:
                loop.run_forever()
    except BaseException as error:
        errors.append(error)
    server_logger.info("Stopping worker [%s]", pid)

    if stop is not None:
        # The stop phases run once; further signals are ignored meanwhile.
        for _signal in [SIGINT, SIGTERM]:
            signal_func(_signal, SIG_IGN)
        if stop.serving:
            # What fails from here is the stop of a server that ran, not a
            # failed start (worker_serve tells the manager accordingly).
            stop.app.state.is_stopping = True

    for step in (
        partial(_run_shutdown_coro, loop, before_stop) if began else None,
        cleanup,
        partial(_run_shutdown_coro, loop, after_stop),
    ):
        try:
            if step:
                step()
        except BaseException as error:
            errors.append(error)

    if heartbeat is not None:
        # Through the stop as well: a worker draining its requests is alive.
        heartbeat.stop()
    remove_unix_socket(unix)
    loop.close()
    server_logger.info("Worker complete [%s]", pid)
    if errors:
        raise errors[0]


def _native_unavailable(loop) -> str | None:
    """Why this process's native host cannot serve another server, or None."""
    host = sys.modules.get("oldman_server.native.host")
    if host is not None and host.serving(loop):
        # A secondary app's server (Sanic._start_servers), say.
        return "NATIVE_SERVER serves one server per process"
    engine = sys.modules.get("oldman_server.native.engine")
    if engine is not None and not engine.startable():
        return (
            "NATIVE_SERVER cannot start in a process forked after it started "
            "(use the spawn start method)"
        )
    return None


def _serve_native(
    host, port, app, sock, unix, bound_unix, backlog, loop,
    register_sys_signals, run_multiple, run_async,
):
    """HTTP/1.1 through the native host when NATIVE_SERVER is set.

    Same lifecycle as _serve_http_1: startup, init and shutdown events, system
    signals and app.stop(); only the listener and connection handling differ.
    With run_async, an AsyncioServer whose server is the native one.
    bound_unix: the unix socket path this process bound, removed at exit (a
    socket the main process passed in stays its own).
    """
    try:
        from oldman_server.native.host import (
            create_server,
            native_config,
            open_engine,
        )
    except ImportError as error:
        raise ServerError(
            "NATIVE_SERVER requires the native host extension "
            "(oldman_server.native._native), which this installation does not "
            "have: the release wheels carry it for Linux x86_64 and aarch64 "
            "(glibc 2.28 or later); from a source tree, build it with "
            "scripts/build.sh"
        ) from error
    # The native host accepts on Sanic's socket (TCP or unix), which stays
    # open while serving and closes with the server, as asyncio's does; like
    # asyncio's create_server, it is put in the listening state first. Its
    # shard 0 hands the connections to the other shards.
    if sock is None:
        # create_server() without a socket: bound here as Sanic's bind_socket
        # binds (a host name resolves), then served like a socket passed in.
        sock = bind_socket(host or "127.0.0.1", port or 0, backlog=backlog)
    sockets = [sock]
    accept = None
    sock.listen(backlog)
    if run_multiple:
        # Worker processes share this socket: they take turns accepting,
        # and none may shut it down for the others when it stops.
        accept = {"listen_shared": True, **accept_turn(app.state.native_accept_counts)}
    # NATIVE_WORKER_LISTEN: this process's own address as well, so that a
    # proxy balances requests over the worker processes.
    own = worker_address(app.config)
    own_sock = None
    if own is not None:
        own_sock = (bind_unix_socket(own[1], mode=unix_socket_mode(app.config),
                                     backlog=backlog) if own[0] == "unix"
                    else worker_listener(own[1], own[2], backlog))
        sockets.append(own_sock)
    own_unix = own[1] if own is not None and own[0] == "unix" else None
    config = native_config(
        app, host or "127.0.0.1", port or 8000, listen_sock=sock, unix=unix,
        accept=accept, worker_sock=own_sock, worker_unix=own_unix,
    )

    def close_sockets():
        for open_sock in sockets:
            open_sock.close()
        # Unless a restarted worker has bound the path since.
        remove_unix_socket(own_unix)

    if run_async:
        return AsyncioServer(
            app=app,
            loop=loop,
            serve_coro=create_server(app, config, sockets),
            connections=set(),
        )
    pid = os.getpid()
    server_logger.info("Starting native worker [%s]", pid)
    # Before the startup steps: a stop signal from then on cancels the start
    # (_Stop), and after_server_stop runs whenever before_server_start did
    # (without it, SIGINT interrupted a step anywhere and SIGTERM killed the
    # process).
    stop = _setup_system_signals(app, run_multiple, register_sys_signals, loop)
    try:
        loop.run_until_complete(app._startup())
        starting = _run_before_start(loop, app, stop)
    except BaseException:
        close_sockets()
        raise
    if not starting:
        close_sockets()
        remove_unix_socket(bound_unix)
        loop.close()
        server_logger.info("Worker complete [%s]", pid)
        return
    app.ack()
    heartbeat = Heartbeat.start(loop, app)
    # A worker process: Python runs on its group's first CPU, apart from its
    # shards and the other workers.
    group = worker_group(app.config)
    try:
        engine = _run_starting(
            loop, open_engine(app, config, python_cpu=group[0] if group else None), stop
        )
    except BaseException:
        close_sockets()
        error_logger.exception("Unable to start server", exc_info=True)
        _release_unstarted(loop, app)
        if heartbeat is not None:
            heartbeat.stop()
        raise  # a failed start: app.run() raises, a worker reports it

    def _cleanup():
        graceful = app.config.GRACEFUL_SHUTDOWN_TIMEOUT
        try:
            _run_shutdown_coro(
                loop, partial(engine.drain_applications, graceful)
            )
            _run_shutdown_coro(loop, engine.aclose)
        finally:
            if not engine.closed:
                # The loop could not complete the close: stop the server anyway.
                engine.abandon()
            close_sockets()
        app.shutdown_tasks(graceful)
        try:
            app.set_serving(False)
        except (BrokenPipeError, ConnectionResetError, EOFError):
            pass

    _run_server_forever(
        loop,
        partial(_after_start, app),
        partial(app._server_event, "shutdown", "before"),
        partial(app._server_event, "shutdown", "after"),
        _cleanup,
        bound_unix,
        pid,
        stop,
        heartbeat,
    )


def _serve_http_1(
    host,
    port,
    app,
    sock,
    unix,
    reuse_port,
    loop,
    protocol,
    backlog,
    register_sys_signals,
    run_multiple,
    run_async,
    connections,
    signal,
    state,
    asyncio_server_kwargs,
):
    connections = connections if connections is not None else set()
    # Each worker process of Sanic's worker manager (run_multiple) serves
    # through its own native host, on the CPUs the main process gave it.
    if native_server(app.config):
        if unavailable := _native_unavailable(loop):
            server_logger.warning(
                f"{unavailable}; this one uses the asyncio server"
            )
        else:
            bound_unix = None
            if unix and sock is None:
                # Bound as for the asyncio server (temporary name, mode,
                # rename); the native host accepts on it. Worker processes
                # get the main process's socket instead (configure_socket).
                sock = bind_unix_socket(
                    unix, mode=unix_socket_mode(app.config), backlog=backlog
                )
                bound_unix = unix
            return _serve_native(
                host, port, app, sock, unix, bound_unix, backlog, loop,
                register_sys_signals, run_multiple, run_async,
            )
    protocol_kwargs = _build_protocol_kwargs(protocol, app.config)
    server = partial(
        protocol,
        loop=loop,
        connections=connections,
        signal=signal,
        app=app,
        state=state,
        unix=unix,
        **protocol_kwargs,
    )
    asyncio_server_kwargs = (
        asyncio_server_kwargs if asyncio_server_kwargs else {}
    )
    if OS_IS_WINDOWS and sock:
        # A socket the main process passed: Windows shares it by handle.
        sock = socket.fromshare(sock.share(os.getpid()))
    # UNIX sockets are always bound by us (to preserve semantics between modes)
    if unix:
        sock = bind_unix_socket(
            unix, mode=unix_socket_mode(app.config), backlog=backlog
        )
    server_coroutine = loop.create_server(
        server,
        None if sock else host,
        None if sock else port,
        reuse_port=reuse_port,
        sock=sock,
        backlog=backlog,
        **asyncio_server_kwargs,
    )

    if run_async:
        return AsyncioServer(
            app=app,
            loop=loop,
            serve_coro=server_coroutine,
            connections=connections,
        )

    pid = os.getpid()
    server_logger.info("Starting worker [%s]", pid)
    # Before the startup steps, as in _serve_native.
    stop = _setup_system_signals(app, run_multiple, register_sys_signals, loop)
    loop.run_until_complete(app._startup())
    if not _run_before_start(loop, app, stop):
        server_coroutine.close()
        if sock is not None:
            sock.close()
        remove_unix_socket(unix)
        loop.close()
        server_logger.info("Worker complete [%s]", pid)
        return
    app.ack()
    heartbeat = Heartbeat.start(loop, app)

    try:
        http_server = _run_starting(loop, server_coroutine, stop)
    except BaseException:
        error_logger.exception("Unable to start server", exc_info=True)
        _release_unstarted(loop, app)
        if heartbeat is not None:
            heartbeat.stop()
        raise  # a failed start: app.run() raises, a worker reports it

    def _cleanup():
        # Stop accepting, then let the connections drain
        http_server.close()

        # Complete all tasks on the loop
        signal.stopped = True
        for connection in connections:
            connection.close_if_idle()

        # Gracefully shutdown timeout.
        # We should provide graceful_shutdown_timeout,
        # instead of letting connection hangs forever.
        # Let's roughly calcucate time.
        graceful = app.config.GRACEFUL_SHUTDOWN_TIMEOUT
        start_shutdown: float = 0
        while connections and (start_shutdown < graceful):
            loop.run_until_complete(asyncio.sleep(0.1))
            start_shutdown = start_shutdown + 0.1

        app.shutdown_tasks(graceful - start_shutdown)

        # Force close non-idle connection after waiting for
        # graceful_shutdown_timeout
        for conn in connections:
            if hasattr(conn, "websocket") and conn.websocket:
                conn.websocket.fail_connection(code=1001)
            else:
                conn.abort()
        # asyncio's own loop (Windows; USE_UVLOOP=False) waits here, since
        # Python 3.12, for every connection to close too, idle keep-alive ones
        # included (uvloop does not): so only once they are closed.
        loop.run_until_complete(http_server.wait_closed())

        try:
            app.set_serving(False)
        except (BrokenPipeError, ConnectionResetError, EOFError):
            pass

    _run_server_forever(
        loop,
        partial(_after_start, app),
        partial(app._server_event, "shutdown", "before"),
        partial(app._server_event, "shutdown", "after"),
        _cleanup,
        unix,
        pid,
        stop,
        heartbeat,
    )


def _build_protocol_kwargs(
    protocol: type[asyncio.Protocol], config: Config
) -> dict[str, int | float]:
    if hasattr(protocol, "websocket_handshake"):
        return {
            "websocket_max_size": config.WEBSOCKET_MAX_SIZE,
            "websocket_ping_timeout": config.WEBSOCKET_PING_TIMEOUT,
            "websocket_ping_interval": config.WEBSOCKET_PING_INTERVAL,
        }
    return {}
