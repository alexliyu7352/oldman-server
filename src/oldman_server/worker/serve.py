from __future__ import annotations

import asyncio
import os
import socket
import warnings

from functools import partial
from multiprocessing.connection import Connection
from typing import Any

from oldman_server.application.constants import ServerStage
from oldman_server.application.state import ApplicationServerInfo
from oldman_server.constants import DEFAULT_BACKLOG
from oldman_server.log import error_logger
from oldman_server.logging.setup import setup_logging
from oldman_server.models.server_types import Signal
from oldman_server.server.protocols.http_protocol import HttpProtocol
from oldman_server.server.runners import _serve_http_1
from oldman_server.worker.loader import AppLoader
from oldman_server.worker.constants import ProcessState
from oldman_server.worker.multiplexer import WorkerMultiplexer
from oldman_server.worker.process import Worker, WorkerProcess


def worker_serve(
    host,
    port,
    app_name: str,
    monitor_publisher: Connection | None,
    app_loader: AppLoader,
    worker_state: dict[str, Any] | None = None,
    server_info: dict[str, list[ApplicationServerInfo]] | None = None,
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
    config: bytes | str | dict[str, Any] | Any | None = None,
    passthru: dict[str, Any] | None = None,
):
    app = None
    try:
        from oldman_server import OldmanServer

        if app_loader:
            app = app_loader.load()
        else:
            app = OldmanServer.get_app(app_name)

        app.refresh(passthru)
        app.setup_loop()
        setup_logging(
            app.state.is_debug, app.config.NO_COLOR, app.config.LOG_EXTRA
        )

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # Hydrate server info if needed
        if server_info:
            for app_name, server_info_objects in server_info.items():
                a = OldmanServer.get_app(app_name)
                if not a.state.server_info:
                    a.state.server_info = []
                    for info in server_info_objects:
                        if not info.settings.get("app"):
                            info.settings["app"] = a
                        a.state.server_info.append(info)

        # When in a worker process, do some init
        worker_name = os.environ.get("OLDMAN_SERVER_WORKER_NAME")
        if worker_name and worker_name.startswith(
            "-".join([Worker.WORKER_PREFIX, WorkerProcess.SERVER_LABEL])
        ):
            # Hydrate apps with any passed server info

            if monitor_publisher is None:
                raise RuntimeError(
                    "No restart publisher found in worker process"
                )
            if worker_state is None:
                raise RuntimeError("No worker state found in worker process")

            # Run secondary servers
            apps = list(OldmanServer._app_registry.values())
            app.before_server_start(partial(app._start_servers, apps=apps))
            for a in apps:
                a.multiplexer = WorkerMultiplexer(
                    monitor_publisher, worker_state
                )

        if app.debug:
            loop.set_debug(app.debug)

        if app.state.server_info:
            primary_server_info = app.state.server_info[0]
            primary_server_info.stage = ServerStage.SERVING
        if config:
            app.update_config(config)

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
    except Exception as e:
        warnings.simplefilter("ignore", category=RuntimeWarning)
        if monitor_publisher:
            error_logger.exception(e)
            if app is not None and app.state.is_stopping is True:
                # The stop of a server that served (a restart, a scale down,
                # the shutdown): logged, it ends no other worker.
                return
            multiplexer = WorkerMultiplexer(monitor_publisher, {})
            name = os.environ.get("OLDMAN_SERVER_WORKER_NAME", "")
            state = (worker_state or {}).get(name, {}).get("state")
            if state == ProcessState.ACKED.name:
                multiplexer.startup_failed()
            else:
                multiplexer.terminate(True)
        else:
            raise e
