import logging
import os

from pathlib import Path
from unittest.mock import Mock

import pytest

from oldman_server import OldmanServer
from oldman_server.application.state import ApplicationServerInfo
from oldman_server.native import native_server
from oldman_server.native.cpus import fast_workers


@pytest.fixture(autouse=True)
def no_skip():
    should_auto_reload = OldmanServer.should_auto_reload
    OldmanServer.should_auto_reload = Mock(return_value=False)
    yield
    OldmanServer._app_registry = {}
    OldmanServer.should_auto_reload = should_auto_reload
    try:
        del os.environ["OLDMAN_SERVER_MOTD_OUTPUT"]
    except KeyError:
        ...


def get_primary(app: OldmanServer) -> ApplicationServerInfo:
    return app.state.server_info[0]


def test_dev(app: OldmanServer):
    app.prepare(dev=True)

    assert app.state.is_debug
    assert app.state.auto_reload


def test_motd_display(app: OldmanServer):
    app.prepare(motd_display={"foo": "bar"})

    assert app.config.MOTD_DISPLAY["foo"] == "bar"
    del app.config.MOTD_DISPLAY["foo"]


@pytest.mark.parametrize("dirs", ("./foo", ("./foo", "./bar")))
def test_reload_dir(app: OldmanServer, dirs, caplog):
    messages = []
    with caplog.at_level(logging.WARNING):
        app.prepare(reload_dir=dirs)

    if isinstance(dirs, str):
        dirs = (dirs,)
        for d in dirs:
            assert Path(d) in app.state.reload_dirs
            messages.append(
                f"Directory {d} could not be located",
            )

    for message in messages:
        assert ("oldman_server.root", logging.WARNING, message) in caplog.record_tuples


def test_fast(app: OldmanServer, caplog):
    @app.after_server_start
    async def stop(app, _):
        app.stop()

    try:
        workers = len(os.sched_getaffinity(0))
    except AttributeError:
        workers = os.cpu_count() or 1
    if native_server(app.config):
        # A native worker runs Python and its shards on 1 + NATIVE_SHARDS CPUs.
        workers = fast_workers(app.config)

    with caplog.at_level(logging.INFO):
        app.prepare(fast=True)

    assert app.state.fast
    assert app.state.workers == workers

    messages = [m[2] for m in caplog.record_tuples]

    if workers == 1:
        worker_fragment = "single worker"
    else:
        worker_fragment = f"w/ {workers} workers"

    assert f"mode: production, goin' fast {worker_fragment}" in messages
