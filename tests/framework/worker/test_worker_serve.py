import logging

from os import environ
from unittest.mock import Mock, patch

import pytest

from oldman_server.app import OldmanServer
from oldman_server.worker.loader import AppLoader
from oldman_server.worker.multiplexer import WorkerMultiplexer
from oldman_server.worker.process import Worker, WorkerProcess
from oldman_server.worker.serve import worker_serve


@pytest.fixture
def mock_app():
    app = Mock()
    server_info = Mock()
    server_info.settings = {"app": app}
    app.state.workers = 1
    # serve() gives the worker manager its stop timeout from it.
    app.config.GRACEFUL_SHUTDOWN_TIMEOUT = 15.0
    # serve() reads WORKER_HEALTH_TIMEOUT to decide on the heartbeat table.
    app.config.WORKER_HEALTH_TIMEOUT = None
    app.listeners = {"main_process_ready": []}
    app.state.server_info = [server_info]
    return app


def args(app, **kwargs):
    params = {**kwargs}
    params.setdefault("host", "127.0.0.1")
    params.setdefault("port", 9999)
    params.setdefault("app_name", "test_config_app")
    params.setdefault("monitor_publisher", None)
    params.setdefault("app_loader", AppLoader(factory=lambda: app))
    return params


def test_config_app(mock_app: Mock):
    with patch("oldman_server.worker.serve._serve_http_1"):
        worker_serve(**args(mock_app, config={"FOO": "BAR"}))
    mock_app.update_config.assert_called_once_with({"FOO": "BAR"})


def test_bad_process(mock_app: Mock, caplog):
    environ["OLDMAN_SERVER_WORKER_NAME"] = (
        f"{Worker.WORKER_PREFIX}-{WorkerProcess.SERVER_LABEL}-FOO"
    )

    message = "No restart publisher found in worker process"
    with pytest.raises(RuntimeError, match=message):
        worker_serve(**args(mock_app))

    message = "No worker state found in worker process"
    publisher = Mock()
    with caplog.at_level(logging.ERROR):
        worker_serve(**args(mock_app, monitor_publisher=publisher))

    assert ("oldman_server.error", logging.ERROR, message) in caplog.record_tuples
    publisher.send.assert_called_once_with("__TERMINATE_EARLY__")

    del environ["OLDMAN_SERVER_WORKER_NAME"]


def test_has_multiplexer(app: OldmanServer):
    environ["OLDMAN_SERVER_WORKER_NAME"] = (
        f"{Worker.WORKER_PREFIX}-{WorkerProcess.SERVER_LABEL}-FOO"
    )

    OldmanServer.register_app(app)
    with patch("oldman_server.worker.serve._serve_http_1"):
        worker_serve(
            **args(app, monitor_publisher=Mock(), worker_state=Mock())
        )
    assert isinstance(app.multiplexer, WorkerMultiplexer)

    del environ["OLDMAN_SERVER_WORKER_NAME"]


@patch("oldman_server.mixins.startup.WorkerManager")
def test_serve_app_implicit(wm: Mock, app):
    app.prepare()
    OldmanServer.serve()
    wm.call_args[0] == app.state.workers


@patch("oldman_server.mixins.startup.WorkerManager")
def test_serve_app_explicit(wm: Mock, mock_app):
    OldmanServer.serve(mock_app)
    wm.call_args[0] == mock_app.state.workers


@patch("oldman_server.mixins.startup.WorkerManager")
def test_serve_app_loader(wm: Mock, mock_app):
    OldmanServer.serve(app_loader=AppLoader(factory=lambda: mock_app))
    wm.call_args[0] == mock_app.state.workers
    # Sanic.serve(factory=lambda: mock_app)


@patch("oldman_server.mixins.startup.WorkerManager")
def test_serve_app_factory(wm: Mock, mock_app):
    OldmanServer.serve(factory=lambda: mock_app)
    wm.call_args[0] == mock_app.state.workers


