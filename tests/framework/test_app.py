import asyncio
import logging
import re

from collections import Counter
from inspect import isawaitable
from os import environ
from unittest.mock import Mock, patch

import pytest

import oldman_server

from oldman_server import OldmanServer
from oldman_server.config import Config
from oldman_server.exceptions import OldmanServerException
from oldman_server.helpers import Default
from oldman_server.log import LOGGING_CONFIG_DEFAULTS
from oldman_server.native import native_server
from oldman_server.response import text
from oldman_server.router import Route

from .conftest import get_port


@pytest.fixture(autouse=True)
def clear_app_registry():
    OldmanServer._app_registry = {}


def test_app_loop_running(app: OldmanServer):
    @app.get("/test")
    async def handler(request):
        assert isinstance(app.loop, asyncio.AbstractEventLoop)
        return text("pass")

    request, response = app.test_client.get("/test")
    assert response.text == "pass"


def test_create_asyncio_server(loop, app: OldmanServer, port: int):
    asyncio_srv_coro = app.create_server(return_asyncio_server=True, port=port)
    assert isawaitable(asyncio_srv_coro)
    srv = loop.run_until_complete(asyncio_srv_coro)
    assert srv.is_serving() is True


def test_asyncio_server_no_start_serving(loop, app: OldmanServer, port):
    asyncio_srv_coro = app.create_server(
        port=port,
        return_asyncio_server=True,
        asyncio_server_kwargs=dict(start_serving=False),
    )
    srv = loop.run_until_complete(asyncio_srv_coro)
    # The native host serves once created: asyncio_server_kwargs, such as
    # start_serving=False, are for the asyncio server.
    assert srv.is_serving() is native_server(app.config)


def test_asyncio_server_start_serving(loop, app: OldmanServer, port):
    asyncio_srv_coro = app.create_server(
        port=port,
        return_asyncio_server=True,
        asyncio_server_kwargs=dict(start_serving=False),
    )
    srv = loop.run_until_complete(asyncio_srv_coro)
    assert srv.is_serving() is native_server(app.config)  # as above
    loop.run_until_complete(srv.startup())
    loop.run_until_complete(srv.start_serving())
    assert srv.is_serving() is True
    wait_close = srv.close()
    loop.run_until_complete(wait_close)
    # Looks like we can't easily test `serve_forever()`


def test_create_server_main(loop, app: OldmanServer, caplog, port):
    app.listener("main_process_start")(lambda *_: ...)
    with caplog.at_level(logging.INFO):
        asyncio_srv_coro = app.create_server(
            return_asyncio_server=True, port=port
        )
        loop.run_until_complete(asyncio_srv_coro)
    assert (
        "oldman_server.root",
        30,
        "Listener events for the main process are not available with "
        "create_server()",
    ) in caplog.record_tuples


def test_create_server_no_startup(loop, app: OldmanServer, port):
    asyncio_srv_coro = app.create_server(
        port=port,
        return_asyncio_server=True,
        asyncio_server_kwargs=dict(start_serving=False),
    )
    srv = loop.run_until_complete(asyncio_srv_coro)
    message = (
        "Cannot run the server without first running await server.startup()"
    )
    with pytest.raises(OldmanServerException, match=message):
        loop.run_until_complete(srv.start_serving())


def test_create_server_main_convenience(loop, app: OldmanServer, caplog, port):
    app.main_process_start(lambda *_: ...)
    with caplog.at_level(logging.INFO):
        asyncio_srv_coro = app.create_server(
            return_asyncio_server=True, port=port
        )
        loop.run_until_complete(asyncio_srv_coro)
    assert (
        "oldman_server.root",
        30,
        "Listener events for the main process are not available with "
        "create_server()",
    ) in caplog.record_tuples


def test_app_loop_not_running(app: OldmanServer):
    with pytest.raises(OldmanServerException) as excinfo:
        app.loop

    assert str(excinfo.value) == (
        "Loop can only be retrieved after the app has started "
        "running. Not supported with `create_server` function"
    )


def test_app_run_raise_type_error(app: OldmanServer, port):
    with pytest.raises(TypeError) as excinfo:
        app.run(loop="loop", port=port)

    assert str(excinfo.value) == (
        "loop is not a valid argument. To use an existing loop, "
        "change to create_server()."
    )


def test_app_route_raise_value_error(app: OldmanServer):
    with pytest.raises(ValueError) as excinfo:

        @app.route("/test")
        async def handler():
            return text("test")

    assert (
        str(excinfo.value)
        == "Required parameter `request` missing in the handler() route?"
    )


def test_app_handle_request_handler_is_none(app: OldmanServer, monkeypatch):
    app.config.TOUCHUP = False
    route = Mock(spec=Route)
    route.extra.request_middleware = []
    route.extra.response_middleware = []

    def mockreturn(*args, **kwargs):
        return route, None, {}

    monkeypatch.setattr(app.router, "get", mockreturn)

    @app.get("/test")
    def handler(request):
        return text("test")

    _, response = app.test_client.get("/test")

    assert (
        "'None' was returned while requesting a handler from the router"
        in response.text
    )


@pytest.mark.parametrize("websocket_enabled", [True, False])
@pytest.mark.parametrize("enable", [True, False])
def test_app_enable_websocket(app: OldmanServer, websocket_enabled, enable):
    app.websocket_enabled = websocket_enabled
    app.enable_websocket(enable=enable)

    assert app.websocket_enabled == enable

    @app.websocket("/ws")
    async def handler(request, ws):
        await ws.send("test")

    assert app.websocket_enabled is True


def test_app_websocket_parameters(app: OldmanServer):
    app.config.WEBSOCKET_MAX_SIZE = 44
    app.config.WEBSOCKET_PING_TIMEOUT = 48
    app.config.WEBSOCKET_PING_INTERVAL = 50
    seen = {}

    @app.websocket("/ws")
    async def handler(request, ws):
        seen.update(max_size=ws.ws_proto.max_message_size,
                    ping_timeout=ws.ping_timeout, ping_interval=ws.ping_interval)
        await ws.send("test")

    _, response = app.test_client.websocket("/ws")
    assert response.opened is True
    assert seen == {"max_size": 44, "ping_timeout": 48, "ping_interval": 50}


def test_handle_request_with_nested_exception(app: OldmanServer, monkeypatch):
    err_msg = "Mock Exception"

    def mock_error_handler_response(*args, **kwargs):
        raise Exception(err_msg)

    monkeypatch.setattr(
        app.error_handler, "response", mock_error_handler_response
    )

    @app.get("/")
    def handler(request):
        raise Exception

    request, response = app.test_client.get("/")
    assert response.status == 500
    assert response.text == "An error occurred while handling an error"


def test_handle_request_with_nested_exception_debug(app: OldmanServer, monkeypatch):
    err_msg = "Mock Exception"

    def mock_error_handler_response(*args, **kwargs):
        raise Exception(err_msg)

    monkeypatch.setattr(
        app.error_handler, "response", mock_error_handler_response
    )

    @app.get("/")
    def handler(request):
        raise Exception

    request, response = app.test_client.get("/", debug=True)
    assert response.status == 500
    assert response.text.startswith(
        f"Error while handling error: {err_msg}\n"
        "Stack: Traceback (most recent call last):\n"
    )


def test_handle_request_with_nested_sanic_exception(
    app: OldmanServer, monkeypatch, caplog
):
    def mock_error_handler_response(*args, **kwargs):
        raise OldmanServerException("Mock OldmanServerException")

    monkeypatch.setattr(
        app.error_handler, "response", mock_error_handler_response
    )

    @app.get("/")
    def handler(request):
        raise Exception

    with caplog.at_level(logging.ERROR):
        request, response = app.test_client.get("/")
    port = request.server_port
    assert port > 0
    assert response.status == 500
    assert "Mock OldmanServerException" in response.text
    assert (
        "oldman_server.error",
        logging.ERROR,
        f"Exception occurred while handling uri: 'http://127.0.0.1:{port}/'",
    ) in caplog.record_tuples


def test_app_name_required():
    with pytest.raises(TypeError):
        OldmanServer()


def test_app_has_test_mode_sync():
    app = OldmanServer("test")

    @app.get("/")
    def handler(request):
        assert request.app.test_mode
        return text("test")

    _, response = app.test_client.get("/")
    assert response.status == 200


def test_app_registry():
    assert len(OldmanServer._app_registry) == 0
    instance = OldmanServer("test")
    assert len(OldmanServer._app_registry) == 1
    assert OldmanServer._app_registry["test"] is instance
    OldmanServer.unregister_app(instance)
    assert len(OldmanServer._app_registry) == 0


def test_app_registry_wrong_type():
    with pytest.raises(
        OldmanServerException, match="Registered app must be an instance of OldmanServer"
    ):
        OldmanServer.register_app(1)


def test_app_registry_name_reuse():
    OldmanServer("test")
    OldmanServer.test_mode = False
    with pytest.raises(
        OldmanServerException, match='App name "test" already in use.'
    ):
        OldmanServer("test")
    OldmanServer.test_mode = True
    OldmanServer("test")


def test_app_registry_retrieval():
    instance = OldmanServer("test")
    assert OldmanServer.get_app("test") is instance


def test_app_registry_retrieval_from_multiple():
    instance = OldmanServer("test")
    OldmanServer("something_else")
    assert OldmanServer.get_app("test") is instance


def test_get_app_does_not_exist():
    with pytest.raises(
        OldmanServerException,
        match=(
            "App name 'does-not-exist' not found.\n"
            "App instantiation must occur outside "
            "if __name__ == '__main__' "
            "block or by using an AppLoader.\nSee Sanic's documentation "
            r"\(the framework layer is a fork of Sanic\): "
            "https://sanic.dev/en/guide/deployment/app-loader.html"
        ),
    ):
        OldmanServer.get_app("does-not-exist")


def test_get_app_does_not_exist_force_create():
    assert isinstance(
        OldmanServer.get_app("does-not-exist", force_create=True), OldmanServer
    )


def test_get_app_default():
    instance = OldmanServer("test")
    assert OldmanServer.get_app() is instance


def test_get_app_no_default():
    with pytest.raises(
        OldmanServerException, match="No apps have been registered."
    ):
        OldmanServer.get_app()


def test_get_app_default_ambiguous():
    OldmanServer("test1")
    OldmanServer("test2")
    with pytest.raises(
        OldmanServerException,
        match=re.escape(
            'Multiple apps found, use OldmanServer.get_app("app_name")'
        ),
    ):
        OldmanServer.get_app()


def test_app_set_attribute_warning(app: OldmanServer):
    message = (
        "Setting variables on OldmanServer instances is not allowed. You should "
        "change your OldmanServer instance to use instance.ctx.foo instead."
    )
    with pytest.raises(AttributeError, match=message):
        app.foo = 1


def test_app_set_context(app: OldmanServer):
    app.ctx.foo = 1

    retrieved = OldmanServer.get_app(app.name)
    assert retrieved.ctx.foo == 1


def test_subclass_initialisation():
    class CustomServer(OldmanServer):
        pass

    CustomServer("test_subclass_initialisation")


def test_bad_custom_config():
    with pytest.raises(
        OldmanServerException,
        match=(
            "When instantiating OldmanServer with config, you cannot also pass "
            "env_prefix"
        ),
    ):
        OldmanServer("test", config=1, env_prefix=1)


def test_custom_config():
    class CustomConfig(Config): ...

    config = CustomConfig()
    app = OldmanServer("custom", config=config)

    assert app.config == config


def test_custom_context():
    class CustomContext: ...

    ctx = CustomContext()
    app = OldmanServer("custom", ctx=ctx)

    assert app.ctx == ctx


@pytest.mark.parametrize("use", (False, True))
def test_uvloop_config(app: OldmanServer, monkeypatch, use):
    @app.get("/test", name="test")
    def handler(request):
        return text("ok")

    try_use_uvloop = Mock()
    monkeypatch.setattr(oldman_server.mixins.startup, "try_use_uvloop", try_use_uvloop)

    # Default config
    app.test_client.get("/test")
    try_use_uvloop.assert_called_once()

    try_use_uvloop.reset_mock()
    app.config["USE_UVLOOP"] = use
    app.test_client.get("/test")

    if use:
        try_use_uvloop.assert_called_once()
    else:
        try_use_uvloop.assert_not_called()


def test_uvloop_cannot_never_called_with_create_server(loop, caplog, monkeypatch):
    apps = (OldmanServer("default-uvloop"), OldmanServer("no-uvloop"), OldmanServer("yes-uvloop"))

    apps[1].config.USE_UVLOOP = False
    apps[2].config.USE_UVLOOP = True

    try_use_uvloop = Mock()
    monkeypatch.setattr(oldman_server.mixins.startup, "try_use_uvloop", try_use_uvloop)


    with caplog.at_level(logging.WARNING):
        for app in apps:
            srv_coro = app.create_server(
                return_asyncio_server=True,
                asyncio_server_kwargs=dict(start_serving=False),
                port=get_port(),
            )
            loop.run_until_complete(srv_coro)

    try_use_uvloop.assert_not_called()  # Check it didn't try to change policy

    message = (
        "You are trying to change the uvloop configuration, but "
        "this is only effective when using the run(...) method. "
        "When using the create_server(...) method oldman-server will use "
        "the already existing loop."
    )

    counter = Counter([(r[1], r[2]) for r in caplog.record_tuples])
    modified = sum(
        1 for app in apps if not isinstance(app.config.USE_UVLOOP, Default)
    )

    assert counter[(logging.WARNING, message)] == modified


def test_multiple_uvloop_configs_display_warning(loop, caplog):
    OldmanServer._uvloop_setting = None  # Reset the setting (changed in prev tests)

    default_uvloop = OldmanServer("default-uvloop")
    no_uvloop = OldmanServer("no-uvloop")
    yes_uvloop = OldmanServer("yes-uvloop")

    no_uvloop.config.USE_UVLOOP = False
    yes_uvloop.config.USE_UVLOOP = True


    with caplog.at_level(logging.WARNING):
        for app in (default_uvloop, no_uvloop, yes_uvloop):
            srv_coro = app.create_server(
                return_asyncio_server=True,
                asyncio_server_kwargs=dict(start_serving=False),
                port=get_port(),
            )
            srv = loop.run_until_complete(srv_coro)
            loop.run_until_complete(srv.startup())

    message = (
        "It looks like you're running several apps with different "
        "uvloop settings. This is not supported and may lead to "
        "unintended behaviour."
    )

    counter = Counter([(r[1], r[2]) for r in caplog.record_tuples])

    assert counter[(logging.WARNING, message)] == 3


def test_cannot_run_fast_and_workers(app: OldmanServer, port):
    message = "You cannot use both fast=True and workers=X"
    with pytest.raises(RuntimeError, match=message):
        app.run(fast=True, workers=4, port=port)


def test_no_workers(app: OldmanServer, port):
    with pytest.raises(RuntimeError, match="Cannot serve with no workers"):
        app.run(workers=0, port=port)


@pytest.mark.parametrize(
    "extra",
    (
        {"fast": True},
        {"workers": 2},
        {"auto_reload": True},
    ),
)
def test_cannot_run_single_process_and_workers_or_auto_reload(
    app: OldmanServer, extra, port
):
    message = (
        "Single process cannot be run with multiple workers or auto-reload"
    )
    with pytest.raises(RuntimeError, match=message):
        app.run(single_process=True, port=port, **extra)


def test_default_configure_logging():
    with patch("oldman_server.app.logging") as mock:
        OldmanServer("Test")

    mock.config.dictConfig.assert_called_with(LOGGING_CONFIG_DEFAULTS)


def test_custom_configure_logging():
    with patch("oldman_server.app.logging") as mock:
        OldmanServer("Test", log_config={"foo": "bar"})

    mock.config.dictConfig.assert_called_with({"foo": "bar"})


def test_disable_configure_logging():
    with patch("oldman_server.app.logging") as mock:
        OldmanServer("Test", configure_logging=False)

    mock.config.dictConfig.assert_not_called()


def test_build_endpoint_name():
    app = OldmanServer("Test")
    name = app._build_endpoint_name("foo", "bar")
    assert name == "Test.foo.bar"


def test_manager_in_main_process_only(app: OldmanServer):
    message = "Can only access the manager from the main process"

    with pytest.raises(OldmanServerException, match=message):
        app.manager

    app._manager = 1
    environ["OLDMAN_SERVER_WORKER_PROCESS"] = "ok"

    with pytest.raises(OldmanServerException, match=message):
        app.manager

    del environ["OLDMAN_SERVER_WORKER_PROCESS"]

    assert app.manager == 1


def test_stop_trigger_terminate(app: OldmanServer):
    app.multiplexer = Mock()

    app.stop()

    app.multiplexer.terminate.assert_called_once()
    app.multiplexer.reset_mock()
    assert len(OldmanServer._app_registry) == 1
    OldmanServer._app_registry.clear()

    app.stop(terminate=True)

    app.multiplexer.terminate.assert_called_once()
    app.multiplexer.reset_mock()
    assert len(OldmanServer._app_registry) == 0
    OldmanServer._app_registry.clear()

    app.stop(unregister=False)
    app.multiplexer.terminate.assert_called_once()


def test_refresh_pass_passthru_data_to_new_instance(app: OldmanServer):
    # arrange
    passthru = {"_manager": 2, "config": {"TOUCHUP": 23}}
    app = app.refresh(passthru)

    assert app._manager == 2
    assert app.config.TOUCHUP == 23
