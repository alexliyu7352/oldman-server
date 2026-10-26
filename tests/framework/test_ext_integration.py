import pytest

from oldman_server import OldmanServer
from oldman_server.ext import Extend, Extension


@pytest.fixture
def stoppable_app(app):
    @app.before_server_start
    async def stop(*_):
        app.stop()

    return app


def test_ext_is_not_loaded_without_a_request(stoppable_app: OldmanServer, mock_extend, port):
    # Unlike Sanic with sanic-ext installed, startup does not extend an app
    # that did not ask for the extensions.
    stoppable_app.run(single_process=True, port=port)
    mock_extend.assert_not_called()


def test_ext_is_not_loaded_when_auto_extend_is_off(app: OldmanServer, mock_extend):
    app.config.AUTO_EXTEND = False
    with pytest.raises(RuntimeError, match="AUTO_EXTEND setting is False"):
        app.extend()
    mock_extend.assert_not_called()


def test_extend_with_args(stoppable_app: OldmanServer, mock_extend, port):
    stoppable_app.extend(built_in_extensions=False)
    stoppable_app.run(single_process=True, port=port)
    mock_extend.assert_called_once_with(
        stoppable_app, built_in_extensions=False, config=None, extensions=None
    )


def test_access_object_sets_up_extension(app: OldmanServer, mock_extend):
    app.ext
    mock_extend.assert_called_once_with(app)


def test_extend_cannot_be_called_multiple_times(app: OldmanServer, mock_extend):
    app.extend()

    message = "Cannot extend the app after its extensions have been set up."
    with pytest.raises(RuntimeError, match=message):
        app.extend()
    mock_extend.assert_called_once_with(
        app, extensions=None, built_in_extensions=True, config=None
    )


def test_extend_uses_the_extensions_shipped_with_the_package(app: OldmanServer, monkeypatch):
    # The real Extend (this module imported it before the autouse mock).
    monkeypatch.setattr("oldman_server.ext.Extend", Extend)
    try:
        app.extend(built_in_extensions=False)
        assert isinstance(app.ext, Extend)
    finally:
        Extend.reset()
        Extension.reset()


def test_can_access_app_ext_while_running(
    app: OldmanServer, mock_extend, ext_instance, port
):
    class IceCream:
        flavor: str

    @app.before_server_start
    async def injections(*_):
        app.ext.add_dependency(IceCream)
        app.stop()

    app.run(single_process=True, port=port)
    ext_instance.add_dependency.assert_called_with(IceCream)
