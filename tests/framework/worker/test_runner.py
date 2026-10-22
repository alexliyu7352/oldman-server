from unittest.mock import Mock, call, patch

import pytest
from oldman_server.app import OldmanServer
from oldman_server.server.runners import _run_server_forever, serve


@patch("oldman_server.server.runners._serve_http_1")
def test_run_http_1(_serve_http_1: Mock, app: OldmanServer):
    serve("", 0, app)
    _serve_http_1.assert_called_once()


@patch("oldman_server.server.runners.remove_unix_socket")
@pytest.mark.parametrize("do_cleanup", (True, False))
def test_run_server_forever(remove_unix_socket: Mock, do_cleanup: bool):
    loop = Mock()
    cleanup = Mock()
    loop.run_forever = Mock(side_effect=KeyboardInterrupt())
    start = Mock()
    before_stop = Mock()
    before_stop.return_value = Mock()
    after_stop = Mock()
    after_stop.return_value = Mock()
    unix = Mock()

    with pytest.raises(KeyboardInterrupt):
        _run_server_forever(
            loop,
            start,
            before_stop,
            after_stop,
            cleanup if do_cleanup else None,
            unix,
            12345,
        )

    start.assert_called_once_with()
    loop.run_forever.assert_called_once_with()
    # Each stop phase runs as one task.
    loop.create_task.assert_has_calls(
        [call(before_stop.return_value), call(after_stop.return_value)]
    )
    loop.run_until_complete.assert_has_calls(
        [call(loop.create_task.return_value)] * 2
    )

    if do_cleanup:
        cleanup.assert_called_once_with()
    else:
        cleanup.assert_not_called()

    remove_unix_socket.assert_called_once_with(unix)
    loop.close.assert_called_once_with()
