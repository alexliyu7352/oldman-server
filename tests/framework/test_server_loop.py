import logging

from unittest.mock import Mock, patch

import pytest

from oldman_server.compat import OS_IS_WINDOWS, UVLOOP_INSTALLED
from oldman_server.server import loop


@pytest.mark.skipif(
    not OS_IS_WINDOWS,
    reason="Not testable with current client",
)
def test_raises_warning_if_os_is_windows(caplog):
    with caplog.at_level(logging.WARNING):
        loop.try_use_uvloop()

    for record in caplog.records:
        if record.message.startswith("You are trying to use"):
            break

    assert record.message == (
        "You are trying to use uvloop, but uvloop is not compatible "
        "with your system. You can disable uvloop completely by setting "
        "the 'USE_UVLOOP' configuration value to false, or simply not "
        "defining it and letting oldman-server handle it for you. "
        "oldman-server will now continue to run using the default event loop."
    )


@pytest.mark.skipif(
    OS_IS_WINDOWS or UVLOOP_INSTALLED,
    reason="Not testable with current client",
)
def test_raises_warning_if_uvloop_not_installed(caplog):
    with caplog.at_level(logging.WARNING):
        loop.try_use_uvloop()

    for record in caplog.records:
        if record.message.startswith("You are trying to use"):
            break

    assert record.message == (
        "You are trying to use uvloop, but uvloop is not "
        "installed in your system. In order to use uvloop "
        "you must first install it. Otherwise, you can disable "
        "uvloop completely by setting the 'USE_UVLOOP' "
        "configuration value to false. oldman-server will now continue "
        "to run with the default event loop."
    )


@pytest.mark.skipif(
    OS_IS_WINDOWS or not UVLOOP_INSTALLED,
    reason="Not testable with current client",
)
def test_logs_when_install_and_runtime_config_mismatch(caplog, monkeypatch):
    getenv = Mock(return_value="no")
    monkeypatch.setattr(loop, "getenv", getenv)

    with caplog.at_level(logging.INFO):
        loop.try_use_uvloop()

    getenv.assert_called_once_with("OLDMAN_SERVER_NO_UVLOOP", "no")
    assert caplog.record_tuples == []

    getenv = Mock(return_value="yes")
    monkeypatch.setattr(loop, "getenv", getenv)
    with caplog.at_level(logging.INFO):
        loop.try_use_uvloop()

    getenv.assert_called_once_with("OLDMAN_SERVER_NO_UVLOOP", "no")
    for record in caplog.records:
        if record.message.startswith("The 'OLDMAN_SERVER_NO_UVLOOP'"):
            break

    # The package always installs uvloop (no install-time opt-out).
    assert record.message == (
        "The 'OLDMAN_SERVER_NO_UVLOOP' environment variable is set, but "
        "oldman-server always installs uvloop and uses it. To keep the "
        "default event loop policy, set the 'USE_UVLOOP' configuration "
        "value to false."
    )


@pytest.mark.skipif(
    OS_IS_WINDOWS or not UVLOOP_INSTALLED,
    reason="Not testable with current client",
)
def test_sets_loop_policy_only_when_not_already_set(monkeypatch):
    import uvloop  # type: ignore

    # Existing policy is not uvloop.EventLoopPolicy
    get_event_loop_policy = Mock(return_value=None)
    monkeypatch.setattr(
        loop.asyncio, "get_event_loop_policy", get_event_loop_policy
    )

    with patch("asyncio.set_event_loop_policy") as set_event_loop_policy:
        loop.try_use_uvloop()
        set_event_loop_policy.assert_called_once()
        args, _ = set_event_loop_policy.call_args
        policy = args[0]
        assert isinstance(policy, uvloop.EventLoopPolicy)

    # Existing policy is uvloop.EventLoopPolicy
    get_event_loop_policy = Mock(return_value=policy)
    monkeypatch.setattr(
        loop.asyncio, "get_event_loop_policy", get_event_loop_policy
    )

    with patch("asyncio.set_event_loop_policy") as set_event_loop_policy:
        loop.try_use_uvloop()
        set_event_loop_policy.assert_not_called()
