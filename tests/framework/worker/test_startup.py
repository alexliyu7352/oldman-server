import sys

from multiprocessing import set_start_method
from unittest.mock import patch

import pytest

from oldman_server import OldmanServer


@pytest.mark.parametrize(
    "start_method,platform,expected",
    (
        (None, "linux", "spawn"),
        (None, "other", "spawn"),
        ("fork", "linux", "fork"),
        ("fork", "other", "fork"),
        ("forkserver", "linux", "forkserver"),
        ("forkserver", "other", "forkserver"),
        ("spawn", "linux", "spawn"),
        ("spawn", "other", "spawn"),
    ),
)
def test_get_context(start_method, platform, expected):
    if start_method:
        OldmanServer.start_method = start_method
    with patch("sys.platform", platform):
        assert OldmanServer._get_startup_method() == expected


@pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="Only test on Linux"
)
def test_set_startup_catch():
    OldmanServer.START_METHOD_SET = False
    set_start_method("fork", force=True)
    OldmanServer.test_mode = False
    message = (
        "Start method 'spawn' was requested, but 'fork' was already set.\n"
        "For more information, see: https://sanic.dev/en/guide/running/manager.html#overcoming-a-coderuntimeerrorcode"
    )
    with pytest.raises(RuntimeError, match=message):
        OldmanServer._set_startup_method()
    OldmanServer.test_mode = True


@pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="Only test on Linux"
)
def test_set_startup_when_the_expected_method_is_already_set():
    # Another library (or the application) set the context first, to the
    # method Sanic wants: nothing to change.
    OldmanServer.START_METHOD_SET = False
    set_start_method("spawn", force=True)
    OldmanServer.test_mode = False
    try:
        OldmanServer._set_startup_method()
        assert OldmanServer.START_METHOD_SET
    finally:
        OldmanServer.test_mode = True
