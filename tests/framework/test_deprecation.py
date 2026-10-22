import pytest

from oldman_server import OldmanServer
from oldman_server.log import deprecation


def test_deprecation():
    message = r"\[DEPRECATION v9\.9\] hello"
    with pytest.warns(DeprecationWarning, match=message):
        deprecation("hello", 9.9)


@pytest.mark.parametrize(
    "filter,expected",
    (("default", 1), ("once", 1), ("ignore", 0)),
)
def test_deprecation_filter(app: OldmanServer, filter, expected, recwarn):
    app.config.DEPRECATION_FILTER = filter
    # Clear any warnings captured during test setup (e.g., from pytest_sanic)
    recwarn.clear()
    deprecation("hello", 9.9)
    assert len(recwarn) == expected
