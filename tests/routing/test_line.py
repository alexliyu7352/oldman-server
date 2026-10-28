import pytest

from oldman_server.routing.line import Line


@pytest.mark.parametrize(
    "line,expected",
    (
        (Line("foo", 0), "foo\n"),
        (Line("foo", 1), "    foo\n"),
        (Line("foo", 2), "        foo\n"),
    ),
)
def test_proper_indentation(line, expected):
    assert str(line) == expected
