import logging
import os
import platform

from unittest.mock import Mock, patch

import pytest

from oldman_server import __version__
from oldman_server.application.logo import LOGO
from oldman_server.application.motd import MOTD, MOTDTTY


@pytest.fixture(autouse=True)
def reset():
    try:
        del os.environ["OLDMAN_SERVER_MOTD_OUTPUT"]
    except KeyError:
        ...


def test_logo_base(app, run_startup):
    logs = run_startup(app)

    assert logs[0][1] == logging.DEBUG
    assert logs[0][2] == LOGO


def test_motd_with_expected_info(app, run_startup):
    logs = run_startup(app)

    assert logs[1][2] == f"oldman-server v{__version__}"
    assert logs[3][2] == "app: test_motd_with_expected_info"
    assert logs[4][2] == "mode: debug, single worker"
    assert logs[5][2] == "server: oldman_server, HTTP/1.1"
    assert logs[6][2] == f"python: {platform.python_version()}"
    assert logs[7][2] == f"platform: {platform.platform()}"


def test_motd_init():
    _orig = MOTDTTY.set_variables
    MOTDTTY.set_variables = Mock()
    motd = MOTDTTY(None, "", {}, {})

    motd.set_variables.assert_called_once()
    MOTDTTY.set_variables = _orig


def test_motd_display(caplog):
    motd = MOTDTTY("       foobar        ", "", {"one": "1"}, {"two": "2"})

    with caplog.at_level(logging.INFO):
        motd.display()

    version_line = f"oldman-server v{__version__}".center(motd.centering_length)
    assert (
        "".join(caplog.messages)
        == f"""
  ┌────────────────────────────────┐
  │ {version_line} │
  │                                │
  ├───────────────────────┬────────┤
  │        foobar         │ one: 1 │
  │                       ├────────┤
  │                       │ two: 2 │
  └───────────────────────┴────────┘
"""
    )


def test_reload_dirs(app):
    app.config.LOGO = None
    app.config.MOTD = True
    app.config.AUTO_RELOAD = True

    with patch.object(MOTD, "output") as mock:
        app.prepare(
            reload_dir="./", auto_reload=True, motd_display={"foo": "bar"}
        )
    mock.assert_called()
    assert mock.call_args.args[2]["auto-reload"] == f"enabled, {os.getcwd()}"
    assert mock.call_args.args[3] == {"foo": "bar"}


def test_the_terminal_box_lines_up_with_the_logo():
    # Every line of the box has the same width: the logo column too.
    from oldman_server.application.logo import get_logo

    out = []
    MOTDTTY(get_logo(), "http://127.0.0.1:8000", {"app": "demo", "mode": "debug"},
            {"server": "oldman_server, HTTP/1.1", "python": "3.14.7"}).display(out=out.append)
    lines = [line.strip() for line in out[0].split("\n") if line.strip()]
    assert len({len(line) for line in lines}) == 1, lines
    assert any("oldman-server" in line and line.startswith("│") for line in lines[3:])
