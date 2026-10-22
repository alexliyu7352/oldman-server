import json
import os
import sys

from pathlib import Path
from typing import Optional
from unittest.mock import patch

import pytest

from sanic_routing import __version__ as __routing_version__

from oldman_server import __version__
from oldman_server.__main__ import main


@pytest.fixture(scope="module", autouse=True)
def tty():
    orig = sys.stdout.isatty
    sys.stdout.isatty = lambda: False
    yield
    sys.stdout.isatty = orig


def capture(command: list[str], caplog=None, capsys=None):
    if capsys:
        capsys.readouterr()
    if caplog:
        caplog.clear()
    os.chdir(Path(__file__).parent)
    try:
        main(command)
    except SystemExit:
        ...
    if capsys:
        captured_err = capsys.readouterr()
        return captured_err
    if caplog:
        return [record.message for record in caplog.records]
    return None


def read_app_info(lines: list[str]):
    for line in lines:
        if line.startswith("{") and line.endswith("}"):  # type: ignore
            return json.loads(line)


@pytest.mark.parametrize(
    "appname,extra",
    (
        ("fake.server.app", None),
        ("fake.server", None),
        ("fake.server:create_app", "--factory"),
        ("fake.server.create_app()", None),
        ("fake.server.create_app", None),
    ),
)
def test_server_run(
    appname: str, extra: Optional[str], caplog: pytest.LogCaptureFixture, port
):
    command = [appname, f"-p={port}"]
    if extra:
        command.append(extra)
    lines = capture(command, caplog)

    assert f"Goin' Fast @ http://127.0.0.1:{port}" in lines


@pytest.mark.parametrize(
    "command",
    (
        ["fake.server.create_app_with_args", "--factory"],
        ["fake.server.create_app_with_args"],
    ),
)
def test_server_run_factory_with_args(caplog, command, port):
    command.append(f"-p={port}")
    lines = capture(command, caplog)

    assert "target=fake.server.create_app_with_args" in lines


def test_server_run_factory_with_args_arbitrary(caplog, port):
    command = [
        "fake.server.create_app_with_args",
        "--factory",
        "--foo=bar",
        f"-p={port}",
    ]
    lines = capture(command, caplog)

    assert "foo=bar" in lines


@pytest.mark.parametrize(
    "cmd",
    (
        ("--host=localhost", "--port={port}"),
        ("-H", "localhost", "-p", "{port}"),
    ),
)
def test_host_port_localhost(cmd: tuple[str, ...], caplog, port):
    cmd = [c.format(port=str(port)) for c in cmd]
    command = ["fake.server.app", *cmd]
    lines = capture(command, caplog)
    expected = f"Goin' Fast @ http://localhost:{port}"

    assert expected in lines


@pytest.mark.parametrize(
    "cmd,expected",
    (
        (
            ("--host=localhost", "--port={port}"),
            "Goin' Fast @ http://localhost:{port}",
        ),
        (
            ("-H", "localhost", "-p", "{port}"),
            "Goin' Fast @ http://localhost:{port}",
        ),
        (
            ("--host=127.0.0.1", "--port={port}"),
            "Goin' Fast @ http://127.0.0.1:{port}",
        ),
        (
            ("-H", "127.0.0.1", "-p", "{port}"),
            "Goin' Fast @ http://127.0.0.1:{port}",
        ),
        (("--host=::", "--port={port}"), "Goin' Fast @ http://[::]:{port}"),
        (("-H", "::", "-p", "{port}"), "Goin' Fast @ http://[::]:{port}"),
        (("--host=::1", "--port={port}"), "Goin' Fast @ http://[::1]:{port}"),
        (("-H", "::1", "-p", "{port}"), "Goin' Fast @ http://[::1]:{port}"),
    ),
)
def test_host_port(cmd: tuple[str, ...], expected: str, caplog, port):
    cmd = [c.format(port=str(port)) for c in cmd]
    expected = expected.format(port=str(port))
    command = ["fake.server.app", *cmd]
    lines = capture(command, caplog)

    assert expected in lines


@pytest.mark.parametrize(
    "num,cmd",
    (
        (1, (f"--workers={1}",)),
        (2, (f"--workers={2}",)),
        (4, (f"--workers={4}",)),
        (1, ("-w", "1")),
        (2, ("-w", "2")),
        (4, ("-w", "4")),
    ),
)
def test_num_workers(num: int, cmd: tuple[str, ...], caplog, port):
    command = ["fake.server.app", *cmd, f"-p={port}"]
    lines = capture(command, caplog)

    if num == 1:
        expected = "mode: production, single worker"
    else:
        expected = f"mode: production, w/ {num} workers"

    assert expected in lines


@pytest.mark.parametrize("cmd", ("--debug",))
def test_debug(cmd: str, caplog, port):
    command = ["fake.server.app", cmd, f"-p={port}"]
    lines = capture(command, caplog)
    info = read_app_info(lines)

    assert info["debug"] is True
    assert info["auto_reload"] is False


@pytest.mark.parametrize("cmd", ("--dev", "-d"))
def test_dev(cmd: str, caplog, port):
    command = ["fake.server.app", cmd, f"-p={port}"]
    lines = capture(command, caplog)
    info = read_app_info(lines)

    assert info["debug"] is True
    assert info["auto_reload"] is True


@pytest.mark.parametrize("cmd", ("--auto-reload", "-r"))
def test_auto_reload(cmd: str, caplog, port):
    command = ["fake.server.app", cmd, f"-p={port}"]
    lines = capture(command, caplog)
    info = read_app_info(lines)

    assert info["debug"] is False, f"Unexpected value of debug {info}"
    assert info["auto_reload"] is True, (
        f"Unexpected value of auto reload {info}"
    )


@pytest.mark.parametrize(
    "cmd,expected",
    (
        ("", False),
        ("--debug", True),
        ("--access-log", True),
        ("--no-access-log", False),
    ),
)
def test_access_logs(cmd: str, expected: bool, caplog, port):
    command = ["fake.server.app", f"-p={port}"]
    if cmd:
        command.append(cmd)
    lines = capture(command, caplog)
    print(lines)
    info = read_app_info(lines)
    if info["access_log"] != expected:
        print(lines)
    assert info["access_log"] is expected, (
        f"Expected: {expected}. Received: {info}. Lines: {lines}"
    )


@pytest.mark.parametrize("cmd", ("--version", "-v"))
def test_version(cmd: str, caplog, capsys):
    command = [cmd]
    capture(command, caplog)
    version_string = f"oldman-server {__version__}; Routing {__routing_version__}\n"
    out, _ = capsys.readouterr()
    assert version_string == out


@pytest.mark.parametrize(
    "cmd,expected",
    (
        ("--noisy-exceptions", True),
        ("--no-noisy-exceptions", False),
    ),
)
def test_noisy_exceptions(cmd: str, expected: bool, caplog, port):
    command = ["fake.server.app", cmd, f"-p={port}"]
    lines = capture(command, caplog)
    info = read_app_info(lines)

    assert info["noisy_exceptions"] is expected


def test_command_no_args(caplog):
    args = ["fake.server.app", "exec", "foo"]
    with patch("sys.argv", ["oldman-server", *args]):
        lines = capture(args, caplog)
    assert "FOO one=None two=None three='...'" in lines


def test_command_with_args(caplog):
    args = [
        "fake.server.app",
        "exec",
        "foo",
        "--one=1",
        "--two=2",
        "--three=3",
    ]
    with patch("sys.argv", ["oldman-server", *args]):
        lines = capture(args, caplog)
    assert "FOO one='1' two='2' three='3'" in lines


def test_command_with_sync_handler(caplog):
    args = ["fake.server.app", "exec", "bar"]
    with patch("sys.argv", ["oldman-server", *args]):
        lines = capture(args, caplog)
    assert "BAR" in lines


def test_command_with_renamed_command(caplog):
    args = ["fake.server.app", "exec", "qqq"]
    with patch("sys.argv", ["oldman-server", *args]):
        lines = capture(args, caplog)
    assert "BAZ" in lines
