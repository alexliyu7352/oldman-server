import sys
import tempfile

from pathlib import Path

import pytest

from oldman_server.server.socket import (
    bind_unix_socket,
    configure_socket,
    remove_unix_socket,
)


pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="no unix sockets on Windows")


@pytest.fixture(autouse=True)
def short_cwd(monkeypatch):
    # The socket goes in the working directory, whose whole path must fit
    # sun_path (104 bytes on macOS): a short one.
    with tempfile.TemporaryDirectory(dir="/tmp") as directory:
        monkeypatch.chdir(directory)
        yield


def test_setup_and_teardown_unix():
    socket_address = "./test.sock"
    path = Path.cwd() / socket_address
    assert not path.exists()
    bind_unix_socket(socket_address)
    assert path.exists()
    remove_unix_socket(socket_address)
    assert not path.exists()


def test_configure_socket():
    socket_address = "./test.sock"
    path = Path.cwd() / socket_address
    assert not path.exists()
    configure_socket({"unix": socket_address, "backlog": 100})
    assert path.exists()
    remove_unix_socket(socket_address)
    assert not path.exists()
