# import asyncio
import logging
import os
import sys

from asyncio import AbstractEventLoop, sleep
from pathlib import Path
from string import ascii_lowercase

import httpcore
import httpx
import pytest

from pytest import LogCaptureFixture

from oldman_server import OldmanServer, __version__
from oldman_server.compat import use_context
from oldman_server.request import Request
from oldman_server.response import text


pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="no unix sockets on Windows")


pytestmark = [
    pytest.mark.skipif(os.name != "posix", reason="UNIX only"),
    pytest.mark.xdist_group(name="unix_socket"),
]
SOCKPATH = Path("/tmp/sanictest.sock")
SOCKPATH2 = "/tmp/sanictest2.sock"
httpx_version = tuple(
    map(int, httpx.__version__.strip(ascii_lowercase).split("."))
)


@pytest.fixture(autouse=True)
def socket_cleanup():
    try:
        os.unlink(SOCKPATH)
    except FileNotFoundError:
        pass
    try:
        os.unlink(SOCKPATH2)
    except FileNotFoundError:
        pass
    # Run test function
    yield
    try:
        os.unlink(SOCKPATH2)
    except FileNotFoundError:
        pass
    try:
        os.unlink(SOCKPATH)
    except FileNotFoundError:
        pass


def test_unix_socket_creation(caplog: LogCaptureFixture):
    from socket import AF_UNIX, socket

    # A stale socket file: nobody listens on it.
    with socket(AF_UNIX) as sock:
        sock.bind(str(SOCKPATH))
    assert os.path.exists(SOCKPATH)

    app = OldmanServer(name="test")

    @app.after_server_start
    def running(app: OldmanServer):
        # Replaced by the server's socket. (Comparing inodes is not enough:
        # the file system may give the new file the stale one's inode.)
        with socket(AF_UNIX) as client:
            client.connect(str(SOCKPATH))
        app.stop()

    with caplog.at_level(logging.INFO):
        app.run(unix=SOCKPATH, single_process=True)

    assert (
        "oldman_server.root",
        logging.INFO,
        f"oldman-server {__version__}: test on {SOCKPATH} http://... "
        "(production mode, 1 worker)",
    ) in caplog.record_tuples
    assert not os.path.exists(SOCKPATH)


@pytest.mark.parametrize("path", (".", "no-such-directory/sanictest.sock"))
def test_invalid_paths(path: str):
    app = OldmanServer(name="test")
    #
    with pytest.raises((FileExistsError, FileNotFoundError)):
        app.run(unix=path, single_process=True)


def test_dont_replace_file():
    SOCKPATH.write_text("File, not socket")

    app = OldmanServer(name="test")

    @app.after_server_start
    def stop(app: OldmanServer):
        app.stop()

    with pytest.raises(FileExistsError):
        app.run(unix=SOCKPATH, single_process=True)


def test_dont_follow_symlink():
    from socket import AF_UNIX, socket

    with socket(AF_UNIX) as sock:
        sock.bind(SOCKPATH2)
    os.symlink(SOCKPATH2, SOCKPATH)

    app = OldmanServer(name="test")

    @app.after_server_start
    def stop(app: OldmanServer):
        app.stop()

    with pytest.raises(FileExistsError):
        app.run(unix=SOCKPATH, single_process=True)


def test_socket_deleted_while_running():
    app = OldmanServer(name="test")

    @app.after_server_start
    async def hack(app: OldmanServer):
        os.unlink(SOCKPATH)
        app.stop()

    app.run(host="myhost.invalid", unix=SOCKPATH, single_process=True)


def test_socket_replaced_with_file():
    app = OldmanServer(name="test")

    @app.after_server_start
    async def hack(app: OldmanServer):
        os.unlink(SOCKPATH)
        with open(SOCKPATH, "w") as f:
            f.write("Not a socket")
        app.stop()

    app.run(host="myhost.invalid", unix=SOCKPATH, single_process=True)


def test_unix_connection():
    app = OldmanServer(name="test")

    @app.get("/")
    def handler(request: Request):
        return text(f"{request.conn_info.server}")

    @app.after_server_start
    async def client(app: OldmanServer):
        if httpx_version >= (0, 20):
            transport = httpx.AsyncHTTPTransport(uds=SOCKPATH)
        else:
            transport = httpcore.AsyncConnectionPool(uds=SOCKPATH)
        try:
            async with httpx.AsyncClient(transport=transport) as client:
                r = await client.get("http://myhost.invalid/")
                assert r.status_code == 200
                assert r.text == os.path.abspath(SOCKPATH)
        finally:
            app.stop()

    app.run(host="myhost.invalid", unix=SOCKPATH, single_process=True)


def handler(request: Request):
    return text(f"{request.conn_info.server}")


async def client(app: OldmanServer, loop: AbstractEventLoop):
    try:
        transport = httpx.AsyncHTTPTransport(uds=SOCKPATH)
        async with httpx.AsyncClient(transport=transport) as client:
            r = await client.get("http://myhost.invalid/")
            assert r.status_code == 200
            assert r.text == os.path.abspath(SOCKPATH)
    finally:
        await sleep(0.2)
        app.stop()


@pytest.mark.skipif(
    sys.platform not in ("linux", "darwin"),
    reason="This test requires fork context",
)
def test_unix_connection_multiple_workers():
    with use_context("fork"):
        app_multi = OldmanServer(name="test")
        app_multi.get("/")(handler)
        app_multi.listener("after_server_start")(client)
        app_multi.run(host="myhost.invalid", unix=SOCKPATH, workers=2)


@pytest.mark.parametrize("mode, expected", [(None, 0o666), (0o660, 0o660), ("0o600", 0o600)])
def test_unix_socket_mode(mode, expected):
    # UNIX_SOCKET_MODE: who may connect (TRUSTED_PROXIES="unix" trusts them all).
    import stat

    app = OldmanServer(name="test")
    if mode is not None:
        app.config.UNIX_SOCKET_MODE = mode
    seen = []

    @app.after_server_start
    def running(app: OldmanServer):
        seen.append(stat.S_IMODE(os.stat(SOCKPATH).st_mode))
        app.stop()

    app.run(unix=SOCKPATH, single_process=True)
    assert seen == [expected]


@pytest.mark.parametrize("mode", [0o1777, 660, "rw", -1])
def test_unix_socket_mode_must_be_permission_bits(mode):
    app = OldmanServer(name="test")
    app.config.UNIX_SOCKET_MODE = mode
    with pytest.raises(ValueError, match="UNIX_SOCKET_MODE"):
        app.run(unix=SOCKPATH, single_process=True)
