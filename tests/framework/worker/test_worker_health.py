"""WORKER_HEALTH_TIMEOUT: the worker manager restarts a server worker whose
event loop stops answering (oldman_server.worker.health).

Each scenario runs a real app with worker processes in its own session; a
worker blocks its own loop (time.sleep from a callback) to be found silent.
"""
import os
import re
import signal
import socket
import subprocess
import sys
import textwrap
import time

from pathlib import Path

import pytest

from test_manager_failures import describe, finish, gone, read_until, session_gone, session_processes

from oldman_server import Config, OldmanServer
from oldman_server.worker import health


pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="POSIX sessions and signals")
# The scenarios check that no process of the service is left, from /proc.
reads_proc = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="reads /proc")


APP = textwrap.dedent(
    """
    import asyncio
    import os
    import time

    from oldman_server import Config, OldmanServer
    from oldman_server.native.cpus import worker_number
    from oldman_server.response import text

    app = OldmanServer("WorkerHealth")
    app.config.GRACEFUL_SHUTDOWN_TIMEOUT = 1


    @app.get("/")
    async def index(request):
        return text(str(os.getpid()))


    def stuck():
        time.sleep(60)


    def block_once(slot, marker, starts=1):
        # Block this worker's loop (as code that never yields would) on its
        # first `starts` starts: counted in `marker`.
        if worker_number() != slot:
            return
        count = int(open(marker).read()) if os.path.exists(marker) else 0
        if count >= starts:
            return
        open(marker, "w").write(str(count + 1))
        asyncio.get_running_loop().call_later(0.5, stuck)

    """
)


def start(directory: Path, body: str, *, workers=2, timeout=1) -> tuple[subprocess.Popen, int]:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    setting = "" if timeout is None else f"app.config.WORKER_HEALTH_TIMEOUT = {timeout!r}\n"
    path = directory / "app.py"
    path.write_text(
        APP
        + setting
        + textwrap.dedent(body)
        + textwrap.dedent(
            f"""
            @app.after_server_start
            async def ready(app):
                print("worker ready", worker_number(), os.getpid(), flush=True)

            if __name__ == "__main__":
                app.run(host="127.0.0.1", port={port}, workers={workers},
                        access_log=False)
            """
        )
    )
    process = subprocess.Popen(
        [sys.executable, str(path)],
        cwd=directory,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    return process, port


def get(port: int, path: str = "/") -> str:
    import urllib.request

    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as response:
        return response.read().decode()


def stop(process: subprocess.Popen, output: bytes, timeout=15) -> str:
    os.kill(process.pid, signal.SIGTERM)
    text = output.decode(errors="replace") + finish(process, timeout=timeout)
    left = session_processes(process.pid)
    assert session_gone(process.pid), [describe(pid) for pid in left]
    return text


@reads_proc
def test_a_worker_whose_loop_blocks_is_killed_and_replaced(tmp_path):
    marker = tmp_path / "blocked"
    process, port = start(
        tmp_path,
        f"""
        @app.after_server_start
        async def block(app):
            block_once(0, {str(marker)!r})
        """,
    )
    try:
        output = read_until(process, b"has not answered")
        warned = time.monotonic()
        pid = int(re.search(rb"Oldman-Server-0-0 \[(\d+)\] has not answered", output).group(1))
        # Killed at once, not stopped gracefully: gone within about a second.
        assert gone(pid, timeout=1.5), describe(pid)
        assert time.monotonic() - warned < 2
        output += read_until(process, b"] killed")
        output += read_until(process, b"worker ready", count=3 - output.count(b"worker ready"))
        # Every request sent once the replacement serves is answered: the
        # killed process takes none (with shard threads of its own, a
        # stuck process would still accept).
        assert all(get(port).isdigit() for _ in range(20))
        text = stop(process, output)
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
    assert text.count("has not answered") == 1, text
    assert "WORKER_HEALTH_TIMEOUT is 1.0s" in text, text
    # faulthandler's dump of the stuck worker, in its stderr: the frame it
    # was blocked in.
    assert re.search(r'File ".*app\.py", line \d+ in stuck', text), text
    assert "the stacks of its threads were written to its stderr" in text, text
    # Killed before its replacement started.
    lines = text.splitlines()
    killed = next(i for i, line in enumerate(lines) if "] killed" in line)
    readies = [i for i, line in enumerate(lines) if "worker ready" in line]
    assert len(readies) == 3 and readies[2] > killed, text
    assert process.returncode == 0, text


@reads_proc
def test_a_restarted_worker_is_watched_again(tmp_path):
    marker = tmp_path / "blocked"
    process, port = start(
        tmp_path,
        f"""
        @app.after_server_start
        async def block(app):
            block_once(0, {str(marker)!r}, starts=2)
        """,
    )
    try:
        output = read_until(process, b"has not answered", count=2, timeout=30)
        output += read_until(process, b"worker ready", count=4 - output.count(b"worker ready"))
        assert get(port).isdigit()
        text = stop(process, output)
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
    assert text.count("has not answered") == 2, text
    assert process.returncode == 0, text


@reads_proc
def test_a_worker_added_by_scaling_is_watched(tmp_path):
    marker = tmp_path / "blocked"
    process, port = start(
        tmp_path,
        f"""
        @app.get("/scale")
        async def scale(request):
            request.app.m.scale(3)
            return text("scaling")

        @app.after_server_start
        async def block(app):
            block_once(2, {str(marker)!r})
        """,
    )
    try:
        output = read_until(process, b"worker ready", count=2)
        assert get(port, "/scale") == "scaling"
        output += read_until(process, b"has not answered")
        output += read_until(process, b"worker ready", count=4 - output.count(b"worker ready"))
        text = stop(process, output)
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
    assert text.count("has not answered") == 1, text
    assert "Oldman-Server-2-0" in text.split("has not answered")[0].rsplit("\n", 1)[-1], text
    assert process.returncode == 0, text


@reads_proc
def test_healthy_workers_are_left_alone_and_stop_quietly(tmp_path):
    process, port = start(tmp_path, "")
    try:
        output = read_until(process, b"worker ready", count=2)
        # Three timeouts of an idle service.
        time.sleep(3.5)
        assert get(port).isdigit()
        text = stop(process, output)
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
    assert "has not answered" not in text, text
    assert text.count("worker ready") == 2, text
    assert "Task was cancelled" not in text, text
    assert "was destroyed" not in text, text
    assert "Traceback" not in text, text
    assert process.returncode == 0, text


@reads_proc
def test_a_stop_during_a_health_restart_ends_the_service(tmp_path):
    marker = tmp_path / "blocked"
    process, _ = start(
        tmp_path,
        f"""
        @app.after_server_start
        async def block(app):
            block_once(0, {str(marker)!r})
        """,
    )
    try:
        output = read_until(process, b"has not answered")
        # Before the blocked process is killed: the shutdown takes it.
        started = time.monotonic()
        text = stop(process, output, timeout=20)
        elapsed = time.monotonic() - started
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
    # The shutdown kills what does not stop within 2 x GRACEFUL_SHUTDOWN_TIMEOUT + 5 s.
    assert elapsed < 12, (elapsed, text)
    assert process.returncode == 0, text


@reads_proc
def test_off_by_default(tmp_path):
    process, port = start(
        tmp_path,
        """
        @app.get("/watched")
        async def watched(request):
            tasks = [task.get_name() for task in asyncio.all_tasks()]
            return text(f"{request.app.state.worker_health} {request.app.state.worker_health_timeout} "
                        f"{any('heartbeat' in name for name in tasks)}")
        """,
        timeout=None,
    )
    try:
        output = read_until(process, b"worker ready", count=2)
        answer = get(port, "/watched")
        text = stop(process, output)
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
    assert answer == "None None False", answer
    assert process.returncode == 0, text


@pytest.mark.parametrize("value", [-1, 0.5, "5", True, float("nan")])
def test_a_malformed_timeout_is_refused(value):
    with pytest.raises(ValueError, match="WORKER_HEALTH_TIMEOUT"):
        health.timeout(Config(defaults={"WORKER_HEALTH_TIMEOUT": value}, env_prefix=None))


@pytest.mark.parametrize("value", [None, 0, False])
def test_unset_or_zero_is_off(value):
    assert health.timeout(Config(defaults={"WORKER_HEALTH_TIMEOUT": value}, env_prefix=None)) is None


def test_a_malformed_timeout_fails_the_start(app: OldmanServer):
    app.config.WORKER_HEALTH_TIMEOUT = -1

    @app.get("/")
    async def index(request):
        ...

    with pytest.raises(ValueError, match="WORKER_HEALTH_TIMEOUT"):
        app.test_client.get("/")
