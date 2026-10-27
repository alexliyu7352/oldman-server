"""The worker manager ends the service on startup failures and shutdown
signals, and workers do not outlive the main process.

Each scenario runs a real app with two worker processes in its own session.
"""
import os
import select
import signal
import socket
import subprocess
import sys
import textwrap
import time

from pathlib import Path

import pytest


# Each scenario has a session of its own and is ended by POSIX signals.
pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="POSIX sessions and signals")


APP = textwrap.dedent(
    """
    import asyncio
    import os
    import sys

    from oldman_server import OldmanServer
    from oldman_server.response import text

    app = OldmanServer("ManagerFailures")


    @app.get("/")
    async def index(request):
        return text("ok")


    @app.main_process_stop
    async def main_stopped(app):
        print("main process stopped", flush=True)

    """
)


def start(directory: Path, body: str) -> subprocess.Popen:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    path = directory / "app.py"
    path.write_text(
        APP
        + textwrap.dedent(body)
        + textwrap.dedent(
            f"""
            if __name__ == "__main__":
                app.run(host="127.0.0.1", port={port}, workers=2,
                        access_log=False, motd=False)
            """
        )
    )
    return subprocess.Popen(
        [sys.executable, str(path)],
        cwd=directory,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )


def read_until(process: subprocess.Popen, marker: bytes, count=1, timeout=20):
    output = b""
    deadline = time.monotonic() + timeout
    while output.count(marker) < count:
        assert time.monotonic() < deadline, output.decode(errors="replace")
        ready, _, _ = select.select([process.stdout], [], [], 0.1)
        if ready:
            chunk = os.read(process.stdout.fileno(), 4096)
            assert chunk, output.decode(errors="replace")
            output += chunk
    return output


def finish(process: subprocess.Popen, timeout=15) -> str:
    try:
        output, _ = process.communicate(timeout=timeout)
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
    return output.decode(errors="replace")


def gone(pid: int, timeout=10) -> bool:
    """Whether pid has exited: no such process, or a zombie (exited and not yet
    reaped; in a container whose init does not reap orphans it stays one)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            status = Path(f"/proc/{pid}/status").read_text().splitlines()
        except FileNotFoundError:
            return True
        if any(line.split()[:2] == ["State:", "Z"] for line in status):
            return True
        time.sleep(0.1)
    return False


def describe(pid: int) -> str:
    """A surviving process's state, for the failure message."""
    try:
        status = Path(f"/proc/{pid}/status").read_text().splitlines()
        wchan = Path(f"/proc/{pid}/wchan").read_text()
    except OSError as error:
        return f"{pid}: {error}"
    fields = [line for line in status
              if line.split(":")[0] in ("State", "PPid", "SigBlk", "SigIgn", "SigCgt", "Threads")]
    return f"{pid}: {fields} wchan={wchan}"


def test_a_worker_failing_after_it_acknowledged_ends_the_service(tmp_path):
    # A worker acknowledges after before_server_start; one whose
    # after_server_start fails then sends __TERMINATE_EARLY__, which the
    # manager must not read as the name of a process to restart.
    process = start(
        tmp_path,
        """
        @app.after_server_start
        async def fail_late(app):
            # Long enough for the main process to have left its startup wait.
            await asyncio.sleep(0.5)
            if app.m.name.startswith("Oldman-Server-0"):
                raise RuntimeError("expected late startup failure")
        """,
    )
    text = finish(process)
    assert "expected late startup failure" in text
    assert "failed after acknowledging its startup" in text
    assert "Failed to restart processes" not in text
    # The manager kills the workers, not itself: the main process shuts down.
    assert "main process stopped" in text
    assert process.returncode == 1


def test_a_shutdown_signal_while_workers_start_ends_the_service(tmp_path):
    process = start(
        tmp_path,
        """
        @app.before_server_start
        async def slow(app):
            print("worker starting", flush=True)
            await asyncio.sleep(30)
        """,
    )
    try:
        read_until(process, b"worker starting")
        os.kill(process.pid, signal.SIGTERM)
        process.wait(timeout=15)
    finally:
        finish(process)


def test_workers_end_when_the_main_process_is_killed(tmp_path):
    process = start(
        tmp_path,
        """
        @app.after_server_start
        async def report(app):
            print(f"worker pid {os.getpid()}", flush=True)
        """,
    )
    try:
        output = read_until(process, b"worker pid", count=2)
        pids = [int(line.split()[-1]) for line in output.decode().splitlines()
                if line.startswith("worker pid")]
        os.kill(process.pid, signal.SIGKILL)
        process.wait()
        survivors = [pid for pid in pids if not gone(pid)]
        assert not survivors, [describe(pid) for pid in survivors]
    finally:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.stdout.close()


def get(port: int) -> str:
    import urllib.request

    with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=5) as response:
        return response.read().decode()


def test_a_worker_failing_while_it_stops_does_not_end_the_service(tmp_path):
    # Worker 0 restarts itself; its after_server_stop fails. That is the end
    # of an old process, not a failed start: the service goes on.
    marker = tmp_path / "restarted"
    process = start(
        tmp_path,
        f"""
        @app.after_server_start
        async def restart_once(app):
            if app.m.name.startswith("Oldman-Server-0") and not os.path.exists({str(marker)!r}):
                open({str(marker)!r}, "w").close()
                app.m.restart()
            print("worker ready", flush=True)

        @app.after_server_stop
        async def fail_on_stop(app):
            if app.m.name.startswith("Oldman-Server-0"):
                raise ValueError("expected stop failure")
        """,
    )
    try:
        output = read_until(process, b"worker ready", count=3)
        time.sleep(1)
        assert process.poll() is None, output.decode(errors="replace")
        os.kill(process.pid, signal.SIGTERM)
        text = output.decode(errors="replace") + finish(process)
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
    assert "expected stop failure" in text
    assert "failed after acknowledging" not in text
    assert "main process stopped" in text
    assert process.returncode == 0, text


def test_a_restarted_worker_that_fails_to_start_leaves_the_others(tmp_path):
    # As with auto-reload picking up a broken file: the restarted worker
    # fails before acknowledging; the others go on serving.
    marker = tmp_path / "restarted"
    (tmp_path / "broken_app.py").write_text(
        APP
        + textwrap.dedent(
            f"""
            if os.environ.get("OLDMAN_SERVER_WORKER_NAME", "").startswith(
                    "Oldman-Server-0") and os.path.exists({str(marker)!r}):
                raise SyntaxError("expected import failure")

            @app.after_server_start
            async def restart_once(app):
                if app.m.name.startswith("Oldman-Server-0") and not os.path.exists({str(marker)!r}):
                    open({str(marker)!r}, "w").close()
                    app.m.restart()
                print("worker ready", flush=True)
            """
        )
    )
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    # Each worker loads the module by name (as oldman's runner does), so the
    # restarted one imports the broken file again.
    (tmp_path / "run.py").write_text(textwrap.dedent(
        f"""
        from oldman_server import OldmanServer
        from oldman_server.worker.loader import AppLoader

        if __name__ == "__main__":
            loader = AppLoader("broken_app:app")
            app = loader.load()
            app.prepare(host="127.0.0.1", port={port}, workers=2, motd=False)
            OldmanServer.serve(primary=app, app_loader=loader)
        """
    ))
    process = subprocess.Popen(
        [sys.executable, "run.py"],
        cwd=tmp_path, env={**os.environ, "PYTHONPATH": str(tmp_path)},
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True,
    )
    try:
        output = read_until(process, b"expected import failure")
        time.sleep(1)
        assert process.poll() is None, output.decode(errors="replace")
        assert get(port) == "ok"
        os.kill(process.pid, signal.SIGTERM)
        text = output.decode(errors="replace") + finish(process)
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
    assert "failed after acknowledging" not in text
    assert "failed to start" in text


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="the native host is Linux only")
@pytest.mark.parametrize("groups, same_thread", [("0,4000;1,4001", False), ("4000;4001", True)],
                         ids=["shards", "same-thread"])
def test_a_native_worker_whose_engine_cannot_start_ends_the_service(tmp_path, groups, same_thread):
    # The worker acknowledges, then its native host fails to start (CPUs
    # that do not exist): the service ends instead of running without it.
    # The groups' size says the mode, whatever the environment sets.
    process = start(
        tmp_path,
        f"""
        app.config.NATIVE_SERVER = True
        app.config.NATIVE_WORKER_CPUS = "{groups}"
        app.config.NATIVE_SAME_THREAD = {same_thread}
        """,
    )
    try:
        text = finish(process, timeout=30)
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
    assert "shard_cpus must be distinct" in text
    # Reported as a failed start: by the startup wait, or after it.
    assert ("terminated before startup was completed" in text
            or "failed after acknowledging" in text), text
    assert process.returncode == 1
