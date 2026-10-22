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
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
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
