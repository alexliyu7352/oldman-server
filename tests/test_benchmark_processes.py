"""Benchmark cleanup owns the pipes even when the child already exited."""
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from run_comparison import stop_process


@pytest.mark.parametrize("running", [False, True])
def test_stop_process_reaps_child_and_closes_owned_pipes(running):
    code = "import sys; print('ready', flush=True)"
    if running:
        code += "; sys.stdin.buffer.read(1)"
    with subprocess.Popen([sys.executable, "-c", code], stdin=subprocess.PIPE,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          bufsize=0, start_new_session=True) as child:
        assert child.stdout.readline() == b"ready\n"
        if not running:
            assert child.wait(5) == 0
        stop_process(child)
        assert child.poll() is not None
        assert all(pipe.closed for pipe in (child.stdin, child.stdout, child.stderr))
        stop_process(child)


@pytest.mark.parametrize("leader_exited", [False, True])
def test_stop_process_kills_remaining_group_workers(leader_exited):
    worker_code = """
import signal
signal.signal(signal.SIGTERM, signal.SIG_IGN)
print('ready', flush=True)
signal.pause()
"""
    leader_code = """
import signal, subprocess, sys
worker = subprocess.Popen([sys.executable, '-c', sys.argv[1]], stdout=subprocess.PIPE)
assert worker.stdout.readline() == b'ready\\n'
print(worker.pid, flush=True)
if sys.argv[2] == 'wait':
    signal.pause()
"""
    with subprocess.Popen([sys.executable, "-c", leader_code, worker_code,
                           "exit" if leader_exited else "wait"],
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          start_new_session=True) as leader:
        try:
            worker_pid = int(leader.stdout.readline())
            if leader_exited:
                assert leader.wait(5) == 0
            stop_process(leader)
            # Orphaned zombies await the host's reaper, but cannot consume CPU
            # or keep sockets open. Wait only for a live worker to terminate.
            stat = Path(f"/proc/{worker_pid}/stat")
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                try:
                    state = stat.read_text().rsplit(")", 1)[1].split()[0]
                except FileNotFoundError:
                    break
                if state == "Z":
                    break
                time.sleep(.01)
            else:
                pytest.fail("worker still running after process-group cleanup")
            assert leader.poll() is not None
            stop_process(leader)
        finally:
            # Even the RED regression must not leak its deliberately stubborn
            # worker into later tests or measurements.
            try:
                os.killpg(leader.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            leader.wait(5)
