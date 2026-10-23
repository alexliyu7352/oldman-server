"""The native runtime's own lifecycle, used directly (the Engine interface):
each case runs in a process of its own, as Seastar starts once per process."""
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

START = textwrap.dedent('''
    import os, select, sys, time
    from oldman_server.native import _native as n
    cpus = sorted(os.sched_getaffinity(0))

    def ready(runtime):
        fd = runtime.fd()
        while runtime.stats()["state"] == 0:
            select.select([fd], [], [], 1)
            runtime.drain(16)
        assert runtime.stats()["state"] == 1, runtime.stats()
        return fd

    def readable(fd, timeout):
        return bool(select.select([fd], [], [], timeout)[0])
''')


def run(script, *args, timeout=30):
    try:
        done = subprocess.run([sys.executable, "-W", "ignore", "-c", START + textwrap.dedent(script),
                               *args], cwd=ROOT, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as expired:
        pytest.fail(f"hung: {expired.stdout!r} {expired.stderr!r}")
    assert done.returncode == 0, done.stderr
    return done.stdout


def test_a_wake_is_not_lost_after_a_server_that_did_not_coalesce():
    # notify_pending was cleared only while the current server coalesced
    # notifications: coalescing, then plain, then coalescing again left it
    # set, and the third server's first completion woke nobody.
    out = run('''
        def config(coalesce):
            config = n.ServerConfig()
            config.coalesce_notifications = coalesce
            return config

        runtime = n.Runtime([cpus[-1]], 16, 1 << 20, [], config(True), "epoll")
        fd = ready(runtime)
        runtime.drain(16)
        assert runtime.submit(1, b"a", 0) and readable(fd, 5)
        generation = runtime.start_server(config(False))
        runtime.drain(16)
        runtime.stop_server(generation)
        generation = runtime.start_server(config(True))
        assert runtime.submit(2, b"b", 0)
        print("woke", readable(fd, 2))
        runtime.drain(16)
        runtime.stop_server(generation)
        runtime.stop()
        while runtime.stats()["state"] < 3:
            readable(fd, 0.1)
            runtime.drain(16)
        runtime.join()
        runtime.close_fd()
    ''')
    assert "woke True" in out


def test_the_process_exits_with_a_result_nobody_took():
    # The persistent runtime's atexit stop refused while a completed result
    # had not been drained; the interpreter then joined its thread for ever.
    out = run('''
        from types import SimpleNamespace
        from oldman_server.native import engine
        config = SimpleNamespace(shard_cpus=(cpus[-1],), max_pending=16, max_pending_bytes=1 << 20,
                                 worker_cpus=(), reactor_backend="epoll")
        runtime, created = engine._persistent_runtime(config)
        fd = ready(runtime)
        assert runtime.submit(1, b"x", 0)
        assert runtime.submit(2, b"y", 300)  # still running at exit
        readable(fd, 5)
        print("exiting", runtime.stats()["pending_requests"])
    ''')
    assert "exiting 2" in out


@pytest.mark.parametrize("mode", ["plain", "threads", "workers"])
def test_a_forked_child_leaves_the_inherited_runtime_alone(mode):
    # A child forked after the runtime started destroyed its copy: joining
    # the parent's threads (or destroying a condition variable the parent's
    # workers wait on) hung it.
    if mode == "workers" and len(os.sched_getaffinity(0)) < 2:
        pytest.skip("needs two CPUs")
    out = run('''
        import threading
        mode = sys.argv[1]
        workers = [cpus[-2]] if mode == "workers" else []
        runtime = n.Runtime([cpus[-1]], 16, 1 << 20, workers, n.ServerConfig(), "epoll")
        fd = ready(runtime)
        pid = os.fork()
        if pid == 0:
            if mode == "threads":  # a new thread may reuse a parent thread's descriptor
                for _ in range(4):
                    threading.Thread(target=time.sleep, args=(3,), daemon=True).start()
            del runtime
            os._exit(0)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            done, status = os.waitpid(pid, os.WNOHANG)
            if done:
                print("child", os.waitstatus_to_exitcode(status))
                break
            time.sleep(0.05)
        else:
            print("child hung")
            os.kill(pid, 9)
            os.waitpid(pid, 0)
        runtime.stop()
        while runtime.stats()["state"] < 3:
            readable(fd, 0.1)
            runtime.drain(16)
        runtime.join()
        runtime.close_fd()
    ''', mode)
    assert "child 0" in out, out
