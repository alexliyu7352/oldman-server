"""The native runtime's own lifecycle, used directly (the Engine interface):
each case runs in a process of its own, as a process starts one runtime."""
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
            runtime.clear_notification()
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
    # set, and the third server's first completion woke nobody. A file
    # operation's result is such a completion.
    out = run('''
        def config(coalesce):
            config = n.ServerConfig()
            config.coalesce_notifications = coalesce
            return config

        runtime = n.Runtime([cpus[-1]], config(True), "epoll")
        fd = ready(runtime)
        runtime.clear_notification()
        runtime.submit_file(1, 0, sys.executable, 0, -1)  # a file_stat
        assert readable(fd, 5)
        generation = runtime.start_server(config(False))
        runtime.clear_notification()
        assert len(runtime.drain_files(16)) == 1
        runtime.stop_server(generation)
        generation = runtime.start_server(config(True))
        runtime.submit_file(2, 0, sys.executable, 0, -1)
        print("woke", readable(fd, 2))
        runtime.clear_notification()
        runtime.stop_server(generation)
        runtime.stop()
        while runtime.stats()["state"] < 3:
            readable(fd, 0.1)
            runtime.clear_notification()
        runtime.join()
        runtime.close_fd()
    ''')
    assert "woke True" in out


@pytest.mark.parametrize("mode", ["plain", "threads"])
def test_a_forked_child_leaves_the_inherited_runtime_alone(mode):
    # A child forked after the runtime started destroyed its copy: joining
    # the parent's threads hung it.
    out = run('''
        import threading
        mode = sys.argv[1]
        runtime = n.Runtime([cpus[-1]], n.ServerConfig(), "epoll")
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
            runtime.clear_notification()
        runtime.join()
        runtime.close_fd()
    ''', mode)
    assert "child 0" in out, out


SAME_THREAD = '''
    import socket

    def pump(runtime, timeout):
        # The poller's turn (what the Engine does): either descriptor, then poll.
        select.select([runtime.fd(), runtime.reactor_fd()], [], [], timeout)
        runtime.clear_notification()
        runtime.poll()

    def until(runtime, state):
        deadline = time.monotonic() + 10
        while runtime.stats()["state"] < state and time.monotonic() < deadline:
            pump(runtime, 0.1)
        assert runtime.stats()["state"] >= state, runtime.stats()

    def server_config():
        config = n.ServerConfig()
        config.application_dispatch_all = True
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            config.http_port = probe.getsockname()[1]
        return config
'''


def run_same_thread(script):
    return run(textwrap.dedent(SAME_THREAD) + textwrap.dedent(script))


def test_a_same_thread_runtime_runs_while_its_thread_polls_it():
    # No shard thread: the control thread's start and stop of the server
    # reach the shard through the notification, and happen only as the
    # thread that made the runtime polls it.
    out = run_same_thread('''
        threads = len(os.listdir("/proc/self/task"))
        config = server_config()
        runtime = n.Runtime([cpus[-1]], config, "epoll", True)
        assert runtime.reactor_fd() >= 0 and runtime.shards() == []
        until(runtime, 1)
        with socket.create_connection(("127.0.0.1", config.http_port), timeout=5):
            pass
        # The control thread and the file threads; none for a shard.
        print("threads", len(os.listdir("/proc/self/task")) - threads)
        runtime.stop()
        until(runtime, 3)
        runtime.join()
        runtime.close_fd()
        print("stopped")
    ''')
    assert "threads 3" in out and "stopped" in out, out


def test_the_polling_thread_starts_and_stops_a_server_itself():
    # Engine.abandon stops a persistent runtime's server from the thread that
    # polls it, which then runs the shard while it waits instead of hanging.
    out = run_same_thread('''
        runtime = n.Runtime([cpus[-1]], n.ServerConfig(), "epoll", True)
        until(runtime, 1)
        config = server_config()
        generation = runtime.start_server(config)
        with socket.create_connection(("127.0.0.1", config.http_port), timeout=5):
            pass
        runtime.stop_server(generation)
        print("stopped server")
        runtime.stop()
        until(runtime, 3)
        runtime.join()
        runtime.close_fd()
    ''')
    assert "stopped server" in out, out


@pytest.mark.parametrize("backend", ["epoll", "io_uring"])
def test_a_same_thread_runtime_serves_on_either_backend(backend):
    # The poller waits on the reactor's descriptor: the epoll set, or the
    # io_uring ring (where the system allows io_uring; else epoll and why).
    from test_sanic_native_run import io_uring_available
    out = run_same_thread(f'''
        config = server_config()
        runtime = n.Runtime([cpus[-1]], config, "{backend}", True)
        print("fallback", repr(runtime.fallback()), runtime.reactor_fd() >= 0)
        until(runtime, 1)
        with socket.create_connection(("127.0.0.1", config.http_port), timeout=5):
            pass
        runtime.stop()
        until(runtime, 3)
        runtime.join()
        runtime.close_fd()
        print("stopped")
    ''')
    expected = "fallback '' True" if backend == "epoll" or io_uring_available() else "fallback 'io_uring"
    assert expected in out and "stopped" in out, out


def test_a_same_thread_runtime_takes_one_cpu():
    with pytest.raises(AssertionError, match="one CPU"):
        run_same_thread('''
            n.Runtime(cpus[-2:], n.ServerConfig(), "epoll", True)
        ''')
