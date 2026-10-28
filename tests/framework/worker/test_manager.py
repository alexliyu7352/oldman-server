import multiprocessing
import signal
import sys
import threading
import time

from logging import ERROR, INFO
from multiprocessing.managers import SyncManager
from signal import SIGINT, SIGTERM
from unittest.mock import Mock, call, patch

import pytest

from oldman_server.compat import OS_IS_WINDOWS
from oldman_server.exceptions import ServerKilled
from oldman_server.worker.constants import RestartOrder
from oldman_server.worker.manager import WorkerManager
from oldman_server.worker.process import Worker


if not OS_IS_WINDOWS:
    from signal import SIGKILL
else:
    SIGKILL = SIGINT

# Kills by process group: POSIX only.
posix_kill = pytest.mark.skipif(OS_IS_WINDOWS, reason="no process groups on Windows")


def fake_serve(): ...


@pytest.fixture
def manager() -> WorkerManager:
    p1 = Mock()
    p1.pid = 1234
    context = Mock()
    context.Process.return_value = p1
    pub = Mock()
    manager = WorkerManager(1, fake_serve, {}, context, (pub, Mock()), {})
    return manager


def stop_after(manager: WorkerManager) -> Mock:
    """A Mock for one of the manager's methods that requests a shutdown, as a
    stop signal arriving while it runs does: monitor() then shuts down."""
    manager.shutdown = Mock(  # type: ignore
        side_effect=lambda: setattr(manager, "_shutting_down", True)
    )
    return Mock(side_effect=lambda *_, **__: setattr(manager, "_stop_requested", True))


def test_manager_no_workers():
    message = "Cannot serve with no workers"
    with pytest.raises(RuntimeError, match=message):
        WorkerManager(0, fake_serve, {}, Mock(), (Mock(), Mock()), {})


@patch("oldman_server.worker.process.os")
def test_terminate(os_mock: Mock):
    process = Mock()
    process.pid = 1234
    context = Mock()
    context.Process.return_value = process
    manager = WorkerManager(1, fake_serve, {}, context, (Mock(), Mock()), {})
    manager.terminate()
    os_mock.kill.assert_called_once_with(1234, SIGINT)


@patch("oldman_server.worker.process.os")
def test_shutown(os_mock: Mock):
    process = Mock()
    process.pid = 1234
    process.is_alive.return_value = True
    context = Mock()
    context.Process.return_value = process
    manager = WorkerManager(1, fake_serve, {}, context, (Mock(), Mock()), {})
    manager.shutdown()
    os_mock.kill.assert_called_once_with(1234, SIGINT)


@posix_kill
@patch("oldman_server.worker.manager.os")
def test_kill(os_mock: Mock):
    process = Mock()
    process.pid = 1234
    context = Mock()
    context.Process.return_value = process
    os_mock.getpgid.return_value = 5678
    manager = WorkerManager(1, fake_serve, {}, context, (Mock(), Mock()), {})
    with pytest.raises(ServerKilled):
        manager.kill()
    os_mock.getpgid.assert_called_once_with(1234)
    os_mock.killpg.assert_called_once_with(5678, SIGKILL)


@posix_kill
@patch("oldman_server.worker.manager.os")
def test_kill_spares_the_main_process_group(os_mock: Mock):
    # Workers in the main process's group: killpg would kill main too.
    process = Mock()
    process.pid = 1234
    context = Mock()
    context.Process.return_value = process
    os_mock.getpgid.return_value = os_mock.getpgrp.return_value = 42
    manager = WorkerManager(1, fake_serve, {}, context, (Mock(), Mock()), {})
    with pytest.raises(ServerKilled):
        manager.kill()
    os_mock.kill.assert_called_once_with(1234, SIGKILL)
    os_mock.killpg.assert_not_called()


@patch("oldman_server.worker.manager.OS_IS_WINDOWS", True)
@patch("oldman_server.worker.manager.os")
def test_kill_on_windows(os_mock: Mock):
    # No process groups: each worker is terminated by itself.
    process = Mock()
    process.pid = 1234
    context = Mock()
    context.Process.return_value = process
    manager = WorkerManager(1, fake_serve, {}, context, (Mock(), Mock()), {})
    with pytest.raises(ServerKilled):
        manager.kill()
    os_mock.kill.assert_called_once_with(1234, SIGTERM)
    os_mock.killpg.assert_not_called()


@posix_kill
@patch("oldman_server.worker.process.os")
@patch("oldman_server.worker.manager.os")
def test_shutdown_signal_send_kill(
    manager_os_mock: Mock, process_os_mock: Mock
):
    process = Mock()
    process.pid = 1234
    context = Mock()
    context.Process.return_value = process
    manager_os_mock.getpgid.return_value = 5678
    manager = WorkerManager(1, fake_serve, {}, context, (Mock(), Mock()), {})
    assert manager._shutting_down is False
    manager.shutdown_signal(SIGINT, None)
    # The signal only requests the shutdown: the main thread begins it.
    process_os_mock.kill.assert_not_called()
    assert manager._shutdown_if_requested() is True
    process_os_mock.kill.assert_called_once_with(1234, SIGINT)
    manager.shutdown_signal(SIGINT, None)
    manager_os_mock.getpgid.assert_called_once_with(1234)
    manager_os_mock.killpg.assert_called_once_with(5678, SIGKILL)


def test_restart_all():
    p1 = Mock()
    p2 = Mock()
    context = Mock()
    context.Process.side_effect = [p1, p2, p1, p2]
    manager = WorkerManager(2, fake_serve, {}, context, (Mock(), Mock()), {})
    assert len(list(manager.transient_processes))
    manager.restart()
    p1.terminate.assert_called_once()
    p2.terminate.assert_called_once()
    context.Process.assert_has_calls(
        [
            call(
                name="Oldman-Server-0-0",
                target=fake_serve,
                kwargs={},
                daemon=True,
            ),
            call(
                name="Oldman-Server-1-0",
                target=fake_serve,
                kwargs={},
                daemon=True,
            ),
            call(
                name="Oldman-Server-0-0",
                target=fake_serve,
                kwargs={},
                daemon=True,
            ),
            call(
                name="Oldman-Server-1-0",
                target=fake_serve,
                kwargs={},
                daemon=True,
            ),
        ]
    )


@pytest.mark.parametrize("zero_downtime", (False, True))
def test_monitor_all(zero_downtime):
    p1 = Mock()
    p2 = Mock()
    sub = Mock()
    incoming = (
        "__ALL_PROCESSES__::STARTUP_FIRST"
        if zero_downtime
        else "__ALL_PROCESSES__:"
    )
    sub.recv.side_effect = [incoming]
    context = Mock()
    context.Process.side_effect = [p1, p2]
    manager = WorkerManager(2, fake_serve, {}, context, (Mock(), sub), {})
    manager.restart = stop_after(manager)  # type: ignore
    manager.wait_for_ack = Mock()  # type: ignore
    manager.monitor()

    restart_order = (
        RestartOrder.STARTUP_FIRST
        if zero_downtime
        else RestartOrder.SHUTDOWN_FIRST
    )
    manager.restart.assert_called_once_with(
        process_names=None,
        reloaded_files="",
        restart_order=restart_order,
    )


@pytest.mark.parametrize("zero_downtime", (False, True))
def test_monitor_all_with_files(zero_downtime):
    p1 = Mock()
    p2 = Mock()
    sub = Mock()
    incoming = (
        "__ALL_PROCESSES__:foo,bar:STARTUP_FIRST"
        if zero_downtime
        else "__ALL_PROCESSES__:foo,bar"
    )
    sub.recv.side_effect = [incoming]
    context = Mock()
    context.Process.side_effect = [p1, p2]
    manager = WorkerManager(2, fake_serve, {}, context, (Mock(), sub), {})
    manager.restart = stop_after(manager)  # type: ignore
    manager.wait_for_ack = Mock()  # type: ignore
    manager.monitor()

    restart_order = (
        RestartOrder.STARTUP_FIRST
        if zero_downtime
        else RestartOrder.SHUTDOWN_FIRST
    )
    manager.restart.assert_called_once_with(
        process_names=None,
        reloaded_files="foo,bar",
        restart_order=restart_order,
    )


@pytest.mark.parametrize("zero_downtime", (False, True))
def test_monitor_one_process(zero_downtime):
    p1 = Mock()
    p1.name = "Testing"
    p2 = Mock()
    sub = Mock()
    incoming = (
        f"{p1.name}:foo,bar:STARTUP_FIRST"
        if zero_downtime
        else f"{p1.name}:foo,bar"
    )
    sub.recv.side_effect = [incoming]
    context = Mock()
    context.Process.side_effect = [p1, p2]
    manager = WorkerManager(2, fake_serve, {}, context, (Mock(), sub), {})
    manager.restart = stop_after(manager)  # type: ignore
    manager.wait_for_ack = Mock()  # type: ignore
    manager.monitor()

    restart_order = (
        RestartOrder.STARTUP_FIRST
        if zero_downtime
        else RestartOrder.SHUTDOWN_FIRST
    )
    manager.restart.assert_called_once_with(
        process_names=[p1.name],
        reloaded_files="foo,bar",
        restart_order=restart_order,
    )


def test_shutdown_signal():
    pub = Mock()
    manager = WorkerManager(1, fake_serve, {}, Mock(), (pub, Mock()), {})
    manager.shutdown = Mock()  # type: ignore

    manager.shutdown_signal(SIGINT, None)
    # Only a request: no message, nothing stopped from the handler.
    pub.send.assert_not_called()
    manager.shutdown.assert_not_called()
    manager._shutdown_if_requested()
    manager.shutdown.assert_called_once_with()


@patch("oldman_server.worker.process.os")
def test_a_stop_signal_while_a_restart_is_handled_stops_the_new_process(process_os_mock):
    # The signal comes while the main thread restarts a worker: the process
    # the restart starts is stopped too, and the messages after it (here a
    # scale-up) create no process.
    old, new = Mock(pid=1), Mock(pid=2)
    context = Mock()
    context.Process.side_effect = [old, new]
    sub = Mock()
    sub.recv.side_effect = ["Oldman-Server-0-0", "__SCALE__:3"]
    manager = WorkerManager(1, fake_serve, {}, context, (Mock(), sub), {})
    manager.wait_for_ack = Mock()  # type: ignore
    restart = manager.restart

    def restart_with_a_stop_signal(**kwargs):
        manager.shutdown_signal(SIGTERM, None)
        restart(**kwargs)

    manager.restart = restart_with_a_stop_signal  # type: ignore

    manager.monitor()

    old.terminate.assert_called_once_with()  # by the restart
    new.start.assert_called_once_with()
    process_os_mock.kill.assert_called_once_with(2, SIGINT)  # by the shutdown
    assert context.Process.call_count == 2  # no scale-up after the stop
    assert manager.num_server == 1


@patch("oldman_server.worker.process.os")
def test_no_process_is_started_after_a_stop_signal(process_os_mock):
    process = Mock(pid=None)
    context = Mock()
    context.Process.return_value = process
    manager = WorkerManager(2, fake_serve, {}, context, (Mock(), Mock()), {})
    manager.shutdown_signal(SIGTERM, None)
    manager.start()
    process.start.assert_not_called()


@patch("oldman_server.worker.process.os")
def test_shutdown_stops_a_zero_downtime_restart_s_previous_process(process_os_mock, monkeypatch):
    # The restart keeps the previous process until the new one acknowledges;
    # a shutdown meanwhile stops it, and the restart's thread then leaves it.
    old, new = Mock(pid=1), Mock(pid=2)
    context = Mock()
    context.Process.side_effect = [old, new]
    manager = WorkerManager(1, fake_serve, {}, context, (Mock(), Mock()), {})
    waiting = []
    monkeypatch.setattr(threading.Thread, "start", lambda thread: waiting.append(thread))
    (process,) = manager.processes
    process.restart(restart_order=RestartOrder.STARTUP_FIRST)
    old.terminate.assert_not_called()

    manager.shutdown()

    old.terminate.assert_called_once_with()
    process_os_mock.kill.assert_called_once_with(2, SIGINT)
    (thread,) = waiting
    thread.run()  # returns at once: the shutdown took the previous process
    old.terminate.assert_called_once_with()


class Slow:
    def reply(self, seconds):
        time.sleep(seconds)
        return "slow reply"


class SlowManager(SyncManager):
    pass


SlowManager.register("Slow", Slow)


@pytest.mark.skipif(not hasattr(signal, "pthread_kill"), reason="POSIX signals")
@pytest.mark.skipif(sys.platform == "darwin", reason="no fork for the manager process on macOS")
@patch("oldman_server.worker.process.os")
def test_a_shutdown_signal_during_a_shared_state_call_leaves_it_alone(process_os_mock):
    # The signal comes while the main thread waits for the shared state's
    # manager (as _sync_states does); the handler's own state updates must not
    # take that call's reply on the main thread's connection.
    shared = SlowManager(ctx=multiprocessing.get_context("fork"))
    shared.start()
    try:
        state, slow = shared.dict(), shared.Slow()
        process = Mock()
        process.pid = 1234
        context = Mock()
        context.Process.return_value = process
        manager = WorkerManager(1, fake_serve, {}, context, (Mock(), Mock()), state)
        (worker_process,) = manager.processes
        name = worker_process.name
        previous = signal.signal(SIGTERM, manager.shutdown_signal)
        timer = threading.Timer(.1, signal.pthread_kill, (threading.main_thread().ident, SIGTERM))
        try:
            timer.start()
            assert slow.reply(.5) == "slow reply"
        finally:
            timer.join()
            signal.signal(SIGTERM, previous)
        # The handler only requested the shutdown; the main thread begins it.
        process_os_mock.kill.assert_not_called()
        assert manager._shutdown_if_requested()
        assert state[name]["state"] == "TERMINATED"
        process_os_mock.kill.assert_called_once_with(1234, SIGINT)
    finally:
        shared.shutdown()


def test_shutdown_servers(caplog):
    p1 = Mock()
    p1.pid = 1234
    context = Mock()
    context.Process.side_effect = [p1]
    pub = Mock()
    manager = WorkerManager(1, fake_serve, {}, context, (pub, Mock()), {})

    with patch("os.kill") as kill:
        with caplog.at_level(ERROR):
            caplog.clear()
            manager.shutdown_server()

            kill.assert_called_once_with(1234, SIGINT)
            kill.reset_mock()

            sanic_errors = [
                r for r in caplog.record_tuples if r[0].startswith("oldman_server")
            ]
            assert not sanic_errors

            manager.shutdown_server()

            kill.assert_not_called()

            assert (
                "oldman_server.error",
                ERROR,
                "Server shutdown failed because a server was not found.",
            ) in caplog.record_tuples


def test_shutdown_servers_named():
    p1 = Mock()
    p1.pid = 1234
    p2 = Mock()
    p2.pid = 6543
    context = Mock()
    context.Process.side_effect = [p1, p2]
    pub = Mock()
    manager = WorkerManager(2, fake_serve, {}, context, (pub, Mock()), {})

    with patch("os.kill") as kill:
        with pytest.raises(KeyError):
            manager.shutdown_server("foo")
        manager.shutdown_server("Server-1")

        kill.assert_called_once_with(6543, SIGINT)


def test_scale(caplog):
    p1 = Mock()
    p1.pid = 1234
    p2 = Mock()
    p2.pid = 3456
    p3 = Mock()
    p3.pid = 5678
    context = Mock()
    context.Process.side_effect = [p1, p2, p3]
    pub = Mock()
    manager = WorkerManager(1, fake_serve, {}, context, (pub, Mock()), {})

    assert len(manager.transient) == 1

    manager.scale(3)
    assert len(manager.transient) == 3

    with patch("os.kill") as kill:
        manager.scale(2)
        assert len(manager.transient) == 2

        manager.scale(1)
        assert len(manager.transient) == 1

        kill.call_count == 2

    with caplog.at_level(INFO):
        manager.scale(1)

    assert (
        "oldman_server.root",
        INFO,
        "No change needed. There are already 1 workers.",
    ) in caplog.record_tuples

    with pytest.raises(ValueError, match=r"Cannot scale to 0 workers\."):
        manager.scale(0)


def test_manage_basic(manager: WorkerManager):
    assert len(manager.transient) == 1
    assert len(manager.durable) == 0
    manager.manage("TEST", fake_serve, kwargs={"foo": "bar"})
    assert len(manager.transient) == 1
    assert len(manager.durable) == 1

    worker_process = manager.durable["TEST"]

    assert isinstance(worker_process, Worker)
    assert worker_process.server_settings == {"foo": "bar"}
    assert worker_process.restartable is False
    assert worker_process.tracked is True
    assert worker_process.auto_start is True
    assert worker_process.num == 1


def test_manage_transient(manager: WorkerManager):
    manager.manage(
        "TEST", fake_serve, kwargs={"foo": "bar"}, workers=3, transient=True
    )
    assert len(manager.transient) == 2
    assert len(manager.durable) == 0

    worker_process = manager.transient["TEST"]

    assert isinstance(worker_process, Worker)
    assert worker_process.restartable is True
    assert worker_process.tracked is True
    assert worker_process.auto_start is True
    assert worker_process.num == 3


def test_manage_restartable(manager: WorkerManager):
    manager.manage(
        "TEST",
        fake_serve,
        kwargs={"foo": "bar"},
        restartable=True,
        auto_start=False,
    )
    assert len(manager.transient) == 1
    assert len(manager.durable) == 1

    worker_process = manager.durable["TEST"]

    assert isinstance(worker_process, Worker)
    assert worker_process.restartable is True
    assert worker_process.tracked is True
    assert worker_process.auto_start is False


def test_manage_untracked(manager: WorkerManager):
    manager.manage("TEST", fake_serve, kwargs={"foo": "bar"}, tracked=False)
    assert len(manager.transient) == 1
    assert len(manager.durable) == 1

    worker_process = manager.durable["TEST"]

    assert isinstance(worker_process, Worker)
    assert worker_process.restartable is False
    assert worker_process.tracked is False
    assert worker_process.auto_start is True


def test_manage_duplicate_ident(manager: WorkerManager):
    manager.manage("TEST", fake_serve, kwargs={"foo": "bar"})
    message = "Worker TEST already exists"
    with pytest.raises(ValueError, match=message):
        manager.manage("TEST", fake_serve, kwargs={"foo": "bar"})


def test_transient_not_restartable(manager: WorkerManager):
    message = "Cannot create a transient worker that is not restartable"
    with pytest.raises(ValueError, match=message):
        manager.manage(
            "TEST",
            fake_serve,
            kwargs={"foo": "bar"},
            transient=True,
            restartable=False,
        )


def test_remove_worker(manager: WorkerManager, caplog):
    worker = manager.manage("TEST", fake_serve, kwargs={})

    assert "Oldman-TEST-0" in worker.worker_state
    assert len(manager.transient) == 1
    assert len(manager.durable) == 1

    manager.remove_worker(worker)
    message = "Worker TEST is tracked and cannot be removed."

    assert "Oldman-TEST-0" in worker.worker_state
    assert len(manager.transient) == 1
    assert len(manager.durable) == 1
    assert ("oldman_server.error", 40, message) in caplog.record_tuples


def test_remove_untracked_worker(manager: WorkerManager, caplog):
    caplog.set_level(20)
    worker = manager.manage("TEST", fake_serve, kwargs={}, tracked=False)
    worker.has_alive_processes = Mock(return_value=True)

    assert "Oldman-TEST-0" in worker.worker_state
    assert len(manager.transient) == 1
    assert len(manager.durable) == 1

    manager.remove_worker(worker)
    message = "Worker TEST has alive processes and cannot be removed."

    assert "Oldman-TEST-0" in worker.worker_state
    assert len(manager.transient) == 1
    assert len(manager.durable) == 1
    assert ("oldman_server.error", 40, message) in caplog.record_tuples

    worker.has_alive_processes = Mock(return_value=False)
    manager.remove_worker(worker)
    message = "Removed worker TEST"

    assert "Oldman-TEST-0" not in worker.worker_state
    assert len(manager.transient) == 1
    assert len(manager.durable) == 0
    assert ("oldman_server.root", 20, message) in caplog.record_tuples


def test_a_new_server_worker_takes_the_lowest_free_number(monkeypatch):
    # The native host maps the worker number to a CPU group, an accept turn
    # and NATIVE_WORKER_LISTEN's {n}: after a scale down and up, the new
    # worker takes the free number, not the next of an ever-growing count
    # (which left a CPU group idle and moved the addresses).
    from oldman_server.native import cpus

    context = Mock()
    context.Process.side_effect = [Mock(pid=pid) for pid in range(100, 110)]
    manager = WorkerManager(3, fake_serve, {}, context, (Mock(), Mock()), {})
    assert sorted(worker.slot for worker in manager.transient.values()) == [0, 1, 2]
    with patch("os.kill"):
        manager.shutdown_server("Server-1")
    worker = manager.create_server()
    assert worker.name == "Server-3" and worker.slot == 1
    (process,) = worker.processes
    # The process learns its number from the environment it is started with.
    seen = {}
    context.Process.side_effect = None
    context.Process.return_value.start.side_effect = lambda: seen.update(
        number=cpus.worker_number())
    monkeypatch.setenv("OLDMAN_SERVER_WORKER_NAME", "")
    worker2 = manager.create_server()
    (process2,) = worker2.processes
    process2.start()
    assert worker2.slot == 3 and seen == {"number": 3}
