"""CPU groups, address reservations and accept turns for native worker processes."""
import ctypes
import errno
import multiprocessing
import socket

import pytest

from oldman_server.config import Config
from oldman_server.native import accept, cpus, listen
from oldman_server.server.socket import bind_socket, reuseport_listener


def listening(sock):
    return bool(sock.getsockopt(socket.SOL_SOCKET, socket.SO_ACCEPTCONN))


@pytest.fixture
def available(monkeypatch):
    def set_cpus(count):
        monkeypatch.setattr(cpus, "_available", lambda: list(range(count)))
    return set_cpus


def config(**settings):
    result = Config()
    result.update(settings)
    return result


def test_groups_of_a_python_cpu_and_two_shards(available):
    available(16)
    assert cpus.worker_groups(config(), 2)[:2] == [(0, 1, 2), (3, 4, 5)]
    assert len(cpus.worker_groups(config(), 2)) == 5
    assert cpus.fast_workers(config()) == 5


def test_fewer_shards_when_the_workers_need_the_cpus(available):
    available(8)
    # 8 CPUs for 4 workers: one shard each.
    assert cpus.worker_groups(config(), 4) == [(0, 1), (2, 3), (4, 5), (6, 7)]
    assert cpus.worker_groups(config(NATIVE_SHARDS=3), 2) == [(0, 1, 2, 3), (4, 5, 6, 7)]
    assert cpus.fast_workers(config(NATIVE_SHARDS=1)) == 4


def test_too_few_cpus_for_a_group(available):
    available(1)
    assert cpus.worker_groups(config(), 2) == [(0, 0)]  # Python shares its CPU
    available(2)
    assert cpus.worker_groups(config(NATIVE_SHARDS=2), 1) == [(0, 1)]


def test_explicit_groups_and_shard_cpus():
    assert cpus.worker_groups(config(NATIVE_WORKER_CPUS="0,1,2; 4,5"), 2) == [(0, 1, 2), (4, 5)]
    assert cpus.worker_groups(config(NATIVE_WORKER_CPUS=[[3, 4]]), 2) == [(3, 4)]
    with pytest.raises(ValueError, match="at least one shard"):
        cpus.worker_groups(config(NATIVE_WORKER_CPUS="0;1,2"), 2)
    with pytest.raises(ValueError, match="NATIVE_SHARD_CPUS"):
        cpus.worker_groups(config(NATIVE_SHARD_CPUS="1,2"), 2)


def test_a_worker_takes_its_group_by_number(monkeypatch):
    planned = config(NATIVE_WORKER_CPUS=[(0, 1, 2), (3, 4, 5)])
    monkeypatch.delenv("OLDMAN_SERVER_WORKER_NAME", raising=False)
    assert cpus.worker_group(planned) is None  # not a worker process
    for name, group in (("Oldman-Server-0-0", (0, 1, 2)), ("Oldman-Server-1-0", (3, 4, 5)),
                        ("Oldman-Server-2-0", (0, 1, 2))):  # scaled beyond the groups
        monkeypatch.setenv("OLDMAN_SERVER_WORKER_NAME", name)
        assert cpus.worker_group(planned) == group
    monkeypatch.setenv("OLDMAN_SERVER_WORKER_NAME", "Oldman-Reloader-0")
    assert cpus.worker_group(planned) is None
    monkeypatch.setenv("OLDMAN_SERVER_WORKER_NAME", "Oldman-Server-0-0")
    assert cpus.worker_group(config()) is None  # nothing planned


def test_a_worker_takes_its_accept_turn_by_number(monkeypatch):
    table = accept.counts(multiprocessing.get_context("spawn"))
    assert ctypes.sizeof(table) == accept.SLOTS * 64  # an AcceptSlot line per worker
    monkeypatch.delenv("OLDMAN_SERVER_WORKER_NAME", raising=False)
    assert accept.turn(table) == {}  # not a worker process
    monkeypatch.setenv("OLDMAN_SERVER_WORKER_NAME", "Oldman-Server-3-0")
    assert accept.turn(None) == {}  # no table: a single process
    assert accept.turn(table) == {"accept_counts": ctypes.addressof(table),
                                  "accept_slots": accept.SLOTS, "accept_slot": 3}
    monkeypatch.setenv("OLDMAN_SERVER_WORKER_NAME", f"Oldman-Server-{accept.SLOTS}-0")
    assert accept.turn(table) == {}  # beyond the table: accepts whenever it can


def test_worker_listen_addresses():
    assert listen.address("/run/app/{n}.sock", 2) == ("unix", "/run/app/2.sock")
    assert listen.address("unix:/run/w-{n}.sock", 0) == ("unix", "/run/w-0.sock")
    assert listen.address("127.0.0.1:81{n:02d}", 3) == ("tcp", "127.0.0.1", 8103)
    assert listen.address("[::1]:900{n}", 1) == ("tcp", "::1", 9001)
    assert listen.address("localhost:900{n}", 1) == ("tcp", "localhost", 9001)
    # Without {n} the workers would collide; a port must fit.
    for template in ("/run/app.sock", "127.0.0.1:8100", "app{n}", "127.0.0.1:9{n}9999", None):
        with pytest.raises(ValueError, match="NATIVE_WORKER_LISTEN"):
            listen.address(template, 1)


def test_a_worker_takes_its_own_address_by_number(monkeypatch):
    monkeypatch.delenv("OLDMAN_SERVER_WORKER_NAME", raising=False)
    assert listen.worker_address(config()) is None
    planned = config(NATIVE_WORKER_LISTEN="/run/w{n}.sock")
    assert listen.worker_address(planned) == ("unix", "/run/w0.sock")  # a single process
    monkeypatch.setenv("OLDMAN_SERVER_WORKER_NAME", "Oldman-Server-2-0")
    assert listen.worker_address(planned) == ("unix", "/run/w2.sock")


def test_a_reserved_address_takes_listeners_and_no_connections():
    reservation = bind_socket("127.0.0.1", 0, reserve=True)
    listeners = []
    try:
        assert not listening(reservation)
        assert reservation.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT)
        address = reservation.getsockname()
        listeners = [reuseport_listener(reservation) for _ in range(2)]
        assert all(listening(listener) for listener in listeners)
        # Taken, as by any listener: another server (Sanic's bind_socket, or
        # another reservation) fails to bind it.
        with pytest.raises(OSError) as error:
            bind_socket(*address, reserve=True)
        assert error.value.errno == errno.EADDRINUSE
        accepted = 0
        for _ in range(8):
            with socket.create_connection(address):
                for listener in listeners:
                    listener.settimeout(0)
                    try:
                        listener.accept()[0].close()
                        accepted += 1
                    except BlockingIOError:
                        pass
        assert accepted == 8  # all on the listeners, none on the reservation
    finally:
        for sock in (reservation, *listeners):
            sock.close()


def test_only_a_reservation_is_one():
    # configure_socket says when the socket is its reservation; a socket the
    # application passes is served as it is, whatever its options.
    import tempfile
    from oldman_server.server.socket import configure_socket
    settings = {"sock": None, "unix": None, "backlog": 100, "host": "127.0.0.1", "port": 0}
    with configure_socket(settings, reserve=True):
        assert settings["sock_reserved"] is True
    settings = {"sock": None, "unix": None, "backlog": 100, "host": "127.0.0.1", "port": 0}
    with configure_socket(settings):
        assert settings["sock_reserved"] is False
    with tempfile.TemporaryDirectory() as directory:
        settings = {"sock": None, "unix": f"{directory}/a.sock", "backlog": 100,
                    "host": None, "port": None}
        with configure_socket(settings, reserve=True):
            assert "sock_reserved" not in settings
def test_a_shared_unix_socket_is_not_inherited_across_exec():
    # The workers get it from multiprocessing (spawn) or with the process
    # (fork); a program the application starts must not keep it listening,
    # or the path is never removed and clients wait on it.
    import tempfile
    from oldman_server.server.socket import configure_socket
    with tempfile.TemporaryDirectory() as directory:
        settings = {"sock": None, "unix": f"{directory}/app.sock", "backlog": 100,
                    "host": None, "port": None}
        sock = configure_socket(settings, reserve=True)
        with sock:
            assert settings["sock"] is sock
            assert not sock.get_inheritable()


@pytest.mark.parametrize("value, expected", [("3", (3,)), ("0", (0,)), ("1,2", (1, 2)), ("5, 6", (5, 6))])
def test_shard_cpus_from_the_environment(monkeypatch, value, expected):
    # The environment loader turns "3" into the int 3 (and "0" into 0), as for
    # any setting: one CPU must still be a CPU list.
    from oldman_server.native.host import _shard_cpus
    monkeypatch.setenv("OLDMAN_SERVER_NATIVE_SHARD_CPUS", value)
    monkeypatch.delenv("OLDMAN_SERVER_WORKER_NAME", raising=False)
    assert _shard_cpus(Config()) == expected
    with pytest.raises(ValueError, match="NATIVE_SHARD_CPUS"):
        cpus.worker_groups(Config(), 2)


def test_worker_listen_templates_fail_in_the_main_process():
    # Every worker's address is checked before the workers start: a port
    # that only overflows for worker 6, a unix path too long once bound
    # under its temporary name, an unknown field.
    from oldman_server import OldmanServer
    for template, workers in (("127.0.0.1:6553{n}", 8), ("/" + "x" * 64 + "/{n}.sock", 2),
                              ("/run/{n}/{other}.sock", 2)):
        app = OldmanServer(f"listen_{workers}_{len(template)}", configure_logging=False)
        app.config.NATIVE_SERVER = True
        app.config.NATIVE_WORKER_LISTEN = template
        app.state.workers = workers
        with pytest.raises(ValueError, match="NATIVE_WORKER_LISTEN"):
            OldmanServer._native_worker_config(app)
