from __future__ import annotations

import secrets
import socket
import stat

from ipaddress import ip_address
from pathlib import Path
from typing import Any

from oldman_server.constants import DEFAULT_BACKLOG



def bind_socket(
    host: str, port: int, *, backlog=DEFAULT_BACKLOG, reserve=False
) -> socket.socket:
    """Create TCP server socket.
    :param host: IPv4, IPv6 or hostname may be specified
    :param port: TCP port number
    :param backlog: Maximum number of connections to queue
    :param reserve: Only reserve the address for the native host, whose
        serving processes each listen there (reuseport_listener), instead of
        listening
    :return: socket.socket object
    """
    location = (host, port)
    # socket.share, socket.fromshare
    try:  # IP address: family must be specified for IPv6 at least
        ip = ip_address(host)
        host = str(ip)
        sock = socket.socket(
            socket.AF_INET6 if ip.version == 6 else socket.AF_INET
        )
    except ValueError:  # Hostname, may become AF_INET or AF_INET6
        sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(location)
    if reserve:
        # Bound without SO_REUSEPORT, so a taken address fails here as for a
        # listener; marked afterwards, so the serving processes can bind
        # their own listeners to it. Not listening, it gets no connections.
        #
        # Accepted limit: while no serving process listens yet (startup, a
        # restart of all workers) the reservation alone holds the address,
        # and a socket that is not listening does not keep others away from
        # it: another socket with SO_REUSEADDR (another instance, or another
        # user's program) can bind it then, and connections are refused
        # rather than queued. SO_REUSEADDR is still needed here: without it
        # the bind fails while the previous run's connections are in
        # TIME_WAIT. Holding a listening socket instead would mean one
        # socket shared by all workers (as for a unix socket). README notes.
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    else:
        sock.listen(backlog)
    sock.set_inheritable(True)
    return sock


def reuseport_listener(reservation: socket.socket, backlog=DEFAULT_BACKLOG):
    """A listener of this process at a reserved address. The kernel spreads
    the address's connections over the processes' listeners."""
    sock = socket.socket(reservation.family, socket.SOCK_STREAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        if reservation.family == socket.AF_INET6:
            sock.setsockopt(
                socket.IPPROTO_IPV6,
                socket.IPV6_V6ONLY,
                reservation.getsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY),
            )
        sock.bind(reservation.getsockname())
        sock.listen(backlog)
    except BaseException:
        sock.close()
        raise
    return sock


def worker_listener(host: str, port: int, backlog=DEFAULT_BACKLOG) -> socket.socket:
    """A worker process's own TCP listener (NATIVE_WORKER_LISTEN). With
    SO_REUSEPORT, so that a restarted worker binds the address while the one it
    replaces may still be closing."""
    try:
        family = socket.AF_INET6 if ip_address(host).version == 6 else socket.AF_INET
    except ValueError:  # a hostname
        family = socket.AF_INET
    sock = socket.socket(family)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind((host, port))
        sock.listen(backlog)
    except BaseException:
        sock.close()
        raise
    return sock


def bind_unix_socket(
    path: Path | str, *, mode=0o666, backlog=DEFAULT_BACKLOG
) -> socket.socket:
    """Create unix socket.
    :param path: filesystem path
    :param backlog: Maximum number of connections to queue
    :return: socket.socket object
    """

    # Sanitise and pre-verify socket path
    path = Path(path)
    folder = path.parent
    if not folder.is_dir():
        raise FileNotFoundError(f"Socket folder does not exist: {folder}")
    try:
        if not stat.S_ISSOCK(path.lstat().st_mode):
            raise FileExistsError(f"Existing file is not a socket: {path}")
    except FileNotFoundError:
        pass
    # Create new socket with a random temporary name
    tmp_path = path.with_name(f"{path.name}.{secrets.token_urlsafe()}")
    sock = socket.socket(socket.AF_UNIX)
    try:
        # Critical section begins (filename races)
        sock.bind(tmp_path.as_posix())
        try:
            tmp_path.chmod(mode)
            # Start listening before rename to avoid connection failures
            sock.listen(backlog)
            tmp_path.rename(path)
        except:  # noqa: E722
            try:
                tmp_path.unlink()
            finally:
                raise
    except:  # noqa: E722
        try:
            sock.close()
        finally:
            raise
    return sock


def unix_socket_mode(config) -> int:
    """config.UNIX_SOCKET_MODE as permission bits: an int (0o660) or an octal
    string ("0o660", "660"; the environment variable)."""
    mode = config.UNIX_SOCKET_MODE
    try:
        if isinstance(mode, str):
            mode = int(mode, 8)
        if type(mode) is not int or not 0 <= mode <= 0o777:
            raise ValueError
    except ValueError:
        raise ValueError(
            f"UNIX_SOCKET_MODE must be permission bits such as 0o660, "
            f"not {config.UNIX_SOCKET_MODE!r}"
        ) from None
    return mode


def remove_unix_socket(path: Path | str | None) -> None:
    """Remove dead unix socket during server exit."""
    if not path:
        return
    try:
        path = Path(path)
        if stat.S_ISSOCK(path.lstat().st_mode):
            # Is it actually dead (doesn't belong to a new server instance)?
            with socket.socket(socket.AF_UNIX) as testsock:
                try:
                    testsock.connect(path.as_posix())
                except ConnectionRefusedError:
                    path.unlink()
    except FileNotFoundError:
        pass


def configure_socket(
    server_settings: dict[str, Any], *, reserve=False, mode=0o666
) -> socket.SocketType | None:
    # Create a listening socket or use the one in settings. With reserve (the
    # native host), a TCP address is only reserved: each serving process
    # listens there itself (reuseport_listener); a unix socket is passed to
    # the serving processes, which all accept on it (unix sockets have no
    # SO_REUSEPORT), instead of each binding and renaming its own over the path.
    sock = server_settings.get("sock")
    unix = server_settings["unix"]
    backlog = server_settings["backlog"]
    if unix:
        unix = Path(unix).absolute()
        sock = bind_unix_socket(unix, mode=mode, backlog=backlog)
        server_settings["unix"] = unix
        if reserve:
            # Not inheritable: workers get it from multiprocessing, not exec.
            server_settings["sock"] = sock
    if sock is None:
        sock = bind_socket(
            server_settings["host"],
            server_settings["port"],
            backlog=backlog,
            reserve=reserve,
        )
        sock.set_inheritable(True)
        server_settings["sock"] = sock
        # Our reservation, not a socket the application passed (which may
        # also be bound, not listening, with SO_REUSEPORT): the serving
        # processes open their listeners at its address.
        server_settings["sock_reserved"] = reserve
        server_settings["host"] = None
        server_settings["port"] = None
    return sock
