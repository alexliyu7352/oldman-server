"""NATIVE_WORKER_LISTEN: an address of each serving process's own, besides the
run's shared one, so that a proxy in front balances requests over the
processes.

Behind a proxy that keeps its upstream connections (nginx and OpenResty do by
default now), which worker serves a request is the proxy's choice of
connection: it takes the most recently freed one first, so under moderate load
most requests go to a few connections and the workers they reached. Listing
every worker's own address as an upstream server lets the proxy balance each
request (nginx: least_conn).

The setting is a template formatted with the worker number n (0, 1, ...; a
single serving process is 0): a unix socket path ("/run/app/{n}.sock", or with
a "unix:" prefix) or host:port ("127.0.0.1:81{n:02d}" for ports 8100, 8101,
...).

Plain Python: the main process checks the setting without the extension.
"""
import os
from string import Formatter

from oldman_server.native.cpus import worker_number

# sockaddr_un holds 107 path bytes; Sanic binds the path with a suffix of 44
# (".", then a temporary name) before renaming it into place.
_UNIX_PATH_MAX = 107 - 44


def address(template, number):
    """("unix", path) or ("tcp", host, port) of worker `number`."""
    if not isinstance(template, str) or not any(
        field == "n" for _, field, _, _ in Formatter().parse(template)
    ):
        raise ValueError(
            f"NATIVE_WORKER_LISTEN must contain {{n}} (the worker number): {template!r}"
        )
    try:
        formatted = template.format(n=number)
    except (KeyError, IndexError, ValueError) as error:
        raise ValueError(
            f"NATIVE_WORKER_LISTEN has a field other than {{n}}: {template!r} ({error!r})"
        ) from None
    if formatted.startswith("unix:"):
        formatted = formatted[len("unix:"):]
    if formatted.startswith("/"):
        if len(os.fsencode(formatted)) > _UNIX_PATH_MAX:
            raise ValueError(
                f"NATIVE_WORKER_LISTEN gives a unix socket path longer than "
                f"{_UNIX_PATH_MAX} bytes: {formatted!r}"
            )
        return ("unix", formatted)
    host, separator, port = formatted.rpartition(":")
    if not separator or not host or not port.isdigit() or not 0 < int(port) < 65536:
        raise ValueError(
            f"NATIVE_WORKER_LISTEN is neither a unix socket path nor host:port: {formatted!r}"
        )
    return ("tcp", host.removeprefix("[").removesuffix("]"), int(port))


def worker_address(config):
    """This serving process's own address, or None without NATIVE_WORKER_LISTEN."""
    template = config.get("NATIVE_WORKER_LISTEN")
    if not template:
        return None
    return address(template, worker_number() or 0)
