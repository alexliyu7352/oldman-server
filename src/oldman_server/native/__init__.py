"""The native host: a C++/Seastar HTTP/1.1 server (the _native extension, built
from native/ by scripts/build.sh), its Python side (engine) and the
application adapter that serves an app through it (host)."""


def native_server(config) -> bool:
    """Whether NATIVE_SERVER is on: True (or 1, as Sanic's environment loader
    may give it). Plain Python, so the worker manager can ask without loading
    the extension."""
    return config.get("NATIVE_SERVER") in (True, 1)
