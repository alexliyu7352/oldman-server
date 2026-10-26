"""The native host: a C++ HTTP/1.1 server on Asio (the _native extension, built
from native/ by scripts/build.sh), its Python side (engine) and the
application adapter that serves an app through it (host)."""


from importlib.util import find_spec
import sys

# The native host is built for Linux. Elsewhere (macOS, Windows: development,
# small local services) the asyncio server serves, NATIVE_SERVER or not.
NATIVE_PLATFORM = sys.platform.startswith("linux")
_noted = False
_extension = None  # whether the extension is installed, once looked up


def _note(message, *args):
    global _noted
    if not _noted:
        _noted = True
        from oldman_server.log import error_logger
        error_logger.warning(message, *args)


def native_server(config) -> bool:
    """Whether the native host serves. NATIVE_SERVER unset (None, the default):
    on Linux when the extension is installed (without it, noted once). On
    (True, or 1 as Sanic's environment loader may give it): on Linux, where
    starting without the extension then fails; elsewhere noted once. Off: no.
    Elsewhere the asyncio server serves. Plain Python, so the worker manager
    can ask without loading the extension."""
    global _extension
    setting = config.get("NATIVE_SERVER")
    if setting is None:
        if not NATIVE_PLATFORM:
            return False
        if _extension is None:
            _extension = find_spec("oldman_server.native._native") is not None
        if not _extension:
            _note("the native host extension (oldman_server.native._native) is not "
                  "installed: the asyncio server serves (NATIVE_SERVER=false says "
                  "so without this warning)")
        return _extension
    if setting not in (True, 1):
        return False
    if not NATIVE_PLATFORM:
        _note("NATIVE_SERVER: the native host runs on Linux only; on %s the "
              "asyncio server serves", sys.platform)
        return False
    return True
