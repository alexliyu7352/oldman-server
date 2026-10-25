"""The native host: a C++ HTTP/1.1 server on Asio (the _native extension, built
from native/ by scripts/build.sh), its Python side (engine) and the
application adapter that serves an app through it (host)."""


import sys

# The native host is built for Linux. Elsewhere (macOS, Windows: development,
# small local services) the asyncio server serves, NATIVE_SERVER or not.
NATIVE_PLATFORM = sys.platform.startswith("linux")
_noted = False


def native_server(config) -> bool:
    """Whether the native host serves: NATIVE_SERVER is on (True, or 1 as
    Sanic's environment loader may give it) and this is Linux; elsewhere the
    setting is noted once and the asyncio server serves. Plain Python, so the
    worker manager can ask without loading the extension."""
    if config.get("NATIVE_SERVER") not in (True, 1):
        return False
    if not NATIVE_PLATFORM:
        global _noted
        if not _noted:
            _noted = True
            from oldman_server.log import error_logger
            error_logger.warning(
                "NATIVE_SERVER: the native host runs on Linux only; on %s the "
                "asyncio server serves", sys.platform)
        return False
    return True
