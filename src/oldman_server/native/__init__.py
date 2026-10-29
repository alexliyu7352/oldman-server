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


def native_cache(seconds: float):
    """Let the native host keep this handler's responses for `seconds` (a
    decorator): a complete 200 to a GET, without Set-Cookie or Vary, that a
    shared cache may store (Cache-Control without private, no-store or
    no-cache), answers GET requests for the same target (path and query)
    until then, written by the host without running Python: no middleware
    (authentication included), signal or handler, and the request headers are
    not looked at, except that requests with Cookie or Authorization are
    neither answered from it nor kept. For public content only. Each shard
    (each worker process in the same-thread mode) keeps its own. The asyncio
    server ignores it.

    Examples:
        ```python
        @app.get("/news")
        @native_cache(1.0)
        async def news(request):
            ...
        ```
    """
    if (isinstance(seconds, bool) or not isinstance(seconds, (int, float))
            or not 0.001 <= seconds <= 3600):
        raise ValueError(f"native_cache takes 0.001 to 3600 seconds: {seconds!r}")

    def mark(handler):
        handler.__native_cache__ = seconds
        return handler
    return mark


def native_server(config) -> bool:
    """Whether the native host serves. NATIVE_SERVER unset (None, the default):
    on Linux when the extension is installed (without it, noted once). On
    (True, or 1 as the framework's environment loader may give it): on Linux, where
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
