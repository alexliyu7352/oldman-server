"""Small exact-route HTTP application API backed by the Seastar runtime.

Handlers execute as ordinary asyncio Tasks. Request target and headers retain the
HTTP wire's Latin-1 bytes; path, query_string and header() decode as Latin-1.
"""
from dataclasses import dataclass, replace
import inspect
import json

from oldman_server.native.engine import ApplicationRequest as Request, Engine


_METHODS = frozenset({"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"})
_RESERVED = frozenset({"native", "hybrid", "copy", "policy", "python", "reference"})


@dataclass(frozen=True, slots=True)
class Response:
    body: bytes | str = b""
    status: int = 200
    headers: tuple[tuple[str, str], ...] = ()


def json_response(value, *, status=200, headers=()):
    body = json.dumps(value, separators=(",", ":")).encode("utf-8")
    pairs = _header_pairs(headers)
    if not any(k.lower() == "content-type" for k, _ in pairs):
        pairs += (("Content-Type", "application/json"),)
    return Response(body, status, pairs)


def _header_pairs(headers):
    if hasattr(headers, "items"):
        headers = headers.items()
    return tuple(tuple(pair) for pair in headers)


class App:
    def __init__(self, *, native_responses=False):
        self._routes = {}
        self._implicit_head = set()
        self._started = False
        self._native_responses = native_responses

    def route(self, path, *, methods=("GET",)):
        if self._started:
            raise RuntimeError("routes must be registered before startup")
        if (not isinstance(path, str) or not path.startswith("/") or "?" in path
                or "#" in path or any(not 32 < ord(ch) < 127 for ch in path)
                or path.strip("/").split("/", 1)[0] in _RESERVED):
            raise ValueError("invalid or reserved application path")
        if (isinstance(methods, str) or not methods
                or any(type(m) is not str or m not in _METHODS for m in methods)):
            raise ValueError("invalid application route method")
        methods = tuple(methods)
        if len(set(methods)) != len(methods):
            raise ValueError("duplicate route method")

        def register(handler):
            if self._started:
                raise RuntimeError("routes must be registered before startup")
            if not callable(handler):
                raise TypeError("route handler must be callable")
            for method in methods:
                key = (method, path)
                if key in self._routes and key not in self._implicit_head:
                    raise ValueError("duplicate application route")
            for method in methods:
                key = (method, path)
                self._routes[key] = handler
                self._implicit_head.discard(key)
            if "GET" in methods and "HEAD" not in methods and ("HEAD", path) not in self._routes:
                self._routes[("HEAD", path)] = handler
                self._implicit_head.add(("HEAD", path))
            return handler

        return register

    def get(self, path):
        return self.route(path, methods=("GET",))

    def post(self, path):
        return self.route(path, methods=("POST",))

    async def _dispatch(self, request: Request):
        handler = self._routes.get((request.method, request.path))
        if handler is None:
            return 404, b"", ()
        result = handler(request)
        # Awaitables include actual coroutine handlers, Futures, and Task values.
        if inspect.isawaitable(result):
            result = await result
        if self._native_responses and isinstance(result, (dict, str, bytes)):
            return result
        if isinstance(result, dict):
            # Keep business serialization per request, without constructing and
            # then unpacking an intermediate Response for this common return type.
            return (200, json.dumps(result, separators=(",", ":")).encode("utf-8"),
                    (("Content-Type", "application/json"),))
        if isinstance(result, str):
            return 200, result.encode("utf-8"), (("Content-Type", "text/plain; charset=utf-8"),)
        if isinstance(result, bytes):
            return 200, result, ()
        if not isinstance(result, Response):
            raise TypeError("handler must return dict, Response, bytes or str")
        body = result.body
        pairs = _header_pairs(result.headers)
        if isinstance(body, str):
            body = body.encode("utf-8")
            if not any(k.lower() == "content-type" for k, _ in pairs):
                pairs += (("Content-Type", "text/plain; charset=utf-8"),)
        # Exact types, bounds and HTTP header safety are checked together by the
        # batched native response boundary, with per-request error isolation.
        return result.status, body, pairs

    async def start(self, config):
        if self._started:
            raise RuntimeError("application already started")
        if config.server is None:
            raise ValueError("application requires an HTTP ServerConfig")
        if config.server.application_dispatch_all:
            raise ValueError("App.start uses exact routes, not framework dispatch")
        if config.server.application_routes:
            raise ValueError("App.start owns the configured application routes")
        if config.server.native_json and ("GET", "/json") in self._routes:
            raise ValueError("native JSON baseline collides with application /json")
        self._max_response_bytes = config.server.max_response_bytes
        config = replace(config, server=replace(config.server,
                            application_routes=tuple(self._routes)))
        self._started = True
        try:
            return await Engine.open(config, application_handler=self._dispatch,
                                     application_native_results=self._native_responses)
        except BaseException:
            self._started = False
            raise


__all__ = ["App", "Request", "Response", "json_response"]
