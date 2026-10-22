from enum import auto

from oldman_server.compat import UpperStrEnum


class HTTPMethod(UpperStrEnum):
    """HTTP methods that are commonly used."""

    GET = auto()
    POST = auto()
    PUT = auto()
    HEAD = auto()
    OPTIONS = auto()
    PATCH = auto()
    DELETE = auto()


HTTP_METHODS = tuple(HTTPMethod.__members__.values())
SAFE_HTTP_METHODS = (HTTPMethod.GET, HTTPMethod.HEAD, HTTPMethod.OPTIONS)
IDEMPOTENT_HTTP_METHODS = (
    HTTPMethod.GET,
    HTTPMethod.HEAD,
    HTTPMethod.PUT,
    HTTPMethod.DELETE,
    HTTPMethod.OPTIONS,
)
CACHEABLE_HTTP_METHODS = (HTTPMethod.GET, HTTPMethod.HEAD)
DEFAULT_HTTP_CONTENT_TYPE = "application/octet-stream"
# Connections a listening socket queues before the server accepts them. Sanic
# uses 100, which a proxy opening many upstream connections at once exceeds
# (connect() on a unix socket then fails; nginx answers 502); the kernel caps
# it at net.core.somaxconn. Gunicorn and Uvicorn use 2048 too.
DEFAULT_BACKLOG = 2048
