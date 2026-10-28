"""Defines basics of HTTP standard."""

import sys

from decimal import Decimal
from importlib import import_module
from inspect import ismodule, signature

from orjson import OPT_NON_STR_KEYS
from orjson import dumps as orjson_dumps


def json_default(obj):
    """orjson's default for the types it does not know: a Decimal (a database
    DECIMAL or NUMERIC column) becomes a JSON number, through float, as ujson
    encoded it (the precision of a double). orjson calls it only for such
    values, so other bodies cost nothing more."""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Type is not JSON serializable: {type(obj).__name__}")


def json_dumps(obj, default=None, option=0) -> bytes:
    """The default JSON encoder of json() responses: orjson, which returns
    compact UTF-8 bytes and also encodes datetime, UUID, dataclasses and
    enums; a Decimal becomes a number (json_default). Dict keys that are not
    str become strings, as with the standard library. default and option are
    orjson's (json()'s extra keyword arguments); a default given replaces
    json_default."""
    return orjson_dumps(obj, json_default if default is None else default, option | OPT_NON_STR_KEYS)


def json_text(obj) -> str:
    """json_dumps as str, for text output (error pages, JSON log lines)."""
    return json_dumps(obj).decode()

# The reason phrase of each status (RFC 9110 section 15, and the IANA
# registry for the WebDAV and other extension codes). The one table both servers
# write status lines from: the native host takes it at startup
# (ServerConfig.reason_phrases). A status not here gets an empty phrase.
STATUS_CODES: dict[int, bytes] = {
    100: b"Continue",
    101: b"Switching Protocols",
    102: b"Processing",
    103: b"Early Hints",
    200: b"OK",
    201: b"Created",
    202: b"Accepted",
    203: b"Non-Authoritative Information",
    204: b"No Content",
    205: b"Reset Content",
    206: b"Partial Content",
    207: b"Multi-Status",
    208: b"Already Reported",
    226: b"IM Used",
    300: b"Multiple Choices",
    301: b"Moved Permanently",
    302: b"Found",
    303: b"See Other",
    304: b"Not Modified",
    305: b"Use Proxy",
    307: b"Temporary Redirect",
    308: b"Permanent Redirect",
    400: b"Bad Request",
    401: b"Unauthorized",
    402: b"Payment Required",
    403: b"Forbidden",
    404: b"Not Found",
    405: b"Method Not Allowed",
    406: b"Not Acceptable",
    407: b"Proxy Authentication Required",
    408: b"Request Timeout",
    409: b"Conflict",
    410: b"Gone",
    411: b"Length Required",
    412: b"Precondition Failed",
    413: b"Content Too Large",
    414: b"URI Too Long",
    415: b"Unsupported Media Type",
    416: b"Range Not Satisfiable",
    417: b"Expectation Failed",
    418: b"I'm a teapot",
    421: b"Misdirected Request",
    422: b"Unprocessable Content",
    423: b"Locked",
    424: b"Failed Dependency",
    425: b"Too Early",
    426: b"Upgrade Required",
    428: b"Precondition Required",
    429: b"Too Many Requests",
    431: b"Request Header Fields Too Large",
    451: b"Unavailable For Legal Reasons",
    500: b"Internal Server Error",
    501: b"Not Implemented",
    502: b"Bad Gateway",
    503: b"Service Unavailable",
    504: b"Gateway Timeout",
    505: b"HTTP Version Not Supported",
    506: b"Variant Also Negotiates",
    507: b"Insufficient Storage",
    508: b"Loop Detected",
    510: b"Not Extended",
    511: b"Network Authentication Required",
}

# According to https://tools.ietf.org/html/rfc2616#section-7.1
_ENTITY_HEADERS = frozenset(
    [
        "allow",
        "content-encoding",
        "content-language",
        "content-length",
        "content-location",
        "content-md5",
        "content-range",
        "content-type",
        "expires",
        "last-modified",
        "extension-header",
    ]
)

# According to https://tools.ietf.org/html/rfc2616#section-13.5.1
_HOP_BY_HOP_HEADERS = frozenset(
    [
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
    ]
)


def has_message_body(status):
    """
    According to the following RFC message body and length SHOULD NOT
    be included in responses status 1XX, 204 and 304.
    https://tools.ietf.org/html/rfc2616#section-4.4
    https://tools.ietf.org/html/rfc2616#section-4.3
    """
    return status not in (204, 304) and not (100 <= status < 200)


def is_entity_header(header):
    """Checks if the given header is an Entity Header"""
    return header.lower() in _ENTITY_HEADERS


def is_hop_by_hop_header(header):
    """Checks if the given header is a Hop By Hop header"""
    return header.lower() in _HOP_BY_HOP_HEADERS


def import_string(module_name, package=None):
    """
    import a module or class by string path.

    :module_name: str with path of module or path to import and
    instantiate a class
    :returns: a module object or one instance from class if
    module_name is a valid path to class

    """
    module, klass = module_name.rsplit(".", 1)
    module = import_module(module, package=package)
    obj = getattr(module, klass)
    if ismodule(obj):
        return obj
    return obj()


def is_atty() -> bool:
    return bool(sys.stdout and sys.stdout.isatty())


def accepts(func, *args, **kwargs) -> bool:
    """Whether `func` can be called with these arguments, by its signature,
    without calling it: a TypeError raised inside a call is the function's
    own, not a sign that it takes other arguments. True when the signature
    cannot be read (some builtins), so the call itself decides."""
    try:
        bound = signature(func)
    except (TypeError, ValueError):
        return True
    try:
        bound.bind(*args, **kwargs)
    except TypeError:
        return False
    return True


class Default:
    """
    It is used to replace `None` or `object()` as a sentinel
    that represents a default value. Sometimes we want to set
    a value to `None` so we cannot use `None` to represent the
    default value, and `object()` is hard to be typed.
    """

    def __repr__(self):
        return "<Default>"

    def __str__(self) -> str:
        return self.__repr__()


_default = Default()
