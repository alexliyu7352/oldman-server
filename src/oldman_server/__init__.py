from types import SimpleNamespace

from typing_extensions import TypeAlias

from oldman_server.__version__ import __version__
from oldman_server.app import OldmanServer
from oldman_server.blueprints import Blueprint
from oldman_server.config import Config
from oldman_server.constants import HTTPMethod
from oldman_server.cors import CORS
from oldman_server.exceptions import (
    BadRequest,
    ExpectationFailed,
    FileNotFound,
    Forbidden,
    HeaderNotFound,
    InternalServerError,
    InvalidHeader,
    MethodNotAllowed,
    NotFound,
    RangeNotSatisfiable,
    OldmanServerException,
    ServerError,
    ServiceUnavailable,
    Unauthorized,
)
from oldman_server.request import Request
from oldman_server.response import (
    HTTPResponse,
    empty,
    file,
    html,
    json,
    raw,
    redirect,
    text,
)
from oldman_server.server.websockets.impl import WebsocketImplProtocol as Websocket


DefaultServer: TypeAlias = "OldmanServer[Config, SimpleNamespace]"
"""
A type alias for an app with a default config and namespace.
"""

DefaultRequest: TypeAlias = Request[DefaultServer, SimpleNamespace]
"""
A type alias for a request with a default app and namespace.
"""

__all__ = (
    "__version__",
    # Common objects
    "OldmanServer",
    "CORS",
    "Config",
    "Blueprint",
    "HTTPMethod",
    "HTTPResponse",
    "Request",
    "Websocket",
    # Common types
    "DefaultServer",
    "DefaultRequest",
    # Common exceptions
    "BadRequest",
    "ExpectationFailed",
    "FileNotFound",
    "Forbidden",
    "HeaderNotFound",
    "InternalServerError",
    "InvalidHeader",
    "MethodNotAllowed",
    "NotFound",
    "RangeNotSatisfiable",
    "OldmanServerException",
    "ServerError",
    "ServiceUnavailable",
    "Unauthorized",
    # Common response methods
    "empty",
    "file",
    "html",
    "json",
    "raw",
    "redirect",
    "text",
)
