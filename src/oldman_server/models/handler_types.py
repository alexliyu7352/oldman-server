from asyncio.events import AbstractEventLoop
from collections.abc import Coroutine
from typing import Any, Callable, TypeVar

import oldman_server

from oldman_server import request
from oldman_server.response import BaseHTTPResponse, HTTPResponse


AppVar = TypeVar("AppVar", bound="oldman_server.OldmanServer")
Request = TypeVar("Request", bound="request.Request")

MiddlewareResponse = (
    HTTPResponse | None | Coroutine[Any, Any, HTTPResponse | None]
)
RequestMiddlewareType = Callable[[Request], MiddlewareResponse]
ResponseMiddlewareType = Callable[
    [Request, BaseHTTPResponse], MiddlewareResponse
]
ErrorMiddlewareType = Callable[
    [Request, BaseException], Coroutine[Any, Any, None] | None
]
MiddlewareType = RequestMiddlewareType | ResponseMiddlewareType
ListenerType = (
    Callable[[AppVar], Coroutine[Any, Any, None] | None]
    | Callable[[AppVar, AbstractEventLoop], Coroutine[Any, Any, None] | None]
)
RouteHandler = Callable[..., Coroutine[Any, Any, HTTPResponse | None]]
SignalHandler = Callable[..., Coroutine[Any, Any, None]]
