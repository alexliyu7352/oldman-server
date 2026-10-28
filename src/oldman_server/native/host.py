"""The application on the native host: each native request runs through the
framework's processing chain (routing, middleware, error handling) as a Task,
and its response, stream, WebSocket tunnel or file goes back to the shards.
"""
import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
import gc
import os
import socket
import sys
from inspect import isawaitable
from time import perf_counter
from types import CoroutineType

from oldman_server import OldmanServer
from oldman_server.application.constants import ServerStage
from oldman_server.application.state import ApplicationServerInfo
from oldman_server import compat as _compat
from oldman_server.compat import Header
from oldman_server.cors import compile as cors_compile
from oldman_server.exceptions import (BadRequest, ExpectationFailed, PayloadTooLarge, RequestCancelled,
                              OldmanServerException, RequestTimeout, ServerError, ServiceUnavailable)
from oldman_server.helpers import has_message_body
from oldman_server.http import http1 as _http1
from oldman_server.http.constants import Stage
from oldman_server.log import access_logger, error_logger, server_logger, websockets_logger
from oldman_server.middleware import Middleware
from oldman_server.models import server_types
from oldman_server.request import Request
from oldman_server.request.types import _NATIVE_UNSET, _PARSED_URLS
from oldman_server.response import BaseHTTPResponse, HTTPResponse, JSONResponse, ResponseStream
from oldman_server.response.types import HEADER_TRANSLATION_TABLE, _SentContentType
from oldman_server.router import Router
from oldman_server.signals import RESERVED_NAMESPACES
from oldman_server.touchup.schemes.base import BaseScheme
from oldman_server.touchup.schemes.ode import RemoveDispatch
from oldman_server.routing.exceptions import NotFound

from . import _native
from .cpus import parse_cpus, same_thread, shard_count, worker_group
from .engine import (BodyError, CACHED, Config, Engine, ServerConfig, STREAM_DATA, STREAM_END,
                     STREAM_START)
from oldman_server.request import form as _form

from websockets.protocol import State as _WSState
from websockets.server import ServerProtocol as _WSServerProtocol
from websockets.typing import Subprotocol


# Multipart forms are parsed natively when the result is the same (see
# native/multipart.hh); otherwise Sanic's parser runs.
_form.native_multipart = _native.parse_multipart

# The native host's current Engine; at most one serves at a time. Streamed
# responses queue their pieces through it.
_engine = None
# Whether the served app has a route with a CORS policy of its own: its
# answers then name their route's policy (see _with_cors).
_cors_codes = False


class _Answered(Exception):
    """_call_flat: the request was answered before routing (a CORS preflight)."""


def _with_cors(request, result):
    """`result` naming the CORS policy of the request's route for the native
    host (the decision kind's high bits): unchanged for the default policy."""
    route = request.route
    code = getattr(route.extra, "cors_code", 0) if route is not None else 0
    if not code or result is None:
        return result
    if len(result) == 3:
        return (*result, code << 8, -1)
    status, body, headers, kind, value = result
    return (status, body, headers, kind | code << 8, value)

# Exact fork types whose processed_headers/send are the BaseHTTPResponse ones;
# their headers are encoded inline. Subclasses keep the generic calls.
_COMPLETE_TYPES = frozenset((HTTPResponse, JSONResponse))
# Values that are never awaitable; skips inspect's ABC check on the hot path.
_NOT_AWAITABLE = frozenset((HTTPResponse, JSONResponse, type(None)))
# The http.* events a signal can name without a parameter.
_HTTP_EVENTS = frozenset(RESERVED_NAMESPACES["http"])
# (name, value) str header pairs -> processed_headers encoding and lowered name.
# Common pairs repeat across responses; cleared when full to stay bounded.
_ENCODED_HEADERS = {}


# Exceptions Sanic's Http raises for what the native host rejects while reading
# a request, by status; the message is the host's (the same text as Sanic's).
_PROTOCOL_ERRORS = {400: BadRequest, 408: RequestTimeout, 413: PayloadTooLarge, 417: ExpectationFailed}


def _timeout_ms(seconds):
    """A Sanic timeout in the host's milliseconds, 0 meaning none. Sanic checks
    its timeouts at least 0.1 s apart, so a shorter one (0 included) acts as
    0.1 s; one past a day is cut to a day; infinity is none."""
    seconds = float(seconds)
    if seconds == float("inf"):
        return 0
    return min(86_400_000, max(100, int(seconds * 1000)))


def _protocol_error(status, message):
    error_class = _PROTOCOL_ERRORS.get(status)
    if error_class is not None:
        return error_class(message)
    return OldmanServerException(message, status_code=status)


def _log_access(request, exchange, started):
    """Http.log_response for a response the exchange sent (ACCESS_LOG)."""
    response = exchange.response
    if exchange.streaming is not None:
        byte = response.headers.get("content-length", "chunked")
    elif exchange.result is not None:
        result = exchange.result
        byte = result[4] if len(result) == 5 else len(result[1])
    else:
        return  # nothing was sent
    extra = {
        "status": getattr(response, "status", 0),
        "byte": byte,
        "host": f"{id(request.transport):X}"[-5:-1] + "unx",
        "request": "nil",
        "duration": f" {1000 * (perf_counter() - started):.1f}ms",
    }
    if ip := request.client_ip:
        extra["host"] = f"{ip}:{request.port}"
    extra["request"] = f"{request.method} {request.url}"
    _http1.access_logger.info("", extra=extra)


def _bytes_body(data):
    """A response body other than bytes, as the asyncio server sends it:
    bytes-like (bytearray, memoryview, a bytes subclass) as its bytes."""
    try:
        return bytes(memoryview(data))
    except TypeError:
        raise ValueError("response body must be bytes") from None


def _final_status(status):
    """Http.http1_response_header's check, for a status that is not a plain
    int of at least 200: a handler's response has a final status (only the
    WebSocket handshake sends a 101, not through here). An int subclass such
    as HTTPStatus is sent as its int value."""
    if not isinstance(status, int) or status < 200:
        raise RuntimeError(f"Invalid response status {status!r}")
    return int(status)


def _encode_header(name, value):
    name = name.translate(HEADER_TRANSLATION_TABLE).encode("ascii")
    value = f"{value}".translate(HEADER_TRANSLATION_TABLE).encode(errors="surrogateescape")
    return name, value, name.lower()


def _cached_header(name, value):
    encoded = _ENCODED_HEADERS.get((name, value))
    if encoded is None:
        encoded = _encode_header(name, value)
        if len(_ENCODED_HEADERS) >= 1024:
            _ENCODED_HEADERS.clear()
        _ENCODED_HEADERS[name, value] = encoded
    return encoded


# content_type -> (shared _SentContentType marker, encoded header tuple) for
# responses whose only header is the default Content-Type. Markers are immutable.
_SENT_CONTENT_TYPES = {}


def _sent_content_type(content_type):
    entry = _SENT_CONTENT_TYPES.get(content_type) if type(content_type) is str else None
    if entry is None:
        name, value, _ = _encode_header("content-type", content_type)
        entry = _SentContentType(content_type), ((name, value),)
        if type(content_type) is str:
            if len(_SENT_CONTENT_TYPES) >= 256:
                _SENT_CONTENT_TYPES.clear()
            _SENT_CONTENT_TYPES[content_type] = entry
    return entry


class _TunnelTransport:
    """The asyncio transport calls WebsocketImplProtocol and SanicProtocol.close
    make, over a native tunnel."""
    __slots__ = ("_tunnel",)

    def __init__(self, tunnel):
        self._tunnel = tunnel

    def write(self, data):
        self._tunnel.write(data)

    def close(self):
        self._tunnel.end()

    def abort(self):
        self._tunnel.abort()

    def is_closing(self):
        return self._tunnel.closing

    def pause_reading(self):
        self._tunnel.pause(True)

    def resume_reading(self):
        self._tunnel.pause(False)

    def can_write_eof(self):
        return False

    def get_write_buffer_size(self):
        return 0

    def get_extra_info(self, name, default=None):
        return self._tunnel.request.transport.get_extra_info(name, default)


class _NativeTunnel:
    """WebSocketProtocol over a native tunnel (the connection after a 101): the
    io_proto WebsocketImplProtocol uses (loop, transport, send, and what
    SanicProtocol.close reads), fed by the Engine with the peer's bytes."""

    def __init__(self, exchange, request, loop):
        self.exchange, self.request, self.loop = exchange, request, loop
        # Its own Engine, not the module's current one: a late call (the
        # WebSocket's close timeout aborts 10 s later) must not reach the
        # server of a later serve in this process, whose request ids repeat.
        self.engine = _engine
        native = exchange.native
        self.request_id, self.shard = native.request_id, native.shard
        self.app = request.app
        self.conn_info = request.conn_info
        self.access_log = request.app.config.ACCESS_LOG
        self._can_write = asyncio.Event()
        self._can_write.set()
        self.transport = _TunnelTransport(self)
        self.closing = self.paused = False
        self.websocket = None
        self._early = []
        self.websocket_url = self.websocket_peer = None

    def attach(self, websocket):
        self.websocket = websocket

    def opened(self):
        request = self.request
        self.websocket_url = request.url
        self.websocket_peer = f"{id(self):X}"[-5:-1] + "unx"
        if ip := request.client_ip:
            self.websocket_peer = f"{ip}:{request.port}"
        self._log("OPEN")
        early, self._early = self._early, None
        for kind, data in early:
            getattr(self, kind)(*data)

    # SanicProtocol.send
    async def send(self, data):
        if self.transport is None or self.closing:
            raise RequestCancelled
        # Each piece is bounded by the host's response limit; a WebSocket frame
        # (up to WEBSOCKET_MAX_SIZE) may be larger.
        step = self.exchange.max_body
        for start in range(0, len(data), step):
            await self.engine.stream_send(self.request_id, self.shard, STREAM_DATA,
                                      bytes(data[start:start + step]))

    def write(self, data):
        if not self.closing:
            step = self.exchange.max_body
            for start in range(0, len(data), step):
                self.engine.stream_write(self.request_id, self.shard, bytes(data[start:start + step]))

    def end(self):
        """transport.close(): written data goes out, then the connection closes."""
        if not self.closing:
            self.closing = True
            self.exchange.streaming, self.exchange.stage = "ended", Stage.IDLE
            self.engine.stream_end(self.request_id, self.shard)

    def abort(self):
        self.closing = True
        self.exchange.streaming, self.exchange.stage = "ended", Stage.IDLE
        if not self.engine.closed:  # a closed one stopped the connection already
            self.engine.stream_abort(self.request_id, self.shard)

    def pause(self, paused):
        if paused != self.paused and not self.closing:
            self.paused = paused
            self.engine.pause_tunnel(self.request_id, self.shard, paused)

    # From the Engine: the peer's bytes, its EOF, the end of the connection.
    def data_received(self, data):
        if self._early is not None:
            self._early.append(("data_received", (data,)))
        else:
            self.websocket.data_received(data)

    def eof_received(self):
        if self._early is not None:
            self._early.append(("eof_received", ()))
        else:
            self.websocket.eof_received()

    def connection_lost(self, exc):
        if self._early is not None:
            self._early.append(("connection_lost", (exc,)))
            return
        self.closing = True
        self.exchange.streaming = "ended"
        self.websocket.connection_lost(exc)
        self.transport = None
        self._log("CLOSE")

    def _log(self, message):
        """WebSocketProtocol.log_websocket."""
        if not self.access_log or not self.websocket_url:
            return
        status = close = ""
        try:
            p = self.websocket.ws_proto
            if p.state == _WSState.CLOSED:
                codes = {1000: "NORMAL", 1001: "GOING AWAY", 1005: "NO STATUS",
                         1006: "ABNORMAL", 1011: "SERVER ERR"}
                if p.close_code == 1006:
                    message = "CLOSE_ABN"
                scode = rcode = 1006
                sdesc = rdesc = ""
                if p.close_sent:
                    scode, sdesc = p.close_sent.code, p.close_sent.reason
                if p.close_rcvd:
                    rcode, rdesc = p.close_rcvd.code, p.close_rcvd.reason
                sdesc = repr(sdesc[:256]) if sdesc else codes.get(scode, "")
                rdesc = repr(rdesc[:256]) if rdesc else codes.get(rcode, "")
                if p.close_rcvd_then_sent or scode == 1006:
                    status = rcode
                    close = (f"{rdesc} from client" if scode in (rcode, 1006)
                             else f"{rdesc} \u25bc\u25b2 {scode} {sdesc}")
                else:
                    status = scode
                    close = (f"{sdesc} from server" if rcode in (scode, 1006)
                             else f"{sdesc} \u25b2\u25bc {rcode} {rdesc}")
        except AttributeError:
            ...
        access_logger.info(message, extra={
            "status": status, "byte": close, "host": self.websocket_peer,
            "request": f" \U0001f50c {self.websocket_url}", "duration": ""})


class _BufferedExchange:
    # request_max_size: Http.read's limit (REQUEST_MAX_SIZE; none for a stream
    # handler); request_bytes: a body still arriving's declared Content-Length
    # (-1 before its first read), or a whole body's size.
    __slots__ = ("stage", "response", "request_body", "body", "max_body", "result", "failure",
                 "native", "streaming", "more_body", "request_max_size", "request_bytes",
                 "response_bytes_left")

    def __init__(self, body, max_body, native=None, more_body=False, request_max_size=float("inf")):
        self.stage = Stage.HANDLER
        self.response = None
        # A request without a body has none to read (Http.request_body unset).
        self.request_body = bool(body) or more_body
        self.body, self.max_body = body, max_body
        self.result = self.failure = None
        # Streamed responses (send with end_stream=False) go through the native
        # host's Engine with this request's id; `streaming` is None, "open" or "ended".
        self.native = native
        self.streaming = None
        # True while the request body is still arriving (body holds what came).
        self.more_body = more_body
        self.request_max_size = request_max_size
        self.request_bytes = -1
        # A streamed response's declared Content-Length less what was sent.
        self.response_bytes_left = None

    def _limit(self, size):
        self.request_bytes = size
        if size > self.request_max_size:
            # Http.read; the host ends the connection after the response.
            self.request_body = self.more_body = False
            raise PayloadTooLarge("Request body exceeds the size limit")

    async def read(self):
        if not self.request_body:
            return None
        if self.more_body:
            if self.request_bytes < 0:
                # The first read: Http.read checks a declared Content-Length; the
                # host reports a chunked body that declares more (receive).
                declared = self.native.header("content-length")
                self._limit(int(declared) if declared is not None else 0)
            if self.body:
                body, self.body = self.body, b""
                return body
            try:
                data = await _engine.receive(self.native.request_id,
                                             self.request_max_size != float("inf"))
            except BodyError as error:
                # Http.read(): a malformed or oversized body raises; the host
                # closes the connection after the response.
                self.request_body = self.more_body = False
                raise _protocol_error(error.status, str(error)) from None
            if data is None:
                self.request_body = self.more_body = False
            return data
        self.request_body = False
        body, self.body = self.body, b""
        self._limit(len(body))
        return body or None

    async def __aiter__(self):
        while (data := await self.read()) is not None:
            if data:
                yield data

    def respond(self, response):
        if self.stage is not Stage.HANDLER:
            # Http.respond: the earlier response stays what was (being) sent.
            self.stage = Stage.FAILED
            raise RuntimeError("Response already started")
        if self.response is not None:
            self.response.stream = None
        self.response = response
        response.stream = self
        return response

    def _fail(self, message):
        """A failure once the response has been handed over (submitted, or a
        stream started): it can no longer be replaced, only end the
        connection. A response refused before that only raises: the stage
        stays HANDLER and the error handler answers, as in Sanic's Http, whose
        stage moves on only once the response is sent."""
        self.failure = ValueError(message)
        self.stage = Stage.FAILED
        raise self.failure

    @property
    def send(self):
        """Http.send: None once the response is complete, so that
        BaseHTTPResponse.send ignores a final empty send and rejects data."""
        if self.result is not None or self.streaming == "ended":
            return None
        return self._send

    async def _send(self, data, end_stream):
        if self.streaming is not None:
            await self._stream_more(data, end_stream)
        elif end_stream:
            self.complete(data, end_stream)
        else:
            await self._stream_start(data)

    async def _stream_start(self, data):
        """The first send(end_stream=False): head and first data of a streamed response."""
        if self.failure is not None:
            raise self.failure
        if self.result is not None:
            self._fail("response is already submitted")
        # Nothing is handed over until stream_send below: a response refused
        # here only raises (see _fail).
        if self.response is None:
            raise ValueError("response is missing")
        if _engine is None or self.native is None:
            raise ValueError("streaming responses are not supported")
        response = self.response
        status = response.status
        if type(status) is not int or status < 200:
            status = _final_status(status)
        body = response.body or b""
        if type(body) is not bytes:
            body = _bytes_body(body)
        if type(data) is not bytes:
            data = _bytes_body(data)
        if len(body) + len(data) > self.max_body:
            raise ValueError("response body exceeds the native limit")
        first = body + data if data else body
        headers, lengths = self._encode_headers(response)
        if lengths and (len(set(lengths)) > 1 or not lengths[0].isdigit()):
            raise ValueError("inconsistent content-length")
        if lengths:
            self.response_bytes_left = int(lengths[0]) - len(first)
        # Sanic's stage while a response is being sent: errors can no longer
        # produce an error response, only end the connection.
        self.streaming, self.stage = "open", Stage.RESPONSE
        kind = STREAM_START
        if _cors_codes:
            # The CORS policy of the request's route (see _with_cors).
            request = Request._current.get(None)
            route = request.route if request is not None else None
            if route is not None:
                kind |= getattr(route.extra, "cors_code", 0) << 8
        await _engine.stream_send(
            self.native.request_id, self.native.shard, kind, first, status,
            tuple(headers), int(lengths[0]) if lengths else -1)

    async def _stream_more(self, data, end_stream):
        if self.failure is not None:
            raise self.failure
        if self.streaming == "ended":
            if end_stream and not data:
                return  # Sanic finalizes once more after an explicit eof.
            self._fail("response is already submitted")
        if type(data) is not bytes:
            data = _bytes_body(data)
        if len(data) > self.max_body:
            self._fail("response body exceeds the native limit")
        if self.response_bytes_left is not None:
            self._count_sent(len(data), end_stream)
        if end_stream:
            self.streaming, self.stage = "ended", Stage.IDLE
        await _engine.stream_send(self.native.request_id, self.native.shard,
                                  STREAM_END if end_stream else STREAM_DATA, data)

    def _count_sent(self, size, end_stream):
        """Http.http1_response_normal: nothing past a declared Content-Length is
        sent, nor an end before it. The stream fails then: the connection ends
        after it (Sanic stops keep-alive once an error comes after the response
        started)."""
        left = self.response_bytes_left - size
        if left < 0 or (end_stream and left):
            self.failure = ServerError("Response was bigger than content-length" if left < 0
                                       else "Response was smaller than content-length")
            self.stage = Stage.FAILED
            raise self.failure
        self.response_bytes_left = left

    def abort(self):
        """End an open stream without completing it; the host closes the
        connection (BaseHTTPResponse.abort(), and the chain exiting with the
        stream open)."""
        if self.streaming == "open":
            self.streaming = "ended"
            _engine.stream_abort(self.native.request_id, self.native.shard)
        elif self.streaming is None and self.result is None:
            raise ServerError(_http1.ABORT_NOT_STARTED)

    def complete(self, data=b"", end_stream=True):
        """BaseHTTPResponse.send(end_stream=True) without the coroutine."""
        if self.failure is not None:
            raise self.failure
        if self.result is not None:
            # Sanic finalizes once more after an explicit request.respond/send.
            if end_stream and not data:
                return
            self._fail("response is already submitted")
        # Nothing is handed over until self.result is set: a response refused
        # here only raises (see _fail).
        if not end_stream:
            raise ValueError("streaming responses are not supported")
        if self.response is None:
            raise ValueError("response is missing")
        body = self.response.body or b""
        if type(body) is not bytes:
            body = _bytes_body(body)
        if type(data) is not bytes:
            data = _bytes_body(data)
        if len(body) + len(data) > self.max_body:
            raise ValueError("response body exceeds the native limit")
        body = body + data if data else body
        response = self.response
        status = response.status
        if type(status) is not int or status < 200:
            status = _final_status(status)
        if type(response) in _COMPLETE_TYPES and response._headers is None:
            # Headers never accessed: processed_headers yields only the default
            # Content-Type, which the response's headers still show afterwards.
            if (status not in (204, 304)  # has_message_body (status is final)
                    and response.content_type is not None):
                response._headers, headers = _sent_content_type(response.content_type)
            else:
                headers = ()
            self.result = status, body, headers
            self.stage = Stage.IDLE
            return
        headers, lengths = self._encode_headers(response)
        # Http.http1_response_header: the host writes the body's length (Sanic
        # replaces any Content-Length), except that a HEAD response keeps the
        # length it was given.
        native = self.native
        if (lengths and native is not None and lengths[0].isdigit()
                and native.method == "HEAD"):
            self.result = status, body, tuple(headers), 0, int(lengths[0])
        else:
            self.result = status, body, tuple(headers)
        self.stage = Stage.IDLE

    def _encode_headers(self, response):
        """processed_headers as owned bytes pairs, plus any Content-Length values."""
        headers, lengths = [], []
        header_bytes = decoded_delta = 0
        if type(response) in _COMPLETE_TYPES:
            # BaseHTTPResponse.processed_headers, inlined: the same setdefault,
            # sanitizing and encoding, so the same values and exceptions.
            if has_message_body(response.status) and response.content_type is not None:
                response.headers.setdefault("content-type", response.content_type)
            for name, value in response.headers.items():
                name, value, lower = (_cached_header(name, value) if type(value) is str
                                      else _encode_header(name, value))
                if lower == b"content-length":
                    lengths.append(value)
                else:
                    header_bytes += len(name) + len(value) + 4
                    if len(headers) == 128 or header_bytes > 8192:
                        raise ValueError("response headers exceed the native limit")
                    headers.append((name, value))
        else:
            # Snapshot Sanic's encoded/sanitized values at logical send. The
            # native batch later copies these owned bytes; no Response is queued.
            for name, value in response.processed_headers:
                lower = name.lower()
                if lower == b"content-length":
                    lengths.append(value)
                else:
                    original_size = len(name) + len(value) + 4
                    header_bytes += original_size
                    if len(headers) == 128 or header_bytes > 8192:
                        raise ValueError("response headers exceed the native limit")
                    if type(name) is bytes and type(value) is bytes:
                        if not name.isascii():
                            name.decode("ascii")  # Preserve send-time rejection.
                        headers.append((name, value))
                    else:
                        # Custom generators may supply mutable values or bytes
                        # subclasses with decode hooks. Preserve the old hook
                        # semantics and freeze their result before returning.
                        name, value = name.decode("ascii"), value.decode("latin-1")
                        if type(name) is not str or type(value) is not str:
                            # The old native str validator rejected these only
                            # after the chain completed. Keep that failure time;
                            # never reinterpret an invalid decoder result as bytes.
                            headers.append((None, None))
                        else:
                            decoded_delta += len(name) + len(value) + 4 - original_size
                            # A decode hook can expand a tiny value into a huge
                            # preexisting str. Bound copies using the actual
                            # cumulative size; keep consuming hooks on failure.
                            if header_bytes + decoded_delta > 8192:
                                pair = None, None
                            else:
                                try:
                                    pair = name.encode("latin-1"), value.encode("latin-1")
                                except UnicodeEncodeError:
                                    pair = None, None  # Same deferred native rejection.
                            headers.append(pair)
        return headers, lengths

    async def send_file(self, path, offset, size, chunk_size):
        """file_stream's reads and writes, done by the native host: `size` bytes
        of `path` from `offset` (None: to the end), framed as chunk_size writes.
        The file is opened first: a missing or unreadable one raises before the
        response starts, as open() does in Sanic's file_stream."""
        stat = await _engine.file_open_stat(path)
        length = max(0, stat.st_size - offset)
        if size is not None:
            length = min(length, size)
        if not length:
            return  # Sanic writes nothing for an empty read
        if self.streaming is None:
            await self._stream_start(b"")
        if self.response_bytes_left is not None:
            self._count_sent(length, False)
        await _engine.stream_file(self.native.request_id, self.native.shard, path, offset, length,
                                  chunk_size)

    async def send_file_body(self, response, path, offset, length):
        """FileResponse.send: the complete response whose body is `length` bytes
        of `path` from `offset`, read and written by the native host. False when
        it cannot be (the response is then sent with the file read into it)."""
        status = response.status
        if (self.native is None or self.streaming is not None or self.result is not None
                or type(status) is not int or not has_message_body(status)):
            return False
        if self.native.method == "HEAD":
            # The body's Content-Length unless one was given, and no body.
            response.headers.setdefault("content-length", str(length))
            response.body = b""
            self.complete()
            return True
        # As Sanic sets it for a complete body, replacing any other value.
        response.headers["content-length"] = str(length)
        await self._stream_start(b"")
        self._count_sent(length, False)
        await _engine.stream_file(self.native.request_id, self.native.shard, path, offset, length,
                                  65536)
        await self._stream_more(b"", True)
        return True

    async def websocket_handshake(self, request, subprotocols=None):
        """WebSocketProtocol.websocket_handshake: the 101 switches the native
        connection to a tunnel served by WebsocketImplProtocol."""
        from oldman_server.server.protocols.websocket_protocol import WebSocketProtocol
        from oldman_server.server.websockets.impl import WebsocketImplProtocol
        config = request.app.config
        try:
            if subprotocols is not None:
                # subprotocols can be a set or frozenset, but ServerProtocol needs a list
                subprotocols = [Subprotocol(subprotocol) for subprotocol in subprotocols]
            ws_proto = _WSServerProtocol(max_size=config.WEBSOCKET_MAX_SIZE,
                                         subprotocols=subprotocols, state=_WSState.OPEN,
                                         logger=websockets_logger)
            resp = ws_proto.accept(WebSocketProtocol.sanic_request_to_ws_request(request))
        except Exception:
            raise OldmanServerException("Failed to open a WebSocket connection.\n"
                                 "See server log for more information.\n", status_code=500)
        if not 100 <= resp.status_code <= 299:
            raise OldmanServerException(resp.body, resp.status_code)
        if _engine is None or self.native is None or self.streaming is not None:
            raise ServerError("the native host cannot switch this request to a WebSocket")
        loop = asyncio.get_running_loop()
        tunnel = _NativeTunnel(self, request, loop)
        # Registered before the 101 goes out: the client may send at once.
        _engine.open_tunnel(self.native.request_id, tunnel)
        headers = tuple((name.encode(), value.encode()) for name, value in resp.headers.items())
        body = resp.body + b"\r\n\r\n" if resp.body else b""
        # "tunnel": after the 101 the connection is the WebSocket's (finish).
        self.streaming, self.stage = "tunnel", Stage.RESPONSE
        await _engine.stream_send(self.native.request_id, self.native.shard, STREAM_START, body,
                                  resp.status_code, headers, -1)
        # HttpProtocol.check_timeouts: no timeouts while a WebSocket is handled.
        websockets_logger.debug("Handling websocket. Timeouts disabled.")
        websocket = WebsocketImplProtocol(
            ws_proto, ping_interval=config.WEBSOCKET_PING_INTERVAL,
            ping_timeout=config.WEBSOCKET_PING_TIMEOUT, close_timeout=10.0)
        tunnel.attach(websocket)
        await websocket.connection_made(tunnel, loop=loop)
        tunnel.opened()
        return websocket

    def _end_tunnel(self):
        """The request is done with a connection switched to a WebSocket: as
        WebSocketProtocol.close does on the asyncio server, its end goes
        through WebsocketImplProtocol (end_connection: the closing handshake,
        then its auto-closer closes the tunnel), so the WebSocket's own tasks
        end with it rather than waiting for a close that never comes."""
        tunnel = _engine.tunnel(self.native.request_id) if _engine is not None else None
        if tunnel is not None and tunnel.websocket is not None:
            tunnel.websocket.end_connection(1001)
        elif tunnel is not None:
            tunnel.end()
        else:  # the tunnel was never registered or is gone: end the stream
            self.streaming, self.stage = "ended", Stage.IDLE
            if _engine is not None:
                _engine.stream_end(self.native.request_id, self.native.shard)

    def finish(self):
        if self.streaming is not None:
            # Http.http1_request_handler after the handler chain: a response
            # still being streamed is ended as by its final send (aborted if
            # sending failed).
            if self.streaming == "open":
                if self.failure is None:
                    self.streaming, self.stage = "ended", Stage.IDLE
                    _engine.stream_end(self.native.request_id, self.native.shard)
                else:
                    self.abort()
            elif self.streaming == "tunnel":
                self._end_tunnel()
            if self.failure is not None:
                raise self.failure
            return None
        if self.failure is not None:
            raise self.failure
        if self.result is None:
            self._fail("incomplete response")
        return self.result


class NativeApplication:
    """One native request -> Sanic processing -> one owned complete response."""
    def __init__(self, app, max_response_bytes):
        self.app = app
        self.max_response_bytes = max_response_bytes
        self.request_max_size = app.config.REQUEST_MAX_SIZE
        # HttpProtocol reads ACCESS_LOG once per connection; once per serve here.
        self._access_log = app.config.ACCESS_LOG
        request_class = app.request_class or Request
        self._request_class = request_class
        self._make_request = (Request._from_native if request_class is Request
                              else self._compat_request)
        # Reuse the pinned Sanic TouchUp event matcher after signal finalization.
        # Bind per adapter: compiling one app must not modify another app's entry.
        handler = NativeApplication._dispatch_request
        if app.config.TOUCHUP and app.signal_router.finalized:
            try:
                # TouchUp 25.12.1 sees normalized wildcard names ("*") but
                # tests for ">". Preserve matches using the router itself.
                app.signal_router.get("http.lifecycle.request")
            except NotFound:
                handler = BaseScheme.build(handler, globals(), app)
        self._handle_request = handler.__get__(self)
        reason = self._flat_unsupported(app)
        self._flat = reason is None
        if reason is not None:
            # Each of these costs every request several microseconds; most
            # come from the application's own settings, so say which.
            server_logger.info(f"The native host serves {app.name} without its fast path: {reason}")
        if self._flat:
            # Compiled for this app as TouchUp compiles handle_request: the
            # dispatch of each http.* event without a signal is removed, so an
            # app with none dispatches nothing.
            self._call_flat = BaseScheme.build(NativeApplication._call_flat, globals(), app).__get__(self)
        # The flat path finishes a plain response natively (sanic_finish) unless
        # something receives http.lifecycle.response, dispatched between the
        # respond and the send: judged as TouchUp judged that dispatch above.
        self._finish_natively = self._flat and RemoveDispatch(
            [signal.name for signal in app.signal_router.routes])._not_registered("http.lifecycle.response")
        # Route tables may be reset and refinalized; their Host demand is read
        # per request. Router methods replaced after startup are not supported.
        self._standard_router = self._flat and app.router._native_routing_methods_standard
        self._cached_routes = cached_routes(app)
        # A route with a CORS policy of its own: answers name it (_with_cors).
        global _cors_codes
        table = app._cors_table
        self._cors_codes = _cors_codes = table is not None and table.route_policies
        # Nothing after the send changes the answer (an access log line, a
        # cached route or a route's CORS policy): sanic_finish may settle the
        # request too.
        self._settle_natively = (not self._access_log and not self._cached_routes
                                 and not self._cors_codes)
        # __call__ changes an answer only for these (one check per request).
        self._adjusts = bool(self._cached_routes) or self._cors_codes

    @staticmethod
    def _flat_unsupported(app):
        """Why _serve_flat cannot reproduce handle_request for this app, or
        None when it can.

        Decided once at startup: replacing these methods, the router or the
        request class, or registering signals, after startup is not supported.
        """
        if app.config.TOUCHUP is not True:
            return "TOUCHUP is off (debug mode turns it off)"
        if not app.signal_router.finalized:
            return "its signals are not finalized"
        request_class = app.request_class or Request
        if request_class is not Request:
            return (f"its request class is {request_class.__module__}.{request_class.__qualname__}, "
                    "not Request")
        if type(app.router) is not Router:
            return f"its router is {type(app.router).__module__}.{type(app.router).__qualname__}, not Router"
        app_class = type(app)
        for name in ("handle_request", "handle_exception", "dispatch",
                     "_run_request_middleware", "_run_response_middleware"):
            if getattr(app_class, name) is not getattr(OldmanServer, name):
                return f"{app_class.__qualname__} replaces {name}"
        # _call_flat dispatches the http.* events handle_request, the
        # middleware runners and _dispatch_request do, at the same points
        # (the others the native host dispatches on neither path). A
        # parameterized one ("http.lifecycle.<name>") TouchUp cannot match.
        for route in app.signal_router.routes:
            if route.name not in _HTTP_EVENTS and route.name.startswith(("http", "<")):
                return f"the signal {route.name} is a parameterized http event"
        return None

    @classmethod
    def _flat_supported(cls, app):
        """Whether _serve_flat reproduces handle_request for this app."""
        return cls._flat_unsupported(app) is None

    async def _dispatch_request(self, request):
        await self.app.dispatch("http.lifecycle.request", inline=True,
                                context={"request": request}, fail_not_found=False)
        await self.app.handle_request(request)

    def _compat_request(self, native, app):
        version, client, server = native.http_version, native.client, native.server
        if not version or (client is None and not native.unix_socket) or server is None:
            raise ValueError("native request metadata is required")
        connection = server_types.native_connection(native)
        # Positional, in Request's order (subclasses may name them differently).
        request = self._request_class(
            native.target, Header(native.sanic_headers()),
            version, native.method, connection.transport, app, Request._native_head(native))
        request.conn_info = connection.conn_info
        request._protocol = connection
        return request

    async def error(self, native):
        """A protocol error the host detected before a request existed."""
        status, message = native.error.split(":", 1)
        return await self._error_response(native, _protocol_error(int(status), message))

    async def _error_response(self, native, exception):
        """Http.error_response() over Http.create_empty_request(): the error
        handler renders the response; the host closes the connection after it."""
        started = perf_counter()
        target = native.target
        url_bytes = (target.decode("ascii", errors="backslashreplace").encode("ascii")
                     if target else b"*")
        app = self.app
        transport = server_types.native_connection(native).transport
        try:
            request = self._request_class(url_bytes, Header({}), "1.1", "NONE", transport, app)
        except OldmanServerException:
            request = self._request_class(b"*", Header({}), "1.1", "NONE", transport, app)
        exchange = _BufferedExchange(b"", self.max_response_bytes, native)
        request.stream = exchange
        try:
            request_middleware = not isinstance(exception, (ServiceUnavailable, RequestCancelled))
            try:
                await app.handle_exception(request, exception, request_middleware)
            except Exception as error:
                await app.handle_exception(request, error, False)
            result = exchange.finish()
            if self._access_log:
                _log_access(request, exchange, started)
            return result
        finally:
            if exchange.response is not None:
                exchange.response.stream = None

    async def _cancelled(self, request, exchange, error):
        """Http.http1_request_handler's CancelledError branch. A response
        timeout, or a CancelledError the handler raised itself, is answered by
        the error handler; a cancellation from the host (the peer left, the
        server stops) ends the request."""
        if _engine is not None and _engine.timed_out(exchange.native.request_id):
            exception = ServiceUnavailable("Response Timeout")
        elif _engine is not None and _engine.lost(exchange.native.request_id):
            exception = RequestCancelled()  # the peer left: Sanic's connection_lost
        elif asyncio.current_task().cancelling():
            raise error
        else:
            exception = error
        if exchange.result is not None or exchange.streaming is not None:
            raise error  # a response is already out: Sanic can only end it
        app = self.app
        try:
            request_middleware = not isinstance(exception, (ServiceUnavailable, RequestCancelled))
            await app.handle_exception(request, exception, request_middleware)
        except Exception as failure:
            await app.handle_exception(request, failure, False)

    def _adjusted(self, request, result):
        """__call__'s answer for a native_cache route or under a route's CORS
        policy."""
        if self._cached_routes and request.method == "GET":
            result = self._cached(request, result)
        if self._cors_codes:
            result = _with_cors(request, result)
        return result

    def _cached(self, request, result):
        """A complete response to a GET for a native_cache route: one the shard
        may keep (engine.CACHED; it checks the rest)."""
        ms = self._cached_routes.get(id(request.route))
        if ms is None or result is None or len(result) != 3:
            return result
        return (*result, CACHED, ms)

    @property
    def entry(self):
        """The coroutine function the Engine awaits for each native request."""
        return self._call_flat if self._flat else self.__call__

    async def __call__(self, native):
        if self._flat:
            return await self._call_flat(native)
        started = perf_counter()
        try:
            request = self._make_request(native, self.app)
        except OldmanServerException as error:
            return await self._error_response(native, error)
        exchange = _BufferedExchange(native.body, self.max_response_bytes, native, native.more_body,
                                     self.request_max_size)
        request.stream = exchange
        request_class = self._request_class
        token = request_class._current.set(request)
        try:
            try:
                await self._handle_request(request)
            except asyncio.CancelledError as error:
                await self._cancelled(request, exchange, error)
            except Exception as error:
                await self.app.handle_exception(request, error)
            result = exchange.finish()
            if self._access_log:
                _log_access(request, exchange, started)
            if self._adjusts:
                result = self._adjusted(request, result)
            return result
        finally:
            request_class._current.reset(token)
            if exchange.streaming is not None:
                exchange.abort()  # a stream still open when the chain exits
            if exchange.response is not None:
                exchange.response.stream = None

    async def _call_flat(self, native, engine=None, request_id=0, shard=0):
        """__call__ for an app _flat_supported accepts, with _dispatch_request,
        OldmanServer.handle_request, Request.respond, both middleware runners and
        BaseHTTPResponse.send inlined into this one coroutine.

        Each step keeps the original order, state changes, exception handling,
        log messages and http.* dispatches; only the per-stage coroutines and
        ABC awaitable checks on known types are gone. Compiled per app by
        TouchUp (see __init__), which removes the dispatches nothing receives.

        With engine (Engine.open's application_settles), this coroutine is the
        request's Task and settles the request itself, as
        Engine._serve_application would around it.
        """
        answer = (500, b"", ())
        settled = False  # by sanic_finish, the answer queued as _application_finished queues it
        try:
            app = self.app
            started = perf_counter() if self._access_log else 0.0
            # The Request and its exchange made, and Request._current set, natively
            # in one call; None until the target's parse is cached (or for a
            # target refused below).
            prepared = native.sanic_prepare(app, self.max_response_bytes, self.request_max_size)
            if prepared is not None:
                request, exchange, token = prepared
            else:
                try:
                    # Request._from_native with one validating native call.
                    target, version, method, body, more_body = native.sanic_request()
                    request = Request._from_native_fields(target, version, method, app, native)
                except OldmanServerException as error:
                    answer = await self._error_response(native, error)
                    return answer
                exchange = _BufferedExchange(body, self.max_response_bytes, native, more_body,
                                             self.request_max_size)
                request.stream = exchange
                token = Request._current.set(request)
            try:
                # ---- Sanic.handle_request ----
                # An exception from the first two dispatches reaches __call__'s
                # handle_exception(request, error) on the other path: the same call.
                run_middleware = True
                try:
                    await app.dispatch("http.lifecycle.request", inline=True,
                                       context={"request": request}, fail_not_found=False)
                    await app.dispatch("http.lifecycle.handle", inline=True, context={"request": request})
                    await app.dispatch("http.routing.before", inline=True, context={"request": request})
                    if app.path_rewriters:
                        app._rewrite_request_path(request)
                    # A CORS preflight is answered here, before routing
                    # (OldmanServer.handle_request), when routes have policies
                    # of their own (the host answers the default policy's).
                    # Compiled in only then (TouchUp's OptionalCors), as the
                    # _cors_codes block at its end and the _Answered handler.
                    if (self._cors_codes and request.method == "OPTIONS"
                            and await app._cors_table.preflight(app, request)):
                        raise _Answered
                    router = app.router
                    # Host-free router.get() results, kept by the router while its
                    # route table stands (None: the Host is read).
                    routes = router._native_routes
                    if (self._standard_router and routes is not None
                            and request._headers is _NATIVE_UNSET):
                        key = (request._parsed_url.path, request.method)
                        found = routes.get(key)
                        if found is None:
                            found = router.get(request.path, request.method, None)
                            if len(routes) >= 1024:
                                routes.clear()
                            routes[key] = found
                        route, handler, kwargs = found
                    else:
                        route, handler, kwargs = router.get(
                            request.path, request.method, request._get_routing_host())
                    request._match_info = {**kwargs}
                    request.route = route
                    # kwargs as router.get() (lru_cache) gave it on the other path.
                    await app.dispatch("http.routing.after", inline=True, context={
                        "request": request, "route": route, "kwargs": kwargs, "handler": handler})
                    extra = route.extra
                    if exchange.request_body and not extra.ignore_body:
                        if hasattr(handler, "is_stream"):
                            # The handler reads request.stream itself, without a size limit.
                            exchange.request_max_size = float("inf")
                        elif not request.body:
                            if exchange.more_body:
                                # Request.receive_body(): the rest is still arriving.
                                request.body = b"".join([data async for data in exchange])
                            else:
                                # Request.receive_body() over the one buffered chunk.
                                body = request.body = exchange.body
                                exchange.body, exchange.request_body = b"", False
                                if len(body) > self.request_max_size:
                                    raise PayloadTooLarge("Request body exceeds the size limit")

                    run_middleware = False
                    response = None
                    middleware = extra.request_middleware
                    if middleware:
                        # ---- Sanic._run_request_middleware ----
                        request._request_middleware_started = True
                        for item in middleware:
                            await app.dispatch("http.middleware.before", inline=True,
                                               context={"request": request, "response": None},
                                               condition={"attach_to": "request"})
                            result = item.func(request) if type(item) is Middleware else item(request)
                            if type(result) is CoroutineType or (
                                    type(result) not in _NOT_AWAITABLE and isawaitable(result)):
                                result = await result
                            await app.dispatch("http.middleware.after", inline=True,
                                               context={"request": request, "response": None},
                                               condition={"attach_to": "request"})
                            if result:
                                response = result
                                break

                    if not response:
                        if handler is None:
                            raise ServerError(
                                "'None' was returned while requesting a handler from the router")
                        await app.dispatch("http.handler.before", inline=True, context={"request": request})
                        match_info = request._match_info
                        response = handler(request, **match_info) if match_info else handler(request)
                        if type(response) is CoroutineType or (
                                type(response) not in _NOT_AWAITABLE and isawaitable(response)):
                            response = await response
                        await app.dispatch("http.handler.after", inline=True, context={"request": request})

                    if (self._finish_natively
                            and not ((request.route and request.route.extra.response_middleware)
                                     or app.response_middleware)
                            and (finished := native.sanic_finish(
                                request, exchange, response, engine if self._settle_natively else None,
                                token))):
                        # Request.respond(response), then send(end_stream=True) completing
                        # the exchange at once, as below: for a plain response, natively;
                        # 2: and the request settled (nothing below changes its answer)
                        # with Request._current reset.
                        if finished == 2:
                            settled, token = True, None
                    else:
                        if request.responded:
                            if response is not None:
                                error_logger.error(
                                    "The response object returned by the route handler "
                                    "will not be sent to client. The request has already "
                                    "been responded to.")
                            if request.stream is not None:
                                response = request.stream.response
                        elif response is not None:
                            # ---- Request.respond(response) ----
                            try:
                                if request.stream is not None and request.stream.response:
                                    raise ServerError("Second respond call is not allowed.")
                            except AttributeError:
                                pass
                            stream = request.stream
                            if isinstance(response, BaseHTTPResponse) and stream:
                                if stream is exchange:
                                    # ---- _BufferedExchange.respond ----
                                    if exchange.stage is not Stage.HANDLER:
                                        exchange.stage = Stage.FAILED
                                        raise RuntimeError("Response already started")
                                    if exchange.response is not None:
                                        exchange.response.stream = None
                                    exchange.response = response
                                    response.stream = exchange
                                else:
                                    response = stream.respond(response)
                                    if type(response) not in _NOT_AWAITABLE and isawaitable(response):
                                        response = await response
                            # An exception from response middleware goes to
                            # app.handle_exception below (not handling an exception yet).
                            middleware = (request.route and request.route.extra.response_middleware
                                          ) or app.response_middleware
                            if middleware and not request._response_middleware_started:
                                request._response_middleware_started = True
                                # ---- Sanic._run_response_middleware ----
                                # `response` changes only if the whole runner returns,
                                # as with the original assignment of its result.
                                final = response
                                for item in middleware:
                                    await app.dispatch("http.middleware.before", inline=True,
                                                       context={"request": request, "response": final},
                                                       condition={"attach_to": "response"})
                                    result = (item.func(request, final) if type(item) is Middleware
                                              else item(request, final))
                                    if type(result) is CoroutineType or (
                                            type(result) not in _NOT_AWAITABLE
                                            and isawaitable(result)):
                                        result = await result
                                    await app.dispatch("http.middleware.after", inline=True,
                                                       context={"request": request,
                                                                "response": result if result else final},
                                                       condition={"attach_to": "response"})
                                    if result:
                                        final = result
                                        if isinstance(final, BaseHTTPResponse):
                                            final = request.stream.respond(final)
                                        break
                                response = final
                            request.responded = True
                        elif not hasattr(handler, "is_websocket"):
                            response = request.stream.response

                        if isinstance(response, BaseHTTPResponse):
                            await app.dispatch("http.lifecycle.response", inline=True,
                                               context={"request": request, "response": response})
                            if (response.stream is exchange and type(response) in _COMPLETE_TYPES
                                    and exchange.streaming is None):
                                # BaseHTTPResponse.send(end_stream=True): exchange.complete(),
                                # its usual case (headers never accessed) inlined.
                                body, status = response.body, response.status
                                if (response._headers is None and exchange.result is None
                                        and exchange.failure is None and type(body) is bytes
                                        and len(body) <= exchange.max_body and type(status) is int
                                        and status >= 200 and status != 204 and status != 304
                                        and response.content_type is not None):
                                    response._headers, headers = _sent_content_type(response.content_type)
                                    exchange.result = status, body, headers
                                    exchange.stage = Stage.IDLE
                                else:
                                    exchange.complete()
                            else:
                                await response.send(end_stream=True)
                        elif isinstance(response, ResponseStream):
                            resp = await response(request)
                            await app.dispatch("http.lifecycle.response", inline=True,
                                               context={"request": request, "response": resp})
                            await response.eof()
                        elif not hasattr(handler, "is_websocket"):
                            raise ServerError(f"Invalid response type {response!r} (need HTTPResponse)")
                except _Answered:
                    pass
                except asyncio.CancelledError as error:
                    await self._cancelled(request, exchange, error)
                except Exception as error:
                    try:
                        await app.handle_exception(request, error, run_middleware=run_middleware)
                    except Exception as failure:
                        # __call__'s own handle_exception around handle_request
                        # (Http.error_response on the asyncio server): handling
                        # the error failed, so that failure gets the error page.
                        await app.handle_exception(request, failure, False)
                if settled:
                    return None
                result = exchange.result
                if result is None or exchange.streaming is not None or exchange.failure is not None:
                    result = exchange.finish()
                if self._access_log:
                    _log_access(request, exchange, started)
                if self._cached_routes and request.method == "GET":
                    result = self._cached(request, result)
                if self._cors_codes:
                    result = _with_cors(request, result)
                answer = result
                return answer
            finally:
                if token is not None:
                    Request._current.reset(token)
                if exchange.streaming is not None:
                    exchange.abort()  # a stream still open when the chain exits
                if exchange.response is not None:
                    exchange.response.stream = None
        except BaseException as error:
            if engine is None:
                raise
            answer = engine._application_error(request_id, error)
        finally:
            if engine is not None and not settled:
                engine._application_finished(request_id, shard, answer)
        return answer

def _shard_cpus(config):
    """In a server worker process, its group's shard CPUs (see cpus.py); else
    NATIVE_SHARD_CPUS (CPU list or "1,2") or the last NATIVE_SHARDS (default
    up to 2) CPUs of this process's affinity set, leaving the rest to Python."""
    group = worker_group(config)
    if same_thread(config):
        if parse_cpus(config.get("NATIVE_SHARD_CPUS")) is not None:
            raise ValueError("NATIVE_SHARD_CPUS places shard threads: the same-thread mode has none")
        # The shard runs on the Python thread; its CPU pins the file threads.
        return group[:1] if group else tuple(sorted(os.sched_getaffinity(0))[:1])
    if group:
        return group[1:]
    explicit = parse_cpus(config.get("NATIVE_SHARD_CPUS"))
    if explicit is not None:
        return explicit
    available = sorted(os.sched_getaffinity(0))
    return tuple(available[-shard_count(config, len(available)):])


def cached_routes(app):
    """{id(route): ms} of the routes whose handler native_cache marked (a
    Route compares by its path and is not hashable)."""
    return {id(route): round(seconds * 1000) for route in app.router.routes
            if (seconds := getattr(route.handler, "__native_cache__", None))}


def native_config(app, host="127.0.0.1", port=8000, *, listen_sock=None, unix=None, accept=None,
                  worker_sock=None, worker_unix=None):
    """engine.Config that serves `app` with its Sanic settings.

    With listen_sock (a listening TCP or AF_UNIX socket the caller keeps open
    while serving, as Sanic's server does), the host accepts on it instead of
    binding host:port; over AF_UNIX, requests report `unix` (or the socket's
    name) as the server address. accept: ServerConfig fields for a listen_sock
    that worker processes share (listen_shared, and the accept turns of
    oldman_server.native.accept.turn). worker_sock: this process's own
    listening socket as well (NATIVE_WORKER_LISTEN), whose requests report
    `worker_unix` (or its name) as the server address over AF_UNIX.

    REQUEST_MAX_SIZE, REQUEST_MAX_HEADER_SIZE (with Sanic's buffer limits),
    RESPONSE_TIMEOUT (until the response starts; at most 24 h), REQUEST_TIMEOUT,
    KEEP_ALIVE and KEEP_ALIVE_TIMEOUT map to the native host.
    Native-only settings: NATIVE_SHARDS / NATIVE_SHARD_CPUS, NATIVE_MAX_REQUESTS
    (in flight, default 4096), NATIVE_MAX_RESPONSE_SIZE (default and limit
    128 MiB), NATIVE_MAX_MEMORY (bytes reserved for in-flight request bodies)
    and NATIVE_REACTOR_BACKEND (engine.Config.reactor_backend).
    """
    config = app.config
    shards = _shard_cpus(config)
    # Python checks the body size as the handler reads it (Http.read); the host
    # reads no further than this after a response (as Sanic).
    max_body = int(min(float(config.REQUEST_MAX_SIZE), 1 << 62))
    memory = int(config.get("NATIVE_MAX_MEMORY") or
                 max(1 << 30, len(shards) * (2 * min(max_body, 128 << 20) + (1 << 20))))
    if listen_sock is None:
        listener = dict(http_port=int(port), http_host=str(host))
    elif listen_sock.family == socket.AF_UNIX:
        listener = dict(listen_fd=listen_sock.fileno(), unix_path=str(unix or listen_sock.getsockname()))
    else:
        listener = dict(listen_fd=listen_sock.fileno())
    if worker_sock is not None:
        listener["worker_listen_fd"] = worker_sock.fileno()
        if worker_sock.family == socket.AF_UNIX:
            listener["worker_unix_path"] = str(worker_unix or worker_sock.getsockname())
    server = ServerConfig(
        **listener, **(accept or {}), application_dispatch_all=True,
        max_requests=int(config.get("NATIVE_MAX_REQUESTS") or 4096), max_bytes=memory,
        max_body_bytes=max_body,
        # Sanic pauses reading at REQUEST_BUFFER_SIZE.
        body_window_bytes=max(1, min(int(config.REQUEST_BUFFER_SIZE), 1 << 30)),
        max_response_bytes=int(config.get("NATIVE_MAX_RESPONSE_SIZE") or 128 * 1024 * 1024),
        decision_timeout_ms=_timeout_ms(config.RESPONSE_TIMEOUT) or 86_400_000,
        # Sanic writes no Date header.
        generate_date_header=False, coalesce_notifications=True, probe_headers="none",
        # Http.set_header_max_size (Config._configure_header_size).
        max_header_bytes=max(1, min(int(config.REQUEST_MAX_HEADER_SIZE),
                                      int(config.REQUEST_BUFFER_SIZE) - 4096,
                                      max_body, _http1.Http.HEADER_CEILING)),
        keep_alive=bool(config.KEEP_ALIVE), framework_errors=True,
        fixed_responses=tuple((path, *response) for path, response in app.fixed_responses.items()),
        response_cache=bool(cached_routes(app)),
        keep_alive_timeout_ms=_timeout_ms(config.KEEP_ALIVE_TIMEOUT),
        # Sanic hands a response to its transport and closes an idle connection
        # after KEEP_ALIVE_TIMEOUT, waiting GRACEFUL_TCP_CLOSE_TIMEOUT for the
        # unsent rest: a peer that stops reading is cut off after about that.
        write_timeout_ms=_timeout_ms(float(config.KEEP_ALIVE_TIMEOUT)
                                     + float(config.GRACEFUL_TCP_CLOSE_TIMEOUT)),
        request_timeout_ms=_timeout_ms(config.REQUEST_TIMEOUT))
    backend = config.get("NATIVE_REACTOR_BACKEND")
    return Config(shard_cpus=shards, server=server, same_thread=same_thread(config),
                  **({} if backend is None else {"reactor_backend": str(backend)}))


def serving(loop):
    """Whether this process's native server is serving on `loop`."""
    return _engine is not None and not _engine.closed and _engine._loop is loop


# CPython's own first collector threshold (3.12; 3.13 and 3.14): a process
# still at it has not tuned its collector.
_DEFAULT_GC_THRESHOLD = 700 if sys.version_info < (3, 13) else 2000


def _tune_gc(app_config):
    """NATIVE_GC_THRESHOLD: the collector's first threshold from when the native
    host serves, while the process has Python's own. The objects of requests in
    flight are alive at each young collection: with hundreds in flight, Python's
    threshold collects every few milliseconds and promotes them, which soon
    starts full collections that scan every object of the program (several ms
    each, all requests in flight waiting, nothing freed). None or 0 leaves the
    collector as it is."""
    threshold = app_config.get("NATIVE_GC_THRESHOLD")
    first, *older = gc.get_threshold()
    if threshold and first == _DEFAULT_GC_THRESHOLD:
        gc.set_threshold(int(threshold), *older)


async def open_engine(app, config, *, python_cpu=None):
    """Start the native host for `app` after its Sanic startup and before
    listeners: lifecycle events stay with the caller. Returns the Engine.
    With python_cpu, this thread runs there from before the server serves."""
    if config.server is None or not config.server.application_dispatch_all:
        raise ValueError("the native host requires application_dispatch_all")
    if not config.server.framework_errors:
        # Sanic's error handler renders protocol errors.
        config = replace(config, server=replace(config.server, framework_errors=True))
    global _engine
    if _engine is not None and not _engine.closed:
        # A previous serve ended without closing its Engine (its loop was
        # interrupted), or another server was prepared: the native host serves
        # one server per process; stop that one before starting this one.
        error_logger.warning("Stopping the running native server: one native server per process")
        _engine.abandon()
    _tune_gc(app.config)
    # CORS (oldman_server.cors): the table finalize() compiled (compiled here
    # for a server created before the app starts), and the fixed responses'
    # policies; nothing when it is off.
    table = app._cors_table if app.state.is_started else cors_compile(app)
    if table is not None:
        fixed = tuple((path, *response, table.fixed.get(path, 0))
                      for path, response in app.fixed_responses.items())
        config = replace(config, server=replace(config.server, fixed_responses=fixed, **table.native()))
    max_response = config.server.max_response_bytes
    if app.state.is_started:
        adapter = NativeApplication(app, max_response)
        handler, error_handler = adapter.entry, adapter.error
        settles = adapter._flat
    else:
        # create_server(): the server listens before app startup. The adapter
        # reads the finalized app, so it is built at the first request.
        def adapter_now():
            adapter = NativeApplication(app, max_response)
            _engine._application_handler = adapter.entry
            _engine._application_settles = adapter._flat
            _engine._application_error_handler = adapter.error
            return adapter
        def handler(native):
            return adapter_now().entry(native)
        def error_handler(native):
            return adapter_now().error(native)
        settles = False
    def opened(engine):
        # Requests can arrive while the server starts (a worker sharing a unix
        # socket, say): the handlers reach the Engine through _engine.
        global _engine
        _engine = engine
        _compat.native_files = _NativeFiles(engine)
        if python_cpu is not None:
            os.sched_setaffinity(0, {python_cpu})

    # Persistent: the process can serve again (app.run, test clients) because
    # the native runtime keeps running between serves.
    try:
        return await Engine.open(
            config, application_handler=handler, application_bytes_headers=True, persistent=True,
            application_error_handler=error_handler, application_settles=settles,
            application_connections=server_types.native_connections, opened=opened)
    except BaseException:
        _engine = _compat.native_files = None
        raise


class _NativeFiles:
    """sanic.compat.native_files while an Engine serves: file stat and reads run
    on its file threads instead of aiofiles' thread pool."""
    __slots__ = ("_engine",)

    def __init__(self, engine):
        self._engine = engine

    def serving(self):
        engine = self._engine
        return not engine._closed and asyncio._get_running_loop() is engine._loop

    def stat(self, path):
        return self._engine.file_stat(path)

    def read(self, path, offset, length):
        return self._engine.file_read(path, offset, length)

    def open(self, path):
        return self._engine.file_open_stat(path)


class NativeServer:
    """asyncio.Server's interface over a native Engine, for app.create_server()."""

    def __init__(self, app, engine, sockets=()):
        self._app = app
        self._engine = engine
        self._sockets = sockets  # closed with the server, as asyncio's
        self._loop = engine._loop
        self._closing = None
        self._forever = None

    def is_serving(self):
        return self._closing is None

    def close(self):
        """Stop serving; wait_closed() waits for the drain and the native stop."""
        if self._closing is None:
            self._closing = self._loop.create_task(self._shutdown())
            if self._forever is not None and not self._forever.done():
                self._forever.cancel()

    async def _shutdown(self):
        try:
            await self._engine.drain_applications(self._app.config.GRACEFUL_SHUTDOWN_TIMEOUT)
            await self._engine.aclose()
        finally:
            if not self._engine.closed:
                self._engine.abandon()
            for sock in self._sockets:
                sock.close()

    async def wait_closed(self):
        if self._closing is not None:
            await asyncio.shield(self._closing)

    async def start_serving(self):
        pass  # Serving since creation, as asyncio's default start_serving=True.

    async def serve_forever(self):
        if self._closing is not None:
            raise RuntimeError(f"server {self!r} is closed")
        self._forever = self._loop.create_future()
        try:
            await self._forever
        except asyncio.CancelledError:
            try:
                self.close()
                await self.wait_closed()
            finally:
                raise
        finally:
            self._forever = None


async def create_server(app, config, sockets=()):
    """The serving half of app.create_server(): a NativeServer for AsyncioServer.
    sockets (the one config's listen_fd refers to, and this process's own
    NATIVE_WORKER_LISTEN socket) close with it."""
    try:
        return NativeServer(app, await open_engine(app, config), sockets)
    except BaseException:
        for sock in sockets:
            sock.close()
        raise


async def _cancel_tasks(app, owner):
    tasks = [task for task in app.tasks if task not in (asyncio.current_task(), owner)]
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    app.purge_tasks()


@asynccontextmanager
async def serve(app, config):
    """Own the native runtime and Sanic listeners, including failure cleanup."""
    if not isinstance(app, OldmanServer):
        raise TypeError("the native host serves an OldmanServer application")
    if app.state.is_started or app.state.native_loop is not None:
        raise RuntimeError("The application is already started")
    if config.server is None or not config.server.application_dispatch_all:
        raise ValueError("the native host requires application_dispatch_all")
    app.state.native_loop = asyncio.get_running_loop()
    info = ApplicationServerInfo(settings={"host": "0.0.0.0", "port": config.server.http_port, "version": 1})
    app.state.server_info.append(info)
    engine, initialized, started, errors = None, False, False, []
    try:
        await app._startup()
        initialized = True
        await app._server_event("init", "before")
        engine = await open_engine(app, config)
        info.stage = ServerStage.SERVING
        app.set_serving(True)
        started = True
        await app._server_event("init", "after")
        yield engine
    except BaseException as error:
        errors.append(error)
    finally:
        app.state.is_stopping = True
        owner = asyncio.current_task()
        async def cleanup():
            # Every step runs even if a listener fails. Caller cancellation must
            # not advance after_stop before Engine's shielded close completes.
            # Listeners pair up: before_server_stop once after_server_start
            # ran, after_server_stop once before_server_start did.
            actions = []
            if started:
                actions.append(lambda: app._server_event("shutdown", "before"))
            if engine is not None:
                actions.append(engine.aclose)
            actions.append(lambda: _cancel_tasks(app, owner))
            if initialized:
                actions.append(lambda: app._server_event("shutdown", "after"))
            for action in actions:
                try:
                    await action()
                except BaseException as error:
                    errors.append(error)
            info.stage = ServerStage.STOPPED
            app.set_serving(False)
            app.state.native_loop = None
            app.state.server_info.remove(info)
        closing = app.loop.create_task(cleanup())
        cancelled = any(isinstance(error, asyncio.CancelledError) for error in errors)
        while not closing.done():
            try:
                await asyncio.shield(closing)
            except asyncio.CancelledError as error:
                if not cancelled:
                    errors.append(error)
                    cancelled = True
        closing.result()
    if len(errors) == 1:
        raise errors[0]
    if errors:
        raise BaseExceptionGroup("native lifecycle failed", errors)


# NativeApplication._call_flat's request and response, as sanic_prepare and
# sanic_finish handle them.
_native.set_sanic_types(Request, _BufferedExchange, _NATIVE_UNSET, Stage.HANDLER, _PARSED_URLS,
                        Request._current, HTTPResponse, JSONResponse, Stage.IDLE, _SENT_CONTENT_TYPES,
                        _sent_content_type)
