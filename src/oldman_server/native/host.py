"""The application on the native host: each native request runs through the
framework's processing chain (routing, middleware, error handling) as a Task,
and its response, stream, WebSocket tunnel or file goes back to the shards.
"""
import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
import os
import socket
from inspect import isawaitable
from time import perf_counter
from types import CoroutineType

from oldman_server import OldmanServer
from oldman_server.application.constants import ServerStage
from oldman_server.application.state import ApplicationServerInfo
from oldman_server import compat as _compat
from oldman_server.compat import Header
from oldman_server.exceptions import (BadRequest, ExpectationFailed, PayloadTooLarge, RequestCancelled,
                              OldmanServerException, ServerError, ServiceUnavailable)
from oldman_server.helpers import has_message_body
from oldman_server.http import http1 as _http1
from oldman_server.http.constants import Stage
from oldman_server.log import access_logger, error_logger, websockets_logger
from oldman_server.middleware import Middleware
from oldman_server.models import server_types
from oldman_server.request import Request
from oldman_server.request.types import _NATIVE_UNSET
from oldman_server.response import BaseHTTPResponse, HTTPResponse, JSONResponse, ResponseStream
from oldman_server.response.types import HEADER_TRANSLATION_TABLE, _SentContentType
from oldman_server.router import Router
from oldman_server.touchup.schemes.base import BaseScheme
from sanic_routing.exceptions import NotFound

from . import _native
from .cpus import parse_cpus, shard_count, worker_group
from .engine import (BodyError, Config, Engine, ServerConfig, STREAM_DATA, STREAM_END,
                     STREAM_START)
from oldman_server.request import form as _form

try:  # websockets >= 11.0, as sanic.server.protocols.websocket_protocol
    from websockets.protocol import State as _WSState
    from websockets.server import ServerProtocol as _WSServerProtocol
except ImportError:  # websockets < 11.0
    from websockets.connection import State as _WSState
    from websockets.server import ServerConnection as _WSServerProtocol
from websockets.typing import Subprotocol


# Multipart forms are parsed natively when the result is the same (see
# native/multipart.hh); otherwise Sanic's parser runs.
_form.native_multipart = _native.parse_multipart

# The native host's current Engine; at most one serves at a time. Streamed
# responses queue their pieces through it.
_engine = None

# Exact fork types whose processed_headers/send are the BaseHTTPResponse ones;
# their headers are encoded inline. Subclasses keep the generic calls.
_COMPLETE_TYPES = frozenset((HTTPResponse, JSONResponse))
# Values that are never awaitable; skips inspect's ABC check on the hot path.
_NOT_AWAITABLE = frozenset((HTTPResponse, JSONResponse, type(None)))
# (name, value) str header pairs -> processed_headers encoding and lowered name.
# Common pairs repeat across responses; cleared when full to stay bounded.
_ENCODED_HEADERS = {}


# Exceptions Sanic's Http raises for what the native host rejects while reading
# a request, by status; the message is the host's (the same text as Sanic's).
_PROTOCOL_ERRORS = {400: BadRequest, 413: PayloadTooLarge, 417: ExpectationFailed}


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
    # request_max_size is only assigned (by Sanic, for stream=True handlers): the
    # body is already buffered within REQUEST_MAX_SIZE and read() returns it whole.
    __slots__ = ("stage", "response", "request_body", "body", "max_body", "result", "failure",
                 "native", "streaming", "more_body", "request_max_size")

    def __init__(self, body, max_body, native=None, more_body=False):
        self.stage = Stage.HANDLER
        self.response = None
        self.request_body = True
        self.body, self.max_body = body, max_body
        self.result = self.failure = None
        # Streamed responses (send with end_stream=False) go through the native
        # host's Engine with this request's id; `streaming` is None, "open" or "ended".
        self.native = native
        self.streaming = None
        # True while the request body is still arriving (body holds what came).
        self.more_body = more_body

    async def read(self):
        if not self.request_body:
            return None
        if self.more_body:
            if self.body:
                body, self.body = self.body, b""
                return body
            try:
                data = await _engine.receive(self.native.request_id)
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
        if self.response is None:
            self._fail("response is missing")
        if _engine is None or self.native is None:
            self._fail("streaming responses are not supported")
        response = self.response
        try:
            status = response.status
            if type(status) is not int or status < 200:
                status = _final_status(status)
            body = response.body or b""
            if type(body) is not bytes:
                body = _bytes_body(body)
            if type(data) is not bytes:
                data = _bytes_body(data)
            if len(body) + len(data) > self.max_body:
                self._fail("response body exceeds the native limit")
            first = body + data if data else body
            headers, lengths = self._encode_headers(response)
            if lengths and (len(set(lengths)) > 1 or not lengths[0].isdigit()):
                self._fail("inconsistent content-length")
        except Exception as error:
            self.failure = error
            self.stage = Stage.FAILED
            raise
        # Sanic's stage while a response is being sent: errors can no longer
        # produce an error response, only end the connection.
        self.streaming, self.stage = "open", Stage.RESPONSE
        await _engine.stream_send(
            self.native.request_id, self.native.shard, STREAM_START, first, status,
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
        if end_stream:
            self.streaming, self.stage = "ended", Stage.IDLE
        await _engine.stream_send(self.native.request_id, self.native.shard,
                                  STREAM_END if end_stream else STREAM_DATA, data)

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
        if not end_stream:
            self._fail("streaming responses are not supported")
        if self.response is None:
            self._fail("response is missing")
        try:
            body = self.response.body or b""
            if type(body) is not bytes:
                body = _bytes_body(body)
            if type(data) is not bytes:
                data = _bytes_body(data)
            if len(body) + len(data) > self.max_body:
                self._fail("response body exceeds the native limit")
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
        except Exception as error:
            self.failure = error
            self.stage = Stage.FAILED
            raise

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
                        self._fail("response headers exceed the native limit")
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
                        self._fail("response headers exceed the native limit")
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
        await _engine.stream_file(self.native.request_id, self.native.shard, path, offset, length,
                                  chunk_size)

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
        self.streaming, self.stage = "open", Stage.RESPONSE
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
        self._flat = self._flat_supported(app)
        # Route tables may be reset and refinalized; their Host demand is read
        # per request. Router methods replaced after startup are not supported.
        self._standard_router = self._flat and app.router._native_routing_methods_standard
        # Host-free router.get() results by (raw path bytes, method) for one
        # route-table generation; misses and errors always use router.get().
        self._routes, self._routes_router, self._routes_generation = {}, None, None

    @staticmethod
    def _flat_supported(app):
        """Whether _serve_flat reproduces handle_request for this app.

        Decided once at startup: replacing these methods, the router or the
        request class, or registering signals, after startup is not supported.
        """
        app_class = type(app)
        return (app.config.TOUCHUP is True and app.signal_router.finalized
                and (app.request_class or Request) is Request
                and type(app.router) is Router
                and all(getattr(app_class, name) is getattr(OldmanServer, name) for name in (
                    "handle_request", "handle_exception", "dispatch",
                    "_run_request_middleware", "_run_response_middleware"))
                # handle_request, the middleware runners and Request.respond only
                # dispatch http.* events; TouchUp removes them when unregistered.
                and not any(route.name.startswith(("http", "<"))
                            for route in app.signal_router.routes))

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
        exchange = _BufferedExchange(native.body, self.max_response_bytes, native, native.more_body)
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
            return result
        finally:
            request_class._current.reset(token)
            if exchange.streaming is not None:
                exchange.abort()  # a stream still open when the chain exits
            if exchange.response is not None:
                exchange.response.stream = None

    async def _call_flat(self, native):
        """__call__ for an app without http.* signals (see _flat_supported), with
        OldmanServer.handle_request, Request.respond, both middleware runners and
        BaseHTTPResponse.send inlined into this one coroutine.

        Each step keeps the original order, state changes, exception handling
        and log messages; only the per-stage coroutines, the dispatch calls that
        TouchUp removes, and ABC awaitable checks on known types are gone.
        """
        app = self.app
        started = perf_counter() if self._access_log else 0.0
        try:
            # Request._from_native with one validating native call.
            target, version, method, body, more_body = native.sanic_request()
            request = Request._from_native_fields(target, version, method, app, native)
        except OldmanServerException as error:
            return await self._error_response(native, error)
        exchange = _BufferedExchange(body, self.max_response_bytes, native, more_body)
        request.stream = exchange
        token = Request._current.set(request)
        try:
            # ---- Sanic.handle_request ----
            run_middleware = True
            try:
                if app.path_rewriters:
                    app._rewrite_request_path(request)
                router = app.router
                if (self._standard_router and not router._routing_requires_host
                        and request._headers is _NATIVE_UNSET):
                    routes = self._routes
                    if (router is not self._routes_router
                            or router._native_generation != self._routes_generation):
                        routes.clear()
                        self._routes_router = router
                        self._routes_generation = router._native_generation
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
                extra = route.extra
                if exchange.request_body and not extra.ignore_body:
                    if hasattr(handler, "is_stream"):
                        # The handler reads request.stream itself (one buffered chunk).
                        exchange.request_max_size = float("inf")
                    elif not request.body:
                        if exchange.more_body:
                            # Request.receive_body(): the rest is still arriving.
                            request.body = b"".join([data async for data in exchange])
                        else:
                            # Request.receive_body() over the one buffered chunk.
                            request.body = exchange.body
                            exchange.body, exchange.request_body = b"", False

                run_middleware = False
                response = None
                middleware = extra.request_middleware
                if middleware:
                    # ---- Sanic._run_request_middleware ----
                    request._request_middleware_started = True
                    for item in middleware:
                        result = item.func(request) if type(item) is Middleware else item(request)
                        if type(result) is CoroutineType or (
                                type(result) not in _NOT_AWAITABLE and isawaitable(result)):
                            result = await result
                        if result:
                            response = result
                            break

                if not response:
                    if handler is None:
                        raise ServerError(
                            "'None' was returned while requesting a handler from the router")
                    response = handler(request, **request._match_info)
                    if type(response) is CoroutineType or (
                            type(response) not in _NOT_AWAITABLE and isawaitable(response)):
                        response = await response

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
                    if isinstance(response, BaseHTTPResponse) and request.stream:
                        response = request.stream.respond(response)
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
                            result = (item.func(request, final) if type(item) is Middleware
                                      else item(request, final))
                            if type(result) is CoroutineType or (
                                    type(result) not in _NOT_AWAITABLE
                                    and isawaitable(result)):
                                result = await result
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
                    if (response.stream is exchange and type(response) in _COMPLETE_TYPES
                            and exchange.streaming is None):
                        exchange.complete()  # BaseHTTPResponse.send(end_stream=True)
                    else:
                        await response.send(end_stream=True)
                elif isinstance(response, ResponseStream):
                    await response(request)
                    await response.eof()
                elif not hasattr(handler, "is_websocket"):
                    raise ServerError(f"Invalid response type {response!r} (need HTTPResponse)")
            except asyncio.CancelledError as error:
                await self._cancelled(request, exchange, error)
            except Exception as error:
                await app.handle_exception(request, error, run_middleware=run_middleware)
            if self._access_log:
                result = exchange.finish()
                _log_access(request, exchange, started)
                return result
            return exchange.finish()
        finally:
            Request._current.reset(token)
            if exchange.streaming is not None:
                exchange.abort()  # a stream still open when the chain exits
            if exchange.response is not None:
                exchange.response.stream = None

def _shard_cpus(config):
    """In a server worker process, its group's shard CPUs (see cpus.py); else
    NATIVE_SHARD_CPUS (CPU list or "1,2") or the last NATIVE_SHARDS (default
    up to 2) CPUs of this process's affinity set, leaving the rest to Python."""
    group = worker_group(config)
    if group:
        return group[1:]
    explicit = parse_cpus(config.get("NATIVE_SHARD_CPUS"))
    if explicit is not None:
        return explicit
    available = sorted(os.sched_getaffinity(0))
    return tuple(available[-shard_count(config, len(available)):])


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
    max_body = min(int(config.REQUEST_MAX_SIZE), 128 * 1024 * 1024)
    memory = int(config.get("NATIVE_MAX_MEMORY") or
                 max(1 << 30, len(shards) * (2 * max_body + (1 << 20))))
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
        max_response_bytes=int(config.get("NATIVE_MAX_RESPONSE_SIZE") or 128 * 1024 * 1024),
        decision_timeout_ms=min(86_400_000, max(1, int(float(config.RESPONSE_TIMEOUT) * 1000))),
        # Sanic writes no Date header.
        generate_date_header=False, coalesce_notifications=True, probe_headers="none",
        # Http.set_header_max_size (Config._configure_header_size).
        max_header_bytes=max(1, min(int(config.REQUEST_MAX_HEADER_SIZE),
                                      int(config.REQUEST_BUFFER_SIZE) - 4096,
                                      int(config.REQUEST_MAX_SIZE), _http1.Http.HEADER_CEILING)),
        keep_alive=bool(config.KEEP_ALIVE), framework_errors=True,
        keep_alive_timeout_ms=int(float(config.KEEP_ALIVE_TIMEOUT) * 1000),
        # Sanic hands a response to its transport and closes an idle connection
        # after KEEP_ALIVE_TIMEOUT, waiting GRACEFUL_TCP_CLOSE_TIMEOUT for the
        # unsent rest: a peer that stops reading is cut off after about that.
        write_timeout_ms=min(int((float(config.KEEP_ALIVE_TIMEOUT)
                                  + float(config.GRACEFUL_TCP_CLOSE_TIMEOUT)) * 1000), 86_400_000),
        request_timeout_ms=int(float(config.REQUEST_TIMEOUT) * 1000))
    backend = config.get("NATIVE_REACTOR_BACKEND")
    return Config(shard_cpus=shards, server=server,
                  **({} if backend is None else {"reactor_backend": str(backend)}))


def serving(loop):
    """Whether this process's native server is serving on `loop`."""
    return _engine is not None and not _engine.closed and _engine._loop is loop


async def open_engine(app, config, *, native_responses=False, python_cpu=None):
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
    max_response = config.server.max_response_bytes
    if app.state.is_started:
        adapter = NativeApplication(app, max_response)
        handler, error_handler = adapter.entry, adapter.error
    else:
        # create_server(): the server listens before app startup. The adapter
        # reads the finalized app, so it is built at the first request.
        def adapter_now():
            adapter = NativeApplication(app, max_response)
            _engine._application_handler = adapter.entry
            _engine._application_error_handler = adapter.error
            return adapter
        def handler(native):
            return adapter_now().entry(native)
        def error_handler(native):
            return adapter_now().error(native)
    def opened(engine):
        # Requests can arrive while the server starts (a worker sharing a unix
        # socket, say): the handlers reach the Engine through _engine.
        global _engine
        _engine = engine
        _compat.native_files = _NativeFiles(engine)
        if python_cpu is not None:
            os.sched_setaffinity(0, {python_cpu})

    # Persistent: the process can serve again (app.run, test clients) because
    # Seastar keeps running between serves.
    try:
        return await Engine.open(
            config, application_handler=handler, application_native_results=native_responses,
            application_bytes_headers=True, persistent=True,
            application_error_handler=error_handler,
            application_connections=server_types.native_connections, opened=opened)
    except BaseException:
        _engine = _compat.native_files = None
        raise


class _NativeFiles:
    """sanic.compat.native_files while an Engine serves: file stat and reads run
    on its shards (Seastar's file API) instead of aiofiles' thread pool."""
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
    sockets (the one config's listen_fd refers to, and the address
    reservation it listens on) close with it."""
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
async def serve(app, config, *, native_responses=False):
    """Own the native runtime and Sanic listeners, including failure cleanup.

    native_responses opts into the experimental ApplicationResponseBatch path.
    The checked tuple path with encoded bytes headers remains the default.
    """
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
        engine = await open_engine(app, config, native_responses=native_responses)
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
