from __future__ import annotations

from asyncio import BaseProtocol
from collections import defaultdict
from collections.abc import Mapping
from contextlib import suppress
from contextvars import ContextVar
from inspect import isawaitable
from types import SimpleNamespace
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    cast,
)

from oldman_server.routing.route import Route
from typing_extensions import TypeVar

from oldman_server.http.stream import Stream
from oldman_server.models.http_types import Credentials
from oldman_server.models.server_types import ConnInfo, native_connection


if TYPE_CHECKING:
    from oldman_server.app import OldmanServer
    from oldman_server.config import Config

import uuid

from urllib.parse import parse_qs, parse_qsl, urlunparse

from httptools import parse_url
from httptools.parser.errors import HttpParserInvalidURLError

from oldman_server.compat import CancelledErrors, Header, NativeHeaders
from oldman_server.constants import (
    CACHEABLE_HTTP_METHODS,
    DEFAULT_HTTP_CONTENT_TYPE,
    IDEMPOTENT_HTTP_METHODS,
    SAFE_HTTP_METHODS,
)
from oldman_server.cookies.request import CookieRequestParameters, parse_cookie
from oldman_server.exceptions import BadRequest, BadURL, ServerError, URITooLong
from oldman_server.headers import (
    AcceptList,
    Options,
    parse_accept,
    parse_content_header,
    parse_credentials,
    parse_forwarded,
    parse_host,
    parse_xforwarded,
    trusted_peer,
)
from oldman_server.http import Stage
from oldman_server.log import error_logger
from oldman_server.models.protocol_types import TransportProtocol
from oldman_server.response import BaseHTTPResponse, HTTPResponse

from .form import parse_multipart_form
from .parameters import RequestParameters


from orjson import loads as json_loads

if TYPE_CHECKING:
    # The default argument of TypeVar is proposed to be added in Python 3.13
    # by PEP 696 (https://www.python.org/dev/peps/pep-0696/).
    # Therefore, we use typing_extensions.TypeVar for compatibility.
    # For more information, see:
    # https://discuss.python.org/t/pep-696-type-defaults-for-typevarlikes
    sanic_type = TypeVar(
        "sanic_type", bound=OldmanServer, default=OldmanServer[Config, SimpleNamespace]
    )
    ctx_type = TypeVar(
        "ctx_type", bound=SimpleNamespace, default=SimpleNamespace
    )
else:
    sanic_type = TypeVar("sanic_type")
    ctx_type = TypeVar("ctx_type")


_NATIVE_UNSET = object()

# Request target bytes -> its parse_url() result, for the native host's
# requests: a URL is immutable (a rewrite replaces it), so requests for the same
# target share one. Bounded in entries and in target length; invalid targets
# are not kept.
_PARSED_URLS: dict[bytes, Any] = {}
_PARSED_URL_MAX = 256


class Request(Generic[sanic_type, ctx_type]):
    """State of HTTP request.

    Args:
        url_bytes (bytes): Raw URL bytes.
        headers (Header): Request headers.
        version (str): HTTP version.
        method (str): HTTP method.
        transport (TransportProtocol): Transport protocol.
        app (OldmanServer): Application instance.
        head (bytes, optional): Request head. Defaults to `b""`.
    """

    _current: ContextVar[Request] = ContextVar("request")
    # orjson: integers beyond 64 bits become float (precision is lost) and
    # NaN/Infinity are refused. Accepted for its speed and documented
    # (docs/en/differences.md); an application needing exact big integers
    # passes loads=json.loads.
    _loads = json_loads

    __slots__ = (
        "__weakref__",
        "_cookies",
        "_ctx",
        "_id",
        "_ip",
        "_parsed_url",
        "_port",
        "_protocol",
        "_remote_addr",
        "_request_middleware_started",
        "_response_middleware_started",
        # Set by app.handle_exception while it sends the error response; unset
        # (read with getattr) otherwise, so ordinary requests pay nothing.
        "_handling_exception",
        "_scheme",
        "_socket",
        "_match_info",
        "_name",
        "app",
        "body",
        "_conn_info",
        "_head",
        "_headers",
        "_native_request",
        "method",
        "parsed_accept",
        "_parsed_args",
        "parsed_cookies",
        "parsed_credentials",
        "parsed_files",
        "parsed_form",
        "parsed_forwarded",
        "parsed_json",
        "_parsed_not_grouped_args",
        "parsed_token",
        "raw_url",
        "responded",
        "route",
        "stream",
        "_transport",
        "version",
    )

    def __init__(
        self,
        url_bytes: bytes,
        headers: Header,
        version: str,
        method: str,
        transport: TransportProtocol,
        app: sanic_type,
        head: bytes = b"",
        *,
        _native_request: Any = None,
    ):
        self.raw_url = url_bytes
        # httptools.parse_url stores URL component offsets and lengths as
        # uint16, so a longer URL would silently wrap and truncate the path
        # or query string. Reject it instead of routing on a corrupted path.
        if len(url_bytes) > 65535:
            raise URITooLong("URL exceeds the maximum size of 65535 bytes")
        try:
            self._parsed_url = parse_url(url_bytes)
        except HttpParserInvalidURLError:
            url = url_bytes.decode(errors="backslashreplace")
            raise BadURL(f"Bad URL: {url}")
        self._id: uuid.UUID | str | int | None = None
        self._name: str | None = None
        self.app = app

        self._native_request = _native_request
        if _native_request is None:
            self.headers = Header(headers)
        else:
            self._headers = _NATIVE_UNSET
        self.version = version
        self.method = method
        if _native_request is None:
            self.transport = transport
            self._head = head
        else:
            self._transport = _NATIVE_UNSET
            self._head = head or _NATIVE_UNSET

        # Init but do not inhale
        self.body = b""
        if _native_request is None:
            self.conn_info = None
        else:
            self._conn_info = _NATIVE_UNSET
        self._ctx: ctx_type | None = None
        self.parsed_accept: AcceptList | None = None
        lazy_query_caches = _native_request is not None and type(self) is Request
        if lazy_query_caches:
            self._parsed_args = _NATIVE_UNSET
        else:
            self.parsed_args = defaultdict(RequestParameters)
        self.parsed_cookies: RequestParameters | None = None
        self.parsed_credentials: Credentials | None = None
        self.parsed_files: RequestParameters | None = None
        self.parsed_form: RequestParameters | None = None
        self.parsed_forwarded: Options | None = None
        self.parsed_json = None
        if lazy_query_caches:
            self._parsed_not_grouped_args = _NATIVE_UNSET
        else:
            self.parsed_not_grouped_args = defaultdict(list)
        self.parsed_token: str | None = None
        self._request_middleware_started = False
        self._response_middleware_started = False
        self.responded: bool = False
        self.route: Route | None = None
        self.stream: Stream | None = None
        self._match_info: dict[str, Any] = {}
        self._protocol: BaseProtocol | None = None

    @classmethod
    def _from_native(cls, native: Any, app: sanic_type):
        native.validate_sanic_request()
        return cls(
            native.target, None, native.http_version, native.method, None, app,
            _native_request=native,
        )

    @classmethod
    def _from_native_fields(
        cls, url_bytes: bytes, version: str, method: str, app: sanic_type,
        native: Any,
    ):
        """``Request(url_bytes, None, version, method, None, app,
        _native_request=native)`` for the exact class, assigning the same
        slots without the general constructor's branches."""
        if cls is not Request:
            return cls(url_bytes, None, version, method, None, app,
                       _native_request=native)
        if len(url_bytes) > 65535:
            raise URITooLong("URL exceeds the maximum size of 65535 bytes")
        self = object.__new__(cls)
        parsed = _PARSED_URLS.get(url_bytes)
        if parsed is None:
            try:
                parsed = parse_url(url_bytes)
            except HttpParserInvalidURLError:
                url = url_bytes.decode(errors="backslashreplace")
                raise BadURL(f"Bad URL: {url}")
            if len(url_bytes) <= _PARSED_URL_MAX:
                if len(_PARSED_URLS) >= 1024:
                    _PARSED_URLS.clear()
                _PARSED_URLS[url_bytes] = parsed
        self._parsed_url = parsed
        self.raw_url = url_bytes
        self._id = self._name = None
        self.app = app
        self._native_request = native
        self._headers = self._transport = self._conn_info = _NATIVE_UNSET
        self.version = version
        self.method = method
        self._head = _NATIVE_UNSET
        self.body = b""
        self._ctx = self.parsed_accept = None
        self._parsed_args = self._parsed_not_grouped_args = _NATIVE_UNSET
        self.parsed_cookies = self.parsed_credentials = self.parsed_files = None
        self.parsed_form = self.parsed_forwarded = self.parsed_json = None
        self.parsed_token = None
        self._request_middleware_started = False
        self._response_middleware_started = False
        self.responded = False
        self.route = self.stream = self._protocol = None
        self._match_info = {}
        return self

    @property
    def parsed_args(self) -> defaultdict[
        tuple[bool, bool, str, str], RequestParameters
    ]:
        if self._parsed_args is _NATIVE_UNSET:
            self._parsed_args = defaultdict(RequestParameters)
        return self._parsed_args

    @parsed_args.setter
    def parsed_args(self, value):
        self._parsed_args = value

    @parsed_args.deleter
    def parsed_args(self):
        del self._parsed_args

    @property
    def parsed_not_grouped_args(self) -> defaultdict[
        tuple[bool, bool, str, str], list[tuple[str, str]]
    ]:
        if self._parsed_not_grouped_args is _NATIVE_UNSET:
            self._parsed_not_grouped_args = defaultdict(list)
        return self._parsed_not_grouped_args

    @parsed_not_grouped_args.setter
    def parsed_not_grouped_args(self, value):
        self._parsed_not_grouped_args = value

    @parsed_not_grouped_args.deleter
    def parsed_not_grouped_args(self):
        del self._parsed_not_grouped_args

    @property
    def headers(self) -> Header:
        if self._headers is _NATIVE_UNSET:
            self._headers = NativeHeaders(self._native_request)
        return self._headers

    @headers.setter
    def headers(self, value):
        self._headers = value

    def _set_path(self, path: str) -> None:
        """Route and report the request under `path` (app.rewrite_path): the
        query string and fragment stay, raw_url keeps what the client sent."""
        if not isinstance(path, str) or not path.startswith("/"):
            raise ServerError(f"A rewritten path must start with '/': {path!r}")
        url = self._parsed_url
        target = path.encode("utf-8")
        if url.schema:  # an absolute-form request target
            authority = url.host or b""
            if url.port:
                authority += b":%d" % url.port
            target = url.schema + b"://" + authority + target
        if url.query:
            target += b"?" + url.query
        if url.fragment:
            target += b"#" + url.fragment
        try:
            self._parsed_url = parse_url(target)
        except HttpParserInvalidURLError:
            raise ServerError(f"A rewritten path is not a valid URL path: {path!r}")

    def _get_routing_host(self):
        if self._native_request is not None and self._headers is _NATIVE_UNSET:
            value = self._native_request.header_bytes("host")
            return value.decode("utf-8", errors="surrogateescape") if value is not None else None
        return self.headers.getone("host", None)

    @staticmethod
    def _native_head(native: Any) -> bytes:
        """A native request's request line and header fields, rebuilt from the
        parsed ones (the host does not keep the bytes it received)."""
        lines = [b"%s %s HTTP/%s" % (native.method.encode(), native.target,
                                      native.http_version.encode())]
        lines.extend(name + b": " + value for name, value in native.headers)
        return b"\r\n".join(lines)

    @property
    def head(self) -> bytes:
        """The request line and headers (for the native host, rebuilt from them)."""
        if self._head is _NATIVE_UNSET:
            self._head = self._native_head(self._native_request)
        return self._head

    @head.setter
    def head(self, value):
        self._head = value

    @property
    def conn_info(self) -> ConnInfo | None:
        if self._conn_info is _NATIVE_UNSET:
            self._conn_info = native_connection(self._native_request).conn_info
        return self._conn_info

    @conn_info.setter
    def conn_info(self, value):
        self._conn_info = value

    @property
    def transport(self) -> TransportProtocol:
        if self._transport is _NATIVE_UNSET:
            self._transport = native_connection(self._native_request).transport
        return self._transport

    @transport.setter
    def transport(self, value):
        self._transport = value

    def __repr__(self):
        class_name = self.__class__.__name__
        return f"<{class_name}: {self.method} {self.path}>"

    @staticmethod
    def make_context() -> ctx_type:
        """Create a new context object.

        This method is called when a new request context is pushed. It is
        a great candidate for overriding in a subclass if you want to
        control the type of context object that is created.

        By default, it returns a `types.SimpleNamespace` instance.

        Returns:
            ctx_type: A new context object.
        """
        return cast(ctx_type, SimpleNamespace())

    @classmethod
    def get_current(cls) -> Request:
        """Retrieve the current request object

        This implements [Context Variables](https://docs.python.org/3/library/contextvars.html)
        to allow for accessing the current request from anywhere.

        A typical usecase is when you want to access the current request
        from a function that is not a handler, such as a logging function:

        ```python
        import logging

        class LoggingFormater(logging.Formatter):
            def format(self, record):
                request = Request.get_current()
                record.url = request.url
                record.ip = request.ip
                return super().format(record)
        ```

        Returns:
            Request: The current request object

        Raises:
            oldman_server.exceptions.ServerError: If it is outside of a request
                lifecycle.
        """  # noqa: E501
        request = cls._current.get(None)
        if not request:
            raise ServerError("No current request")
        return request

    @classmethod
    def generate_id(*_) -> uuid.UUID | str | int:
        """Generate a unique ID for the request.

        This method is called to generate a unique ID for each request.
        By default, it returns a `uuid.UUID` instance.

        Returns:
            uuid.UUID | str | int: A unique ID for the request.
        """
        return uuid.uuid4()

    @property
    def ctx(self) -> ctx_type:
        """The current request context.

        This is a context object for the current request. It is created
        by `Request.make_context` and is a great place to store data
        that you want to be accessible during the request lifecycle.

        Returns:
            ctx_type: The current request context.
        """
        if not self._ctx:
            self._ctx = self.make_context()
        return self._ctx

    def reset_response(self) -> None:
        """Reset the response object.

        This clears much of the state of the object. It should
        generally not be called directly, but is called automatically as
        part of the request lifecycle.

        Raises:
            oldman_server.exceptions.ServerError: If the response has already been
                sent.
        """
        try:
            if (
                self.stream is not None
                and self.stream.stage is not Stage.HANDLER
            ):
                raise ServerError(
                    "Cannot reset response because previous response was sent."
                )
            self.stream.response.stream = None  # type: ignore
            self.stream.response = None  # type: ignore
            self.responded = False
        except AttributeError:
            pass

    async def respond(
        self,
        response: BaseHTTPResponse | None = None,
        *,
        status: int = 200,
        headers: Mapping[str, str] | None = None,
        content_type: str | None = None,
    ):
        """Respond to the request without returning.

        This method can only be called once, as you can only respond once.
        If no ``response`` argument is passed, one will be created from the
        ``status``, ``headers`` and ``content_type`` arguments.

        **The first typical usecase** is if you wish to respond to the
        request without returning from the handler:

        ```python
        @app.get("/")
        async def handler(request: Request):
            data = ...  # Process something

            json_response = json({"data": data})
            await request.respond(json_response)

        @app.on_response
        async def add_header(_, response: HTTPResponse):
            # Middlewares still get executed as expected
            response.headers["one"] = "two"
        ```

        **The second possible usecase** is for when you want to directly
        respond to the request:

        ```python
        response = await request.respond(content_type="text/csv")
        await response.send("foo,")
        await response.send("bar")

        # You can control the completion of the response by calling
        # the 'eof()' method:
        await response.eof()
        ```

        Args:
            response (ResponseType): Response instance to send.
            status (int): Status code to return in the response.
            headers (Mapping[str, str] | None): Headers to return in the response, copied; defaults to None.
            content_type (str | None): Content-Type header of the response, defaults to None.

        Returns:
            FinalResponseType: Final response being sent (may be different from the
                "response" parameter because of middlewares), which can be
                used to manually send data.
        """  # noqa: E501
        try:
            if self.stream is not None and self.stream.response:
                raise ServerError("Second respond call is not allowed.")
        except AttributeError:
            pass
        # This logic of determining which response to use is subject to change
        if response is None:
            response = HTTPResponse(
                status=status,
                headers=headers,
                content_type=content_type,
            )

        # Connect the response
        if isinstance(response, BaseHTTPResponse) and self.stream:
            response = self.stream.respond(response)

            if isawaitable(response):
                response = await response  # type: ignore
        # Run response middleware
        try:
            middleware = (
                self.route and self.route.extra.response_middleware
            ) or self.app.response_middleware
            if middleware and not self._response_middleware_started:
                self._response_middleware_started = True
                response = await self.app._run_response_middleware(
                    self, response, middleware
                )
        except CancelledErrors:
            raise
        except Exception:
            if not getattr(self, "_handling_exception", False):
                # Like an exception from the handler: app.handle_exception
                # answers with the error handler, without this middleware.
                # Nothing is sent yet: the handler may catch this and
                # answer otherwise (the middleware does not run again).
                with suppress(ServerError):
                    self.reset_response()
                raise
            # Already the error response: send it.
            error_logger.exception(
                "Exception occurred in one of response middleware handlers"
            )
        self.responded = True
        return response

    async def receive_body(self):
        """Receive request.body, if not already received.

        Streaming handlers may call this to receive the full body. The framework calls
        this function before running any handlers of non-streaming routes.

        Custom request classes can override this for custom handling of both
        streaming and non-streaming routes.
        """
        if not self.body:
            self.body = b"".join([data async for data in self.stream])

    @property
    def name(self) -> str | None:
        """The route name

        In the following pattern:

        ```
        <AppName>.[<BlueprintName>.]<HandlerName>
        ```

        Returns:
            str | None: The route name
        """
        if self._name:
            return self._name
        elif self.route:
            return self.route.name
        return None

    @property
    def endpoint(self) -> str | None:
        """Alias of `oldman_server.request.Request.name`

        Returns:
            str | None: The route name
        """
        return self.name

    @property
    def uri_template(self) -> str | None:
        """The defined URI template

        Returns:
            str | None: The defined URI template
        """
        if self.route:
            return f"/{self.route.path}"
        return None

    @property
    def protocol(self) -> TransportProtocol:
        """The HTTP protocol instance

        Returns:
            Protocol: The HTTP protocol instance
        """
        if not self._protocol:
            if self._native_request is not None:
                # Like a closed transport's get_protocol(): None once closed.
                connection = native_connection(self._native_request)
                self._protocol = None if connection.closed else connection
            else:
                self._protocol = self.transport.get_protocol()
        return self._protocol  # type: ignore

    @property
    def raw_headers(self) -> bytes:
        """The unparsed HTTP headers

        Returns:
            bytes: The unparsed HTTP headers
        """
        _, headers = self.head.split(b"\r\n", 1)
        return bytes(headers)

    @property
    def request_line(self) -> bytes:
        """The first line of a HTTP request

        Returns:
            bytes: The first line of a HTTP request
        """
        reqline, _ = self.head.split(b"\r\n", 1)
        return bytes(reqline)

    @property
    def id(self) -> uuid.UUID | str | int | None:
        """A request ID passed from the client, or generated from the backend.

        By default, this will look in a request header defined at:
        `self.app.config.REQUEST_ID_HEADER`. It defaults to
        `X-Request-ID`. The framework will try to cast the ID into a `UUID` or an
        `int`.

        If there is not a UUID from the client, then the framework will try
        to generate an ID by calling `Request.generate_id()`. The default
        behavior is to generate a `UUID`. You can customize this behavior
        by subclassing `Request` and overwriting that method.

        ```python
        from oldman_server import OldmanServer, Request
        from itertools import count

        class IntRequest(Request):
            counter = count()

            def generate_id(self):
                return next(self.counter)

        app = OldmanServer("MyApp", request_class=IntRequest)
        ```

        Returns:
            uuid.UUID | str | int | None: A request ID passed from the
                client, or generated from the backend.
        """
        if not self._id:
            self._id = self.headers.getone(
                self.app.config.REQUEST_ID_HEADER,
                self.__class__.generate_id(self),  # type: ignore
            )

            # Try casting to a UUID or an integer
            if isinstance(self._id, str):
                try:
                    self._id = uuid.UUID(self._id)
                except ValueError:
                    try:
                        self._id = int(self._id)  # type: ignore
                    except ValueError:
                        ...

        return self._id  # type: ignore

    @property
    def json(self) -> Any:
        """The request body parsed as JSON

        Returns:
            Any: The request body parsed as JSON
        """
        if self.parsed_json is None:
            self.load_json()

        return self.parsed_json

    def load_json(self, loads=None) -> Any:
        """Load the request body as JSON

        Args:
            loads (Callable, optional): A custom JSON loader. Defaults to None.

        Raises:
            BadRequest: If the request body cannot be parsed as JSON

        Returns:
            Any: The request body parsed as JSON
        """
        try:
            if not loads:
                loads = self.__class__._loads

            self.parsed_json = loads(self.body)
        except Exception:
            if not self.body:
                return None
            raise BadRequest("Failed when parsing body as json")

        return self.parsed_json

    @property
    def accept(self) -> AcceptList:
        """Accepted response content types.

        A convenience handler for easier RFC-compliant matching of MIME types,
        parsed as a list that can match wildcards and includes */* by default.

        Returns:
            AcceptList: Accepted response content types
        """
        if self.parsed_accept is None:
            self.parsed_accept = parse_accept(self.headers.get("accept"))
        return self.parsed_accept

    @property
    def token(self) -> str | None:
        """Attempt to return the auth header token.

        Returns:
            str | None: The auth header token
        """
        if self.parsed_token is None:
            prefixes = ("Bearer", "Token")
            _, token = parse_credentials(
                self.headers.getone("authorization", None), prefixes
            )
            self.parsed_token = token
        return self.parsed_token

    @property
    def credentials(self) -> Credentials | None:
        """Attempt to return the auth header value.

        Covers NoAuth, Basic Auth, Bearer Token, Api Token authentication
        schemas.

        Returns:
            Credentials | None: A Credentials object with token, or username
                and password related to the request
        """
        if self.parsed_credentials is None:
            try:
                prefix, credentials = parse_credentials(
                    self.headers.getone("authorization", None)
                )
                if credentials:
                    self.parsed_credentials = Credentials(
                        auth_type=prefix, token=credentials
                    )
            except ValueError:
                pass
        return self.parsed_credentials

    def get_form(
        self, keep_blank_values: bool = False
    ) -> RequestParameters | None:
        """Method to extract and parse the form data from a request.

        Args:
            keep_blank_values (bool): Whether to discard blank values from the form data.

        Returns:
            RequestParameters | None: The parsed form data.
        """  # noqa: E501
        self.parsed_form = RequestParameters()
        self.parsed_files = RequestParameters()
        content_type = self.headers.getone(
            "content-type", DEFAULT_HTTP_CONTENT_TYPE
        )
        content_type, parameters = parse_content_header(content_type)
        try:
            if content_type == "application/x-www-form-urlencoded":
                self.parsed_form = RequestParameters(
                    parse_qs(
                        self.body.decode("utf-8"),
                        keep_blank_values=keep_blank_values,
                    )
                )
            elif content_type == "multipart/form-data":
                # TODO: Stream this instead of reading to/from memory
                boundary = parameters["boundary"].encode(  # type: ignore
                    "utf-8"
                )  # type: ignore
                self.parsed_form, self.parsed_files = parse_multipart_form(
                    self.body, boundary
                )
        except Exception:
            error_logger.exception("Failed when parsing form")

        return self.parsed_form

    @property
    def form(self) -> RequestParameters | None:
        """The request body parsed as form data

        Returns:
            RequestParameters | None: The request body parsed as form data
        """
        if self.parsed_form is None:
            self.get_form()

        return self.parsed_form

    @property
    def files(self) -> RequestParameters | None:
        """The request body parsed as uploaded files

        Returns:
            RequestParameters | None: The request body parsed as uploaded files
        """  # noqa: E501
        if self.parsed_files is None:
            self.form  # compute form to get files

        return self.parsed_files

    def get_args(
        self,
        keep_blank_values: bool = False,
        strict_parsing: bool = False,
        encoding: str = "utf-8",
        errors: str = "replace",
    ) -> RequestParameters:
        """Parse `query_string` using `urllib.parse.parse_qs`.

        This methods is used by the `args` property, but it also
        can be used directly if you need to change default parameters.

        Args:
            keep_blank_values (bool): Flag indicating whether blank values in
                percent-encoded queries should be treated as blank strings.
                A `True` value indicates that blanks should be retained as
                blank strings. The default `False` value indicates that
                blank values are to be ignored and treated as if they were
                not included.
            strict_parsing (bool): Flag indicating what to do with parsing
                errors. If `False` (the default), errors are silently ignored.
                If `True`, errors raise a `ValueError` exception.
            encoding (str): Specify how to decode percent-encoded sequences
                into Unicode characters, as accepted by the
                `bytes.decode()` method.
            errors (str): Specify how to decode percent-encoded sequences
                into Unicode characters, as accepted by the
                `bytes.decode()` method.

        Returns:
            RequestParameters: A dictionary containing the parsed arguments.
        """
        if (
            keep_blank_values,
            strict_parsing,
            encoding,
            errors,
        ) not in self.parsed_args:
            if self.query_string:
                self.parsed_args[
                    (keep_blank_values, strict_parsing, encoding, errors)
                ] = RequestParameters(
                    parse_qs(
                        qs=self.query_string,
                        keep_blank_values=keep_blank_values,
                        strict_parsing=strict_parsing,
                        encoding=encoding,
                        errors=errors,
                    )
                )

        return self.parsed_args[
            (keep_blank_values, strict_parsing, encoding, errors)
        ]

    args = property(get_args)
    """Convenience property to access `Request.get_args` with default values.
    """

    def get_query_args(
        self,
        keep_blank_values: bool = False,
        strict_parsing: bool = False,
        encoding: str = "utf-8",
        errors: str = "replace",
    ) -> list:
        """Parse `query_string` using `urllib.parse.parse_qsl`.

        This methods is used by `query_args` propertyn but can be used
        directly if you need to change default parameters.

        Args:
            keep_blank_values (bool): Flag indicating whether blank values in
                percent-encoded queries should be treated as blank strings.
                A `True` value indicates that blanks should be retained as
                blank strings. The default `False` value indicates that
                blank values are to be ignored and treated as if they were
                not included.
            strict_parsing (bool): Flag indicating what to do with
                parsing errors. If `False` (the default), errors are
                silently ignored. If `True`, errors raise a
                `ValueError` exception.
            encoding (str): Specify how to decode percent-encoded sequences
                into Unicode characters, as accepted by the
                `bytes.decode()` method.
            errors (str): Specify how to decode percent-encoded sequences
                into Unicode characters, as accepted by the
                `bytes.decode()` method.

        Returns:
            list: A list of tuples containing the parsed arguments.
        """
        if (
            keep_blank_values,
            strict_parsing,
            encoding,
            errors,
        ) not in self.parsed_not_grouped_args:
            if self.query_string:
                self.parsed_not_grouped_args[
                    (keep_blank_values, strict_parsing, encoding, errors)
                ] = parse_qsl(
                    qs=self.query_string,
                    keep_blank_values=keep_blank_values,
                    strict_parsing=strict_parsing,
                    encoding=encoding,
                    errors=errors,
                )
        return self.parsed_not_grouped_args[
            (keep_blank_values, strict_parsing, encoding, errors)
        ]

    query_args = property(get_query_args)
    """Convenience property to access `Request.get_query_args` with default values.
    """  # noqa: E501

    def get_cookies(self) -> RequestParameters:
        cookie = self.headers.getone("cookie", "")
        self.parsed_cookies = CookieRequestParameters(parse_cookie(cookie))
        return self.parsed_cookies

    @property
    def cookies(self) -> RequestParameters:
        """Incoming cookies on the request

        Returns:
            RequestParameters: Incoming cookies on the request
        """

        if self.parsed_cookies is None:
            self.get_cookies()
        return cast(CookieRequestParameters, self.parsed_cookies)

    @property
    def content_type(self) -> str:
        """Content-Type header form the request

        Returns:
            str: Content-Type header form the request
        """
        return self.headers.getone("content-type", DEFAULT_HTTP_CONTENT_TYPE)

    @property
    def match_info(self) -> dict[str, Any]:
        """Matched path parameters after resolving route

        Returns:
            dict[str, Any]: Matched path parameters after resolving route
        """
        return self._match_info

    @match_info.setter
    def match_info(self, value):
        self._match_info = value

    @property
    def ip(self) -> str:
        """Peer ip of the socket

        Returns:
            str: Peer ip of the socket
        """
        return self.conn_info.client_ip if self.conn_info else ""

    @property
    def port(self) -> int:
        """Peer port of the socket

        Returns:
            int: Peer port of the socket
        """
        return self.conn_info.client_port if self.conn_info else 0

    @property
    def socket(self) -> tuple[str, int] | tuple[None, None]:
        """Information about the connected socket if available

        Returns:
            tuple[str, int] | tuple[None, None]: Information about the
                connected socket if available, in the form of a tuple of
                (ip, port)
        """
        return (
            self.conn_info.peername
            if self.conn_info and self.conn_info.peername
            else (None, None)
        )

    @property
    def path(self) -> str:
        """Path of the local HTTP request

        Returns:
            str: Path of the local HTTP request
        """
        return self._parsed_url.path.decode("utf-8")

    @property
    def network_paths(self) -> list[Any] | None:
        """Access the network paths if available

        Returns:
            list[Any] | None: Access the network paths if available
        """
        if self.conn_info is None:
            return None
        return self.conn_info.network_paths

    # Proxy properties (using SERVER_NAME/forwarded/request/transport info)

    @property
    def forwarded(self) -> Options:
        """Active proxy information obtained from request headers, as specified in the application configuration.

        Field names by, for, proto, host, port and path are normalized.
        - for and by IPv6 addresses are bracketed
        - port (int) is only set by port headers, not from host.
        - path is url-unencoded

        Additional values may be available from new style Forwarded headers.

        Returns:
            Options: proxy information from request headers
        """  # noqa: E501
        if self.parsed_forwarded is None:
            config = self.app.config
            trusted = config.TRUSTED_PROXIES
            if trusted is not None and not trusted_peer(self.conn_info, trusted):
                # Not from a trusted proxy: its forwarded headers are the client's.
                self.parsed_forwarded = {}
            else:
                self.parsed_forwarded = (
                    parse_forwarded(self.headers, config)
                    or parse_xforwarded(self.headers, config)
                    or {}
                )
        return self.parsed_forwarded

    @property
    def remote_addr(self) -> str:
        """Client IP address, if available from proxy.

        Returns:
            str: IPv4, bracketed IPv6, UNIX socket name or arbitrary string
        """
        if not hasattr(self, "_remote_addr"):
            self._remote_addr = str(self.forwarded.get("for", ""))
        return self._remote_addr

    @property
    def client_ip(self) -> str:
        """
        Client IP address.
        1. proxied remote address `self.forwarded['for']`
        2. local peer address `self.ip`

        Prefer this over `remote_addr` for determining the
        client address regardless of whether the service runs behind a proxy
        or not (proxy deployment needs separate configuration).

        Returns:
            str: IPv4, bracketed IPv6, UNIX socket name or arbitrary string
        """
        return self.remote_addr or self.ip

    @property
    def scheme(self) -> str:
        """Determine request scheme.

        1. `config.SERVER_NAME` if in full URL format
        2. proxied proto/scheme
        3. local connection protocol

        Returns:
            str: http|https|ws|wss or arbitrary value given by the headers.
        """
        if not hasattr(self, "_scheme"):
            if (
                self.app.websocket_enabled
                and self.headers.upgrade.lower() == "websocket"
            ):
                scheme = "ws"
            else:
                scheme = "http"
            proto = None
            sp = self.app.config.get("SERVER_NAME", "").split("://", 1)
            if len(sp) == 2:
                proto = sp[0]
            elif "proto" in self.forwarded:
                proto = str(self.forwarded["proto"])
            if proto:
                # Give ws/wss if websocket, otherwise keep the same
                scheme = proto.replace("http", scheme)
            self._scheme = scheme

        return self._scheme

    @property
    def host(self) -> str:
        """The currently effective server 'host' (hostname or hostname:port).

        1. `config.SERVER_NAME` overrides any client headers
        2. proxied host of original request
        3. request host header
        hostname and port may be separated by
        `oldman_server.headers.parse_host(request.host)`.

        Returns:
            str: the first matching host found, or empty string
        """
        server_name = self.app.config.get("SERVER_NAME")
        if server_name:
            return server_name.split("//", 1)[-1].split("/", 1)[0]
        return str(
            self.forwarded.get("host") or self.headers.getone("host", "")
        )

    @property
    def server_name(self) -> str:
        """hostname the client connected to, by `request.host`

        Returns:
            str: hostname the client connected to, by `request.host`
        """
        return parse_host(self.host)[0] or ""

    @property
    def server_port(self) -> int:
        """The port the client connected to, by forwarded `port` or `request.host`.

        Default port is returned as 80 and 443 based on `request.scheme`.

        Returns:
            int: The port the client connected to, by forwarded `port` or `request.host`.
        """  # noqa: E501
        port = self.forwarded.get("port") or parse_host(self.host)[1]
        return int(port or (80 if self.scheme in ("http", "ws") else 443))

    @property
    def server_path(self) -> str:
        """Full path of current URL; uses proxied or local path

        Returns:
            str: Full path of current URL; uses proxied or local path
        """
        return str(self.forwarded.get("path") or self.path)

    @property
    def query_string(self) -> str:
        """Representation of the requested query

        Returns:
            str: Representation of the requested query
        """
        if self._parsed_url.query:
            return self._parsed_url.query.decode("utf-8")
        else:
            return ""

    @property
    def url(self) -> str:
        """The URL

        Returns:
            str: The URL
        """
        return urlunparse(
            (self.scheme, self.host, self.path, None, self.query_string, None)
        )

    def url_for(self, view_name: str, **kwargs) -> str:
        """Retrieve a URL for a given view name.

        Same as `oldman_server.OldmanServer.url_for`, but automatically determine `scheme`
        and `netloc` base on the request. Since this method is aiming
        to generate correct schema & netloc, `_external` is implied.

        Args:
            view_name (str): The view name to generate URL for.
            **kwargs: Arbitrary keyword arguments to build URL query string.

        Returns:
            str: The generated URL.
        """
        # Full URL SERVER_NAME can only be handled in app.url_for
        try:
            sp = self.app.config.get("SERVER_NAME", "").split("://", 1)
            if len(sp) == 2:
                return self.app.url_for(view_name, _external=True, **kwargs)
        except AttributeError:
            pass

        scheme = self.scheme
        host = self.server_name
        port = self.server_port

        if (scheme.lower() in ("http", "ws") and port == 80) or (
            scheme.lower() in ("https", "wss") and port == 443
        ):
            netloc = host
        else:
            netloc = f"{host}:{port}"

        return self.app.url_for(
            view_name, _external=True, _scheme=scheme, _server=netloc, **kwargs
        )

    @property
    def is_safe(self) -> bool:
        """Whether the HTTP method is safe.

        See https://datatracker.ietf.org/doc/html/rfc7231#section-4.2.1

        Returns:
            bool: Whether the HTTP method is safe.
        """
        return self.method in SAFE_HTTP_METHODS

    @property
    def is_idempotent(self) -> bool:
        """Whether the HTTP method is iempotent.

        See https://datatracker.ietf.org/doc/html/rfc7231#section-4.2.2

        Returns:
            bool: Whether the HTTP method is iempotent.
        """
        return self.method in IDEMPOTENT_HTTP_METHODS

    @property
    def is_cacheable(self) -> bool:
        """Whether the HTTP method is cacheable.

        See https://datatracker.ietf.org/doc/html/rfc7231#section-4.2.3

        Returns:
            bool: Whether the HTTP method is cacheable.
        """
        return self.method in CACHEABLE_HTTP_METHODS
