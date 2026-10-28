from __future__ import annotations

from collections.abc import Coroutine, Iterator, Mapping
from datetime import datetime
from pathlib import PurePath
from types import MemberDescriptorType
from typing import TYPE_CHECKING, Any, AnyStr, Callable, TypeVar

from oldman_server.compat import Header, read_file_async
from oldman_server.cookies import CookieJar
from oldman_server.cookies.response import Cookie, SameSite
from oldman_server.exceptions import OldmanServerException, ServerError
from oldman_server.helpers import (
    Default,
    OPT_NON_STR_KEYS,
    _default,
    has_message_body,
    json_default,
    json_dumps,
    orjson_dumps,
)
from oldman_server.http import Http


if TYPE_CHECKING:
    from oldman_server.request import Request
else:
    Request = TypeVar("Request")


HEADER_TRANSLATION_TABLE = str.maketrans("", "", "\r\n\x00")


class _SentContentType:
    """Headers never accessed before a send that applied only the default
    Content-Type; ``headers`` materializes them as processed_headers would have.
    """

    __slots__ = ("content_type",)

    def __init__(self, content_type):
        self.content_type = content_type


class BaseHTTPResponse:
    """The base class for all HTTP Responses"""

    __slots__ = (
        "body",
        "content_type",
        "stream",
        "status",
        "_headers",
        "_cookies",
    )

    _dumps = json_dumps

    def __init__(self):
        self.body: bytes | None = None
        self.content_type: str | None = None
        self.stream: Http | None = None
        self.status: int = None
        self._headers: Header | _SentContentType | None = None
        self._cookies: CookieJar | None = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Sanic's responses keep headers in a slot, and response subclasses
        # written for stock Sanic may list "headers" in their own __slots__:
        # that slot would hide the property below.
        if isinstance(cls.__dict__.get("headers"), MemberDescriptorType):
            cls.headers = BaseHTTPResponse.headers

    def __repr__(self):
        class_name = self.__class__.__name__
        return f"<{class_name}: {self.status} {self.content_type}>"

    def _encode_body(self, data: str | bytes | None):
        if data is None:
            return b""
        return data.encode() if hasattr(data, "encode") else data  # type: ignore

    @property
    def headers(self) -> Header:
        """The response headers, created on first access.

        Returns:
            Header: The response headers
        """
        headers = self._headers
        if headers is None or type(headers) is _SentContentType:
            sent, headers = headers, Header()
            if sent is not None:
                headers["content-type"] = sent.content_type
            self._headers = headers
        return headers

    @headers.setter
    def headers(self, value: Header) -> None:
        self._headers = value

    def _has_header(self, name: str) -> bool:
        """Whether `name` (lower case) is among the headers, without creating
        them as `headers` would."""
        headers = self._headers
        if headers is None:
            return False
        if type(headers) is _SentContentType:
            return name == "content-type"
        return name in headers

    @property
    def cookies(self) -> CookieJar:
        """The response cookies.

        Returns:
            CookieJar: The response cookies
        """
        if self._cookies is None:
            self._cookies = CookieJar(self.headers)
        return self._cookies

    @property
    def processed_headers(self) -> Iterator[tuple[bytes, bytes]]:
        """Obtain a list of header tuples encoded in bytes for sending.

        Add and remove headers based on status and content_type.

        Returns:
            Iterator[Tuple[bytes, bytes]]: A list of header tuples encoded in bytes for sending
        """  # noqa: E501
        if has_message_body(self.status) and self.content_type is not None:
            self.headers.setdefault("content-type", self.content_type)
        # Encode headers into bytes
        return (
            (
                self._sanitize_header_value(name).encode("ascii"),
                self._sanitize_header_value(f"{value}").encode(
                    errors="surrogateescape"
                ),
            )
            for name, value in self.headers.items()
        )

    async def send(
        self,
        data: AnyStr | None = None,
        end_stream: bool | None = None,
    ) -> None:
        """Send any pending response headers and the given data as body.

        Args:
            data (Optional[AnyStr], optional): str or bytes to be written. Defaults to `None`.
            end_stream (Optional[bool], optional): whether to close the stream after this block. Defaults to `None`.
        """  # noqa: E501
        if data is None and end_stream is None:
            end_stream = True
        if self.stream is None:
            raise OldmanServerException(
                "No stream is connected to the response object instance."
            )
        if self.stream.send is None:
            if end_stream and not data:
                return
            raise ServerError(
                "Response stream was ended, no more response data is "
                "allowed to be sent."
            )
        data = data.encode() if hasattr(data, "encode") else data or b""  # type: ignore
        await self.stream.send(
            data,  # type: ignore
            end_stream=end_stream or False,
        )

    def add_cookie(
        self,
        key: str,
        value: str,
        *,
        path: str = "/",
        domain: str | None = None,
        secure: bool = True,
        max_age: int | None = None,
        expires: datetime | None = None,
        httponly: bool = False,
        samesite: SameSite | None = "Lax",
        partitioned: bool = False,
        comment: str | None = None,
        host_prefix: bool = False,
        secure_prefix: bool = False,
    ) -> Cookie:
        """Add a cookie to the CookieJar

        Args:
            key (str): The key to be added
            value (str): The value to be added
            path (str, optional): Path of the cookie. Defaults to `"/"`.
            domain (Optional[str], optional): Domain of the cookie. Defaults to `None`.
            secure (bool, optional): Whether the cookie is secure. Defaults to `True`.
            max_age (Optional[int], optional): Max age of the cookie. Defaults to `None`.
            expires (Optional[datetime], optional): Expiry date of the cookie. Defaults to `None`.
            httponly (bool, optional): Whether the cookie is http only. Defaults to `False`.
            samesite (Optional[SameSite], optional): SameSite policy of the cookie. Defaults to `"Lax"`.
            partitioned (bool, optional): Whether the cookie is partitioned. Defaults to `False`.
            comment (Optional[str], optional): Comment of the cookie. Defaults to `None`.
            host_prefix (bool, optional): Whether to add __Host- as a prefix to the key. This requires that path="/", domain=None, and secure=True. Defaults to `False`.
            secure_prefix (bool, optional): Whether to add __Secure- as a prefix to the key. This requires that secure=True. Defaults to `False`.

        Returns:
            Cookie: The cookie that was added
        """  # noqa: E501
        return self.cookies.add_cookie(
            key=key,
            value=value,
            path=path,
            domain=domain,
            secure=secure,
            max_age=max_age,
            expires=expires,
            httponly=httponly,
            samesite=samesite,
            partitioned=partitioned,
            comment=comment,
            host_prefix=host_prefix,
            secure_prefix=secure_prefix,
        )

    def delete_cookie(
        self,
        key: str,
        *,
        path: str = "/",
        domain: str | None = None,
        host_prefix: bool = False,
        secure_prefix: bool = False,
    ) -> None:
        """Delete a cookie

        This will effectively set it as Max-Age: 0, which a browser should
        interpret it to mean: "delete the cookie".

        Since it is a browser/client implementation, your results may vary
        depending upon which client is being used.

        Args:
            key (str): The key to be deleted
            path (str, optional): Path of the cookie. Defaults to `"/"`.
            domain (Optional[str], optional): Domain of the cookie. Defaults to `None`.
            host_prefix (bool, optional): Whether to add __Host- as a prefix to the key. This requires that path="/", domain=None, and secure=True. Defaults to `False`.
            secure_prefix (bool, optional): Whether to add __Secure- as a prefix to the key. This requires that secure=True. Defaults to `False`.
        """  # noqa: E501
        self.cookies.delete_cookie(
            key=key,
            path=path,
            domain=domain,
            host_prefix=host_prefix,
            secure_prefix=secure_prefix,
        )

    @staticmethod
    def _sanitize_header_value(value: str) -> str:
        return value.translate(HEADER_TRANSLATION_TABLE)


class HTTPResponse(BaseHTTPResponse):
    """HTTP response to be sent back to the client.

    Args:
        body (Optional[Any], optional): The body content to be returned. Defaults to `None`.
        status (int, optional): HTTP response number. Defaults to `200`.
        headers (Optional[Union[Header, Dict[str, str]]], optional): Headers to be returned. Defaults to `None`.
        content_type (Optional[str], optional): Content type to be returned (as a header). Defaults to `None`.
    """  # noqa: E501

    __slots__ = ()

    def __init__(
        self,
        body: Any = None,
        status: int = 200,
        headers: Mapping[str, str] | None = None,
        content_type: str | None = None,
    ):
        # Same final state as BaseHTTPResponse.__init__() followed by these
        # assignments, without building and discarding an empty Header first.
        self.stream = None
        self.content_type: str | None = content_type
        # _encode_body for the exact class without the call: bytes unchanged,
        # str encoded.
        kind = type(body)
        if type(self) is HTTPResponse and (kind is bytes or kind is str):
            self.body = body if kind is bytes else body.encode()
        else:
            self.body = self._encode_body(body)
        self.status = status
        self._headers = Header(headers) if headers else None
        self._cookies = None

    async def eof(self):
        """Send a EOF (End of File) message to the client."""
        await self.send("", True)

    def abort(self) -> None:
        """End a streamed response without completing it.

        The connection closes after the data already sent, so the client
        sees the response is incomplete: no final chunk, or fewer bytes than
        its Content-Length. Use it when the rest of the body cannot be
        produced, for instance when its upstream failed. Later sends raise,
        and the handler is cancelled at its next await, as when the client
        disconnects.

        Raises:
            ServerError: Nothing was sent yet; raise an exception or return an
                error response instead. Once the response ended, abort() does
                nothing.
        """
        if self.stream is not None:
            self.stream.abort()

    async def __aenter__(self):
        return self.send

    async def __aexit__(self, *_):
        await self.eof()


class FileResponse(HTTPResponse):
    """file()'s response for a large file on the native host: when it is
    sent, the host reads the file range and writes it to the connection, so
    its bytes never become a Python object. ``body`` is None; a body set
    before the send is sent instead of the file."""

    __slots__ = ("_file",)

    def __init__(
        self,
        location: str | PurePath,
        offset: int,
        length: int,
        status: int = 200,
        headers: Mapping[str, str] | None = None,
        content_type: str | None = None,
    ):
        super().__init__(None, status, headers, content_type)
        self.body = None
        self._file = location, offset, length

    async def send(
        self,
        data: AnyStr | None = None,
        end_stream: bool | None = None,
    ) -> None:
        file, self._file = self._file, None
        if file is not None and self.body is None:
            # The native host's exchange; otherwise the file is read here.
            send_file_body = getattr(self.stream, "send_file_body", None)
            if (
                send_file_body is not None
                and not data
                and end_stream is not False
                and await send_file_body(self, *file)
            ):
                return
            self.body = await read_file_async(*file)
        await super().send(data, end_stream)


class JSONResponse(HTTPResponse):
    """Convenience class for JSON responses

    HTTP response to be sent back to the client, when the response
    is of json type. Offers several utilities to manipulate common
    json data types.

    Args:
        body (Optional[Any], optional): The body content to be returned. Defaults to `None`.
        status (int, optional): HTTP response number. Defaults to `200`.
        headers (Optional[Union[Header, Dict[str, str]]], optional): Headers to be returned. Defaults to `None`.
        content_type (str, optional): Content type to be returned (as a header). Defaults to `"application/json"`.
        dumps (Optional[Callable[..., AnyStr]], optional): The function to use for json encoding. Defaults to `None`.
        **kwargs (Any, optional): The kwargs to pass to the json encoding function. Defaults to `{}`.
    """  # noqa: E501

    __slots__ = (
        "_body",
        "_body_manually_set",
        "_initialized",
        "_raw_body",
        "_use_dumps",
        "_use_dumps_kwargs",
    )

    def __init__(
        self,
        body: Any = None,
        status: int = 200,
        headers: Mapping[str, str] | None = None,
        content_type: str = "application/json",
        dumps: Callable[..., AnyStr] | None = None,
        **kwargs: Any,
    ):
        self._initialized = False
        self._body_manually_set = False

        self._use_dumps: Callable[..., str | bytes] = (
            dumps or BaseHTTPResponse._dumps
        )
        self._use_dumps_kwargs = kwargs

        self._raw_body = body

        # HTTPResponse.__init__, inlined: the same final state, with the
        # serialized body encoded and assigned once.
        self.stream = None
        self.content_type = content_type
        if kwargs:
            dumped = self._use_dumps(body, **kwargs)
        elif self._use_dumps is json_dumps:
            # json_dumps without its Python frame.
            dumped = orjson_dumps(body, json_default, OPT_NON_STR_KEYS)
        else:
            dumped = self._use_dumps(body)
        if type(self) is JSONResponse:
            # The body setter and _encode_body, for the exact class.
            self._body = dumped if type(dumped) is bytes else self._encode_body(dumped)
        else:
            self.body = self._encode_body(dumped)
        self.status = status
        self._headers = Header(headers) if headers else None
        self._cookies = None

        self._initialized = True

    def _check_body_not_manually_set(self):
        if self._body_manually_set:
            raise OldmanServerException(
                "Cannot use raw_body after body has been manually set."
            )

    @property
    def raw_body(self) -> Any:
        """Returns the raw body, as long as body has not been manually set previously.

        NOTE: This object should not be mutated, as it will not be
        reflected in the response body. If you need to mutate the
        response body, consider using one of the provided methods in
        this class or alternatively call set_body() with the mutated
        object afterwards or set the raw_body property to it.

        Returns:
            Optional[Any]: The raw body
        """  # noqa: E501
        self._check_body_not_manually_set()
        return self._raw_body

    @raw_body.setter
    def raw_body(self, value: Any):
        self._body_manually_set = False
        self._body = self._encode_body(
            self._use_dumps(value, **self._use_dumps_kwargs)
        )
        self._raw_body = value

    @property  # type: ignore
    def body(self) -> bytes | None:  # type: ignore
        """Returns the response body.

        Returns:
            Optional[bytes]: The response body
        """
        return self._body

    @body.setter
    def body(self, value: bytes | None):
        self._body = value
        if not self._initialized:
            return
        self._body_manually_set = True

    def set_body(
        self,
        body: Any,
        dumps: Callable[..., AnyStr] | None = None,
        **dumps_kwargs: Any,
    ) -> None:
        """Set the response body to the given value, using the given dumps function

        Sets a new response body using the given dumps function
        and kwargs, or falling back to the defaults given when
        creating the object if none are specified.

        Args:
            body (Any): The body to set
            dumps (Optional[Callable[..., AnyStr]], optional): The function to use for json encoding. Defaults to `None`.
            **dumps_kwargs (Any, optional): The kwargs to pass to the json encoding function. Defaults to `{}`.

        Examples:
            ```python
            response = JSONResponse({"foo": "bar"})
            response.set_body({"bar": "baz"})
            assert response.body == b'{"bar": "baz"}'
            ```
        """  # noqa: E501
        self._body_manually_set = False
        self._raw_body = body

        use_dumps = dumps or self._use_dumps
        use_dumps_kwargs = dumps_kwargs if dumps else self._use_dumps_kwargs

        self._body = self._encode_body(use_dumps(body, **use_dumps_kwargs))

    def append(self, value: Any) -> None:
        """Appends a value to the response raw_body, ensuring that body is kept up to date.

        This can only be used if raw_body is a list.

        Args:
            value (Any): The value to append

        Raises:
            OldmanServerException: If the body is not a list
        """  # noqa: E501

        self._check_body_not_manually_set()

        if not isinstance(self._raw_body, list):
            raise OldmanServerException("Cannot append to a non-list object.")

        self._raw_body.append(value)
        self.raw_body = self._raw_body

    def extend(self, value: Any) -> None:
        """Extends the response's raw_body with the given values, ensuring that body is kept up to date.

        This can only be used if raw_body is a list.

        Args:
            value (Any): The values to extend with

        Raises:
            OldmanServerException: If the body is not a list
        """  # noqa: E501

        self._check_body_not_manually_set()

        if not isinstance(self._raw_body, list):
            raise OldmanServerException("Cannot extend a non-list object.")

        self._raw_body.extend(value)
        self.raw_body = self._raw_body

    def update(self, *args, **kwargs) -> None:
        """Updates the response's raw_body with the given values, ensuring that body is kept up to date.

        This can only be used if raw_body is a dict.

        Args:
            *args: The args to update with
            **kwargs: The kwargs to update with

        Raises:
            OldmanServerException: If the body is not a dict
        """  # noqa: E501

        self._check_body_not_manually_set()

        if not isinstance(self._raw_body, dict):
            raise OldmanServerException("Cannot update a non-dict object.")

        self._raw_body.update(*args, **kwargs)
        self.raw_body = self._raw_body

    def pop(self, key: Any, default: Any = _default) -> Any:
        """Pops a key from the response's raw_body, ensuring that body is kept up to date.

        This can only be used if raw_body is a dict or a list.

        Args:
            key (Any): The key to pop
            default (Any, optional): The default value to return if the key is not found. Defaults to `_default`.

        Raises:
            OldmanServerException: If the body is not a dict or a list
            TypeError: If the body is a list and a default value is provided

        Returns:
            Any: The value that was popped
        """  # noqa: E501

        self._check_body_not_manually_set()

        if not isinstance(self._raw_body, (list, dict)):
            raise OldmanServerException(
                "Cannot pop from a non-list and non-dict object."
            )

        if isinstance(default, Default):
            value = self._raw_body.pop(key)
        elif isinstance(self._raw_body, list):
            raise TypeError("pop doesn't accept a default argument for lists")
        else:
            value = self._raw_body.pop(key, default)

        self.raw_body = self._raw_body

        return value


class ResponseStream:
    """A compat layer to bridge the gap after the deprecation of StreamingHTTPResponse.

    It will be removed when:
    - file_stream is moved to new style streaming
    - file and file_stream are combined into a single API
    """  # noqa: E501

    __slots__ = (
        "_cookies",
        "content_type",
        "headers",
        "request",
        "response",
        "status",
        "streaming_fn",
    )

    def __init__(
        self,
        streaming_fn: Callable[
            [BaseHTTPResponse | ResponseStream],
            Coroutine[Any, Any, None],
        ],
        status: int = 200,
        headers: Mapping[str, str] | None = None,
        content_type: str | None = None,
    ):
        self.streaming_fn = streaming_fn
        self.status = status
        # Its own headers: the caller's mapping (a Header included) is copied,
        # never shared, so middleware and cookies do not change it.
        self.headers = Header(headers) if headers else Header()
        self.content_type = content_type
        self.request: Request | None = None
        self._cookies: CookieJar | None = None

    async def write(self, message: str):
        await self.response.send(message)

    async def stream(self) -> HTTPResponse:
        if not self.request:
            raise ServerError("Attempted response to unknown request")
        self.response = await self.request.respond(
            headers=self.headers,
            status=self.status,
            content_type=self.content_type,
        )
        await self.streaming_fn(self)
        return self.response

    async def eof(self) -> None:
        await self.response.eof()

    @property
    def cookies(self) -> CookieJar:
        if self._cookies is None:
            self._cookies = CookieJar(self.headers)
        return self._cookies

    # Answered from this object's own status, headers and content type, not
    # from the response stream() makes: response middleware reads them before
    # that response exists. stream() passes the same values to respond(), so
    # the headers are those sent.
    _sanitize_header_value = staticmethod(BaseHTTPResponse._sanitize_header_value)
    processed_headers = BaseHTTPResponse.processed_headers

    @property
    def body(self):
        """None: the content is produced while it is sent."""
        return None

    def __call__(self, request: Request) -> ResponseStream:
        self.request = request
        return self

    def __await__(self):
        return self.stream().__await__()
