from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from email.utils import formatdate, parsedate_to_datetime
from mimetypes import guess_type
from os import path, stat_result
from pathlib import PurePath
from time import time
from typing import Any, AnyStr, Callable
from urllib.parse import quote_plus

from oldman_server.compat import (
    Header,
    open_async,
    read_file_async,
    serving_native_files,
    stat_async,
)
from oldman_server.constants import DEFAULT_HTTP_CONTENT_TYPE
from oldman_server.helpers import Default, _default
from oldman_server.log import logger
from oldman_server.models.protocol_types import HTMLProtocol, Range

from .types import FileResponse, HTTPResponse, JSONResponse, ResponseStream


def empty(
    status: int = 204, headers: Mapping[str, str] | None = None
) -> HTTPResponse:
    """Returns an empty response to the client.

    Args:
        status (int, optional): HTTP response code. Defaults to `204`.
        headers ([type], optional): Custom HTTP headers. Defaults to `None`.

    Returns:
        HTTPResponse: An empty response to the client.
    """
    return HTTPResponse(body=b"", status=status, headers=headers)


def json(
    body: Any,
    status: int = 200,
    headers: Mapping[str, str] | None = None,
    content_type: str = "application/json",
    dumps: Callable[..., AnyStr] | None = None,
    **kwargs: Any,
) -> JSONResponse:
    """Returns response object with body in json format.

    Args:
        body (Any): Response data to be serialized.
        status (int, optional): HTTP response code. Defaults to `200`.
        headers (Dict[str, str], optional): Custom HTTP headers. Defaults to `None`.
        content_type (str, optional): The content type (string) of the response. Defaults to `"application/json"`.
        dumps (Callable[..., AnyStr], optional): A custom json dumps function. Defaults to `None`.
        **kwargs (Any): Remaining arguments that are passed to the json encoder.

    Returns:
        JSONResponse: A response object with body in json format.
    """  # noqa: E501
    if kwargs:
        return JSONResponse(body, status, headers, content_type, dumps, **kwargs)
    return JSONResponse(body, status, headers, content_type, dumps)


def text(
    body: str,
    status: int = 200,
    headers: Mapping[str, str] | None = None,
    content_type: str = "text/plain; charset=utf-8",
) -> HTTPResponse:
    """Returns response object with body in text format.

    Args:
        body (str): Response data.
        status (int, optional): HTTP response code. Defaults to `200`.
        headers (Dict[str, str], optional): Custom HTTP headers. Defaults to `None`.
        content_type (str, optional): The content type (string) of the response. Defaults to `"text/plain; charset=utf-8"`.

    Returns:
        HTTPResponse: A response object with body in text format.

    Raises:
        TypeError: If the body is not a string.
    """  # noqa: E501
    if not isinstance(body, str):
        raise TypeError(
            f"Bad body type. Expected str, got {type(body).__name__})"
        )

    return HTTPResponse(body, status, headers, content_type)


def raw(
    body: AnyStr | None,
    status: int = 200,
    headers: Mapping[str, str] | None = None,
    content_type: str = DEFAULT_HTTP_CONTENT_TYPE,
) -> HTTPResponse:
    """Returns response object without encoding the body.

    Args:
        body (Optional[AnyStr]): Response data.
        status (int, optional): HTTP response code. Defaults to `200`.
        headers (Dict[str, str], optional): Custom HTTP headers. Defaults to `None`.
        content_type (str, optional): The content type (string) of the response. Defaults to `"application/octet-stream"`.

    Returns:
        HTTPResponse: A response object without encoding the body.
    """  # noqa: E501
    return HTTPResponse(body, status, headers, content_type)


def html(
    body: str | bytes | HTMLProtocol,
    status: int = 200,
    headers: Mapping[str, str] | None = None,
) -> HTTPResponse:
    """Returns response object with body in html format.

    Body should be a `str` or `bytes` like object, or an object with `__html__` or `_repr_html_`.

    Args:
        body (Union[str, bytes, HTMLProtocol]): Response data.
        status (int, optional): HTTP response code. Defaults to `200`.
        headers (Dict[str, str], optional): Custom HTTP headers. Defaults to `None`.

    Returns:
        HTTPResponse: A response object with body in html format.
    """  # noqa: E501
    if not isinstance(body, (str, bytes)):
        if hasattr(body, "__html__"):
            body = body.__html__()
        elif hasattr(body, "_repr_html_"):
            body = body._repr_html_()

    return HTTPResponse(
        body,
        status=status,
        headers=headers,
        content_type="text/html; charset=utf-8",
    )


async def validate_file(
    request_headers: Header, last_modified: datetime | float | int
) -> HTTPResponse | None:
    """Validate file based on request headers.

    Args:
        request_headers (Header): The request headers.
        last_modified (Union[datetime, float, int]): The last modified date and time of the file.

    Returns:
        Optional[HTTPResponse]: A response object with status 304 if the file is not modified.
    """  # noqa: E501
    try:
        if_modified_since = request_headers.getone("If-Modified-Since")
    except KeyError:
        return None
    try:
        if_modified_since = parsedate_to_datetime(if_modified_since)
    except (TypeError, ValueError):
        logger.warning(
            "Ignorning invalid If-Modified-Since header received: '%s'",
            if_modified_since,
        )
        return None
    if not isinstance(last_modified, datetime):
        last_modified = datetime.fromtimestamp(
            float(last_modified), tz=timezone.utc
        ).replace(microsecond=0)

    if (
        last_modified.utcoffset() is None
        and if_modified_since.utcoffset() is not None
    ):
        logger.warning(
            "Cannot compare tz-aware and tz-naive datetimes. To avoid "
            "this conflict oldman-server is converting last_modified to UTC."
        )
        last_modified.replace(tzinfo=timezone.utc)
    elif (
        last_modified.utcoffset() is not None
        and if_modified_since.utcoffset() is None
    ):
        logger.warning(
            "Cannot compare tz-aware and tz-naive datetimes. To avoid "
            "this conflict oldman-server is converting if_modified_since to UTC."
        )
        if_modified_since.replace(tzinfo=timezone.utc)
    if last_modified.timestamp() <= if_modified_since.timestamp():
        return HTTPResponse(status=304)

    return None


# On the native host, file() sends a file of at least this many bytes from the
# file to the connection (FileResponse): as a Python object its bytes would cost
# more, in copies and the fresh pages of a large allocation. A smaller file
# costs less read into the response than sent that way.
_NATIVE_SEND_SIZE = 128 << 10


def _read_length(stat: stat_result, offset: int, size: int | None) -> int:
    """How many bytes reading `size` (None: to the end) from `offset` gets."""
    length = max(0, stat.st_size - offset)
    return length if size is None else min(length, size)


async def file(
    location: str | PurePath,
    status: int = 200,
    request_headers: Header | None = None,
    validate_when_requested: bool = True,
    mime_type: str | None = None,
    headers: Mapping[str, str] | None = None,
    filename: str | None = None,
    last_modified: datetime | float | int | Default | None = _default,
    max_age: float | int | None = None,
    no_store: bool | None = None,
    _range: Range | None = None,
    _stat: stat_result | None = None,
) -> HTTPResponse:
    """Return a response object with file data.

    Args:
        location (Union[str, PurePath]): Location of file on system.
        status (int, optional): HTTP response code. Won't enforce the passed in status if only a part of the content will be sent (206) or file is being validated (304). Defaults to 200.
        request_headers (Optional[Header], optional): The request headers.
        validate_when_requested (bool, optional): If `True`, will validate the file when requested. Defaults to True.
        mime_type (Optional[str], optional): Specific mime_type.
        headers (Optional[Dict[str, str]], optional): Custom Headers.
        filename (Optional[str], optional): Override filename.
        last_modified (Optional[Union[datetime, float, int, Default]], optional): The last modified date and time of the file.
        max_age (Optional[Union[float, int]], optional): Max age for cache control.
        no_store (Optional[bool], optional): Any cache should not store this response. Defaults to None.
        _range (Optional[Range], optional):
        _stat (Optional[os.stat_result], optional): The file's stat, when the caller has one.

    Returns:
        HTTPResponse: The response object with the file data.
    """  # noqa: E501

    if isinstance(last_modified, datetime):
        last_modified = last_modified.replace(microsecond=0).timestamp()
    elif isinstance(last_modified, Default):
        _stat = await stat_async(location)
        last_modified = _stat.st_mtime

    if (
        validate_when_requested
        and request_headers is not None
        and last_modified
    ):
        response = await validate_file(request_headers, last_modified)
        if response:
            return response

    # The response's own headers: the caller's mapping is copied, not changed.
    headers = Header(headers) if headers else Header()
    if last_modified:
        headers.setdefault(
            "Last-Modified", formatdate(last_modified, usegmt=True)
        )

    if filename:
        headers.setdefault(
            "Content-Disposition", f'attachment; filename="{filename}"'
        )

    if no_store:
        cache_control = "no-store"
    elif max_age:
        cache_control = f"public, max-age={max_age}"
        headers.setdefault(
            "expires",
            formatdate(
                time() + max_age,
                usegmt=True,
            ),
        )
    else:
        cache_control = "no-cache"

    headers.setdefault("cache-control", cache_control)

    filename = filename or path.split(location)[-1]

    if _range:
        offset, size = _range.start, _range.size
        headers["Content-Range"] = (
            f"bytes {_range.start}-{_range.end}/{_range.total}"
        )
        status = 206
    else:
        offset, size = 0, None

    content_type = mime_type or guess_content_type(
        filename, fallback="text/plain; charset=utf-8"
    )
    files = serving_native_files()
    if files is not None:
        # A file of unknown size, or a large one, is opened here: it raises
        # in the handler as reading it would.
        opened = None
        if _stat is None:
            _stat = opened = await files.open(location)
        if _read_length(_stat, offset, size) >= _NATIVE_SEND_SIZE:
            if opened is None:
                opened = await files.open(location)
            response = FileResponse(
                location,
                offset,
                _read_length(opened, offset, size),
                status=status,
                content_type=content_type,
            )
            response.headers = headers
            return response
    out_stream = await read_file_async(location, offset, size)
    response = HTTPResponse(
        body=out_stream, status=status, content_type=content_type
    )
    response.headers = headers
    return response


def redirect(
    to: str,
    headers: Mapping[str, str] | None = None,
    status: int = 302,
    content_type: str = "text/html; charset=utf-8",
) -> HTTPResponse:
    """Cause a HTTP redirect (302 by default) by setting a Location header.

    Args:
        to (str): path or fully qualified URL to redirect to
        headers (Optional[Mapping[str, str]], optional): headers to include in the response, copied (the mapping itself is not changed). Defaults to None.
        status (int, optional): status code (int) of the new request, defaults to 302. Defaults to 302.
        content_type (str, optional): the content type (string) of the response. Defaults to "text/html; charset=utf-8".

    Returns:
        HTTPResponse: A response object with the redirect.
    """  # noqa: E501
    # URL Quote the URL before redirecting
    safe_to = quote_plus(to, safe=":/%#?&=@[]!$&'()*+,;")

    response = HTTPResponse(
        status=status, headers=headers, content_type=content_type
    )
    # According to RFC 7231, a relative URI is now permitted. Set on the
    # response's own headers: the caller's mapping is not changed.
    response.headers["Location"] = safe_to
    return response


async def file_stream(
    location: str | PurePath,
    status: int = 200,
    chunk_size: int = 4096,
    mime_type: str | None = None,
    headers: Mapping[str, str] | None = None,
    filename: str | None = None,
    _range: Range | None = None,
) -> ResponseStream:
    """Return a streaming response object with file data.

    :param location: Location of file on system.
    :param chunk_size: The size of each chunk in the stream (in bytes)
    :param mime_type: Specific mime_type.
    :param headers: Custom Headers.
    :param filename: Override filename.
    :param _range:

    Args:
        location (Union[str, PurePath]): Location of file on system.
        status (int, optional): HTTP response code. Won't enforce the passed in status if only a part of the content will be sent (206) or file is being validated (304). Defaults to `200`.
        chunk_size (int, optional): The size of each chunk in the stream (in bytes). Defaults to `4096`.
        mime_type (Optional[str], optional): Specific mime_type.
        headers (Optional[Dict[str, str]], optional): Custom HTTP headers.
        filename (Optional[str], optional): Override filename.
        _range (Optional[Range], optional): The range of bytes to send.
    """  # noqa: E501
    # The response's own headers: the caller's mapping is copied, not changed.
    headers = Header(headers) if headers else Header()
    if filename:
        headers.setdefault(
            "Content-Disposition", f'attachment; filename="{filename}"'
        )
    filename = filename or path.split(location)[-1]
    mime_type = mime_type or guess_type(filename)[0] or "text/plain"
    if _range:
        start = _range.start
        end = _range.end
        total = _range.total

        headers["Content-Range"] = f"bytes {start}-{end}/{total}"
        status = 206

    async def _streaming_fn(response):
        stream = getattr(getattr(response, "response", response), "stream", None)
        send_file = getattr(stream, "send_file", None)
        if send_file is not None and chunk_size > 0:
            # The native host reads the file and writes the same chunks.
            await send_file(
                location,
                _range.start if _range else 0,
                _range.size if _range else None,
                chunk_size,
            )
            return
        async with await open_async(location, mode="rb") as f:
            if _range:
                await f.seek(_range.start)
                to_send = _range.size
                while to_send > 0:
                    # Fork fix: upstream reads min(_range.size, chunk_size) and
                    # can send more than the range.
                    content = await f.read(min((to_send, chunk_size)))
                    if len(content) < 1:
                        break
                    to_send -= len(content)
                    await response.write(content)
            else:
                while True:
                    content = await f.read(chunk_size)
                    if len(content) < 1:
                        break
                    await response.write(content)

    response = ResponseStream(
        streaming_fn=_streaming_fn, status=status, content_type=mime_type
    )
    response.headers = headers
    return response


def guess_content_type(
    file_path: str | PurePath,
    fallback: str = DEFAULT_HTTP_CONTENT_TYPE,
) -> str:
    """Guess the content type (rather than MIME only) by the file extension."""
    mediatype = guess_type(file_path)[0]
    if mediatype is None:
        return fallback
    if mediatype.startswith("text/"):
        return f"{mediatype}; charset=utf-8"
    return mediatype
