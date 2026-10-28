from __future__ import annotations

import os
import re

from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING

from oldman_server.exceptions import HeaderNotFound, RangeNotSatisfiable
from oldman_server.models.protocol_types import Range


if TYPE_CHECKING:
    from oldman_server import Request


_DIGITS = re.compile(r"[0-9]+")


class ContentRangeHandler(Range):
    """The byte range of a static file a request asks for, evaluated as
    RFC 9110 section 14 says: `start` and `end` are always within the file.

    Raises:
        HeaderNotFound: no range to apply, so the whole file is sent: the
            request has no Range, its unit is not bytes (MUST be ignored),
            it asks for several ranges (no multipart/byteranges here), its
            If-Range does not match the file's Last-Modified (an entity tag
            never matches: none is sent), or the file is empty.
        RangeNotSatisfiable: 416 with `Content-Range: bytes */<length>`: the
            range is malformed, starts at or after the end of the file, or
            is an empty suffix.

    Args:
        request (Request): The incoming request object.
        stats (os.stat_result): The stats of the file being served.
    """

    __slots__ = ("start", "end", "size", "total", "headers")

    def __init__(self, request: Request, stats: os.stat_result) -> None:
        self.total = total = stats.st_size
        header = request.headers.getone("range", None)
        if header is None:
            raise HeaderNotFound("Range Header Not Found")
        unit, equals, ranges = header.partition("=")
        if (
            not equals
            or unit.strip().lower() != "bytes"
            or "," in ranges
            or not total
            or not self._if_range_matches(request, stats)
        ):
            raise HeaderNotFound("Range not applied")

        first, dash, last = (part.strip() for part in ranges.partition("-"))
        if not dash or not (first or last):
            raise RangeNotSatisfiable("Invalid for Content Range parameters", self)
        if first and not _DIGITS.fullmatch(first):
            raise RangeNotSatisfiable(f"'{first}' is invalid for Content Range", self)
        if last and not _DIGITS.fullmatch(last):
            raise RangeNotSatisfiable(f"'{last}' is invalid for Content Range", self)

        if first:
            start = int(first)
            end = int(last) if last else total - 1
            if end < start:
                raise RangeNotSatisfiable("Invalid for Content Range parameters", self)
            if start >= total:
                raise RangeNotSatisfiable("The range starts after the end of the file", self)
            # A last position past the end means the end (14.1.2).
            end = min(end, total - 1)
        else:
            # A suffix: the last bytes, the whole file when it asks for more.
            length = int(last)
            if not length:
                raise RangeNotSatisfiable("An empty suffix range", self)
            start, end = max(total - length, 0), total - 1

        self.start = start
        self.end = end
        self.size = end - start + 1
        self.headers = {"Content-Range": f"bytes {start}-{end}/{total}"}

    @staticmethod
    def _if_range_matches(request: Request, stats: os.stat_result) -> bool:
        """No If-Range, or one whose date is the Last-Modified the file is
        sent with (to the second, as formatdate writes it)."""
        validator = request.headers.getone("if-range", None)
        if validator is None:
            return True
        try:
            return int(parsedate_to_datetime(validator).timestamp()) == int(stats.st_mtime)
        except (TypeError, ValueError, IndexError, OverflowError):
            return False  # an entity tag, or not a date

    def __bool__(self):
        return hasattr(self, "size") and self.size > 0
