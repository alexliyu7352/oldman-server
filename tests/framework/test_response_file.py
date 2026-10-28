from datetime import datetime, timezone
from logging import INFO

import pytest

from oldman_server.compat import Header
from oldman_server.response import file_stream
from oldman_server.response.convenience import guess_content_type, validate_file


@pytest.mark.parametrize(
    "ifmod,lastmod,expected",
    (
        ("Sat, 01 Apr 2023 00:00:00 GMT", 1672524000, None),
        (
            "Sat, 01 Apr 2023 00:00:00",
            1672524000,
            "converting if_modified_since",
        ),
        (
            "Sat, 01 Apr 2023 00:00:00 GMT",
            datetime(2023, 1, 1, 0, 0, 0),
            "converting last_modified",
        ),
        (
            "Sat, 01 Apr 2023 00:00:00",
            datetime(2023, 1, 1, 0, 0, 0),
            None,
        ),
        (
            "Sat, 01 Apr 2023 00:00:00 GMT",
            datetime(2023, 1, 1, 0, 0, 0).replace(tzinfo=timezone.utc),
            None,
        ),
        (
            "Sat, 01 Apr 2023 00:00:00",
            datetime(2023, 1, 1, 0, 0, 0).replace(tzinfo=timezone.utc),
            "converting if_modified_since",
        ),
    ),
)
@pytest.mark.asyncio
async def test_file_timestamp_validation(
    lastmod, ifmod, expected, caplog: pytest.LogCaptureFixture
):
    headers = Header([["If-Modified-Since", ifmod]])

    with caplog.at_level(INFO):
        response = await validate_file(headers, lastmod)
    assert response.status == 304
    records = caplog.records
    if not expected:
        assert len(records) == 0
    else:
        record = records[0]
        assert expected in record.message


@pytest.mark.parametrize(
    "file_path,expected",
    (
        ("test.html", "text/html; charset=utf-8"),
        ("test.txt", "text/plain; charset=utf-8"),
        ("test.css", "text/css; charset=utf-8"),
        # Python's own table says text/javascript; mimetypes also reads the
        # system's /etc/mime.types, which on Debian/Ubuntu says
        # application/javascript (no charset added: not a text/ type).
        ("test.js", {"text/javascript; charset=utf-8", "application/javascript"}),
        ("test.csv", "text/csv; charset=utf-8"),
        # Python's table says application/xml; the system's /etc/mime.types on
        # RHEL-family systems (the manylinux image) says text/xml.
        ("test.xml", {"application/xml", "text/xml; charset=utf-8"}),
        # Fallback for unknown types
        ("test.file", "application/octet-stream"),
    ),
)
def test_guess_content_type(file_path, expected):
    """Test that guess_content_type correctly adds charset for text types."""
    result = guess_content_type(file_path)
    assert result in expected if isinstance(expected, set) else result == expected


def test_guess_content_type_with_custom_fallback():
    """Test that guess_content_type uses custom fallback for unknown types."""
    result = guess_content_type("no_extension", fallback="custom/type")
    assert result == "custom/type"


def test_guess_content_type_with_pathlib():
    """Test that guess_content_type works with pathlib Path objects."""
    from pathlib import Path

    result = guess_content_type(Path("test.html"))
    assert result == "text/html; charset=utf-8"


def test_a_file_stream_response_in_response_middleware(app, tmp_path):
    # Before the stream starts, a file_stream() response has no body in
    # memory and its headers are those it will send.
    path = tmp_path / "data.bin"
    path.write_bytes(b"z" * 10)
    seen = {}

    @app.get("/")
    async def index(request):
        return await file_stream(path)

    @app.on_response
    async def inspect(request, response):
        seen["body"] = response.body
        seen["headers"] = dict(response.processed_headers)
        response.headers["x-seen"] = "1"

    _, response = app.test_client.get("/")
    assert response.status == 200
    assert response.body == b"z" * 10
    assert response.headers["x-seen"] == "1"
    assert seen["body"] is None
    assert seen["headers"][b"content-type"] == b"application/octet-stream"
