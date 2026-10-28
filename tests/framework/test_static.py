import logging
import os
import sys
from collections import Counter
from pathlib import Path
from time import gmtime, strftime
from urllib.parse import unquote

import pytest
from oldman_server import OldmanServer, text
from oldman_server.exceptions import FileNotFound, ServerError
from oldman_server.response import file

pytestmark = pytest.mark.xdist_group(name="static_files")


@pytest.fixture(scope="module")
def double_dotted_directory_file(static_file_directory: str):
    """Generate double dotted directory and its files"""
    file_path = Path(static_file_directory) / "dotted.." / "dot.txt"
    double_dotted_dir = file_path.parent
    Path.mkdir(double_dotted_dir, exist_ok=True)
    with open(file_path, "w") as f:
        f.write("DOT\n")
    yield file_path
    Path.unlink(file_path)
    Path.rmdir(double_dotted_dir)


def get_file_path(static_file_directory, file_name):
    return os.path.join(static_file_directory, file_name)


def get_file_content(static_file_directory, file_name):
    """The content of the static file to check"""
    with open(get_file_path(static_file_directory, file_name), "rb") as file:
        return file.read()


@pytest.fixture(scope="module")
def large_file(static_file_directory):
    large_file_path = os.path.join(static_file_directory, "large.file")

    size = 2 * 1024 * 1024
    with open(large_file_path, "w") as f:
        f.write("a" * size)

    yield large_file_path

    os.remove(large_file_path)


@pytest.fixture(autouse=True, scope="module")
def symlink(static_file_directory):
    src = os.path.abspath(
        os.path.join(os.path.dirname(static_file_directory), "conftest.py")
    )
    symlink = "symlink"
    dist = os.path.join(static_file_directory, symlink)
    if os.path.islink(dist):
        os.remove(dist)
    os.symlink(src, dist)
    yield symlink
    if os.path.exists(dist):
        os.remove(dist)


@pytest.fixture(autouse=True, scope="module")
def hard_link(static_file_directory):
    src = os.path.abspath(
        os.path.join(os.path.dirname(static_file_directory), "conftest.py")
    )
    hard_link = "hard_link"
    dist = os.path.join(static_file_directory, hard_link)
    if os.path.exists(dist):
        os.remove(dist)
    os.link(src, dist)
    yield hard_link
    if os.path.exists(dist):
        os.remove(dist)


@pytest.mark.parametrize(
    "file_name",
    ["test.file", "decode me.txt", "python.png", "symlink", "hard_link"],
)
def test_static_file(app, static_file_directory, file_name):
    app.static(
        "/testing.file", get_file_path(static_file_directory, file_name)
    )

    request, response = app.test_client.get("/testing.file")
    assert response.status == 200
    assert response.body == get_file_content(static_file_directory, file_name)


@pytest.mark.parametrize(
    "file_name",
    ["test.file", "decode me.txt", "python.png", "symlink", "hard_link"],
)
def test_static_file_pathlib(app, static_file_directory, file_name):
    file_path = Path(get_file_path(static_file_directory, file_name))
    app.static("/testing.file", file_path)
    request, response = app.test_client.get("/testing.file")
    assert response.status == 200
    assert response.body == get_file_content(static_file_directory, file_name)


@pytest.mark.parametrize(
    "file_name",
    [
        "test.file",
        "decode me.txt",
        "python.png",
        "symlink",
        "hard_link",
    ],
)
def test_static_file_pathlib_relative_path_traversal(
    app, static_file_directory, file_name, monkeypatch
):
    """A directory given relative to the working directory, through '..'."""
    monkeypatch.chdir(Path(__file__).parent)

    file_path = "./static/../static/"
    app.static("/", file_path, follow_external_symlink_files=True)
    _, response = app.test_client.get(f"/{file_name}")
    assert response.status == 200
    assert response.body == get_file_content(static_file_directory, file_name)


@pytest.mark.parametrize(
    "file_name",
    [b"test.file", b"decode me.txt", b"python.png"],
)
def test_static_file_bytes(app, static_file_directory, file_name):
    bsep = os.path.sep.encode("utf-8")
    file_path = static_file_directory.encode("utf-8") + bsep + file_name
    message = "Static file or directory must be a path-like object or string"
    with pytest.raises(TypeError, match=message):
        app.static("/testing.file", file_path)


@pytest.mark.parametrize(
    "file_name",
    [{}, [], object()],
)
def test_static_file_invalid_path(app, static_file_directory, file_name):
    app.route("/")(lambda x: x)
    with pytest.raises(ValueError):
        app.static("/testing.file", file_name)
    request, response = app.test_client.get("/testing.file")
    assert response.status == 404


@pytest.mark.parametrize(
    "file_name,expected_content_type",
    [
        ("decode me.txt", "text/plain; charset=utf-8"),
        ("test.html", "text/html; charset=utf-8"),
        ("python.png", "image/png"),
        # Note: file() default for unknown types differs from app.static
        ("test.file", "text/plain; charset=utf-8"),
    ],
)
def test_file_response_content_type(
    app: OldmanServer, file_name, expected_content_type, static_file_directory
):
    """Responses by file() rather than app.static."""

    @app.get("/files/<filename>")
    def file_route(request, filename):
        file_path = os.path.join(static_file_directory, filename)
        file_path = os.path.abspath(unquote(file_path))
        return file(file_path)

    request, response = app.test_client.get(f"/files/{file_name}")
    assert response.status == 200
    assert response.headers["Content-Type"] == expected_content_type


@pytest.mark.parametrize(
    "file_name,expected_content_type",
    [
        ("decode me.txt", "text/plain; charset=utf-8"),
        ("test.html", "text/html; charset=utf-8"),
        ("python.png", "image/png"),
        ("test.file", "application/octet-stream"),
    ],
)
def test_static_file_content_type(
    app: OldmanServer, file_name, expected_content_type, static_file_directory
):
    """Test that file responses automatically include charset for text."""

    app.static("/", static_file_directory)
    request, response = app.test_client.get(file_name)
    assert response.status == 200
    assert response.headers["Content-Type"] == expected_content_type


def test_static_file_content_type_forced(app, static_file_directory):
    app.static(
        "/testing.file",
        get_file_path(static_file_directory, "decode me.txt"),
        content_type="text/plain;charset=ISO-8859-1",
    )

    request, response = app.test_client.get("/testing.file")
    assert response.status == 200
    assert response.headers["Content-Type"] == "text/plain;charset=ISO-8859-1"


@pytest.mark.parametrize(
    "file_name", ["test.file", "decode me.txt", "symlink", "hard_link"]
)
@pytest.mark.parametrize("base_uri", ["/static", "", "/dir"])
def test_static_directory(app, file_name, base_uri, static_file_directory):
    app.static(
        base_uri,
        static_file_directory,
        follow_external_symlink_files=(file_name == "symlink"),
    )

    request, response = app.test_client.get(uri=f"{base_uri}/{file_name}")
    assert response.status == 200
    assert response.body == get_file_content(static_file_directory, file_name)


@pytest.mark.parametrize("file_name", ["test.file", "decode me.txt"])
def test_static_head_request(app, file_name, static_file_directory):
    app.static(
        "/testing.file",
        get_file_path(static_file_directory, file_name),
        use_content_range=True,
    )

    request, response = app.test_client.head("/testing.file")
    assert response.status == 200
    assert "Accept-Ranges" in response.headers
    assert "Content-Length" in response.headers
    assert int(response.headers["Content-Length"]) == len(
        get_file_content(static_file_directory, file_name)
    )


# Headers a HEAD response may differ in from GET: per response or per connection.
_PER_RESPONSE = {"date", "connection", "keep-alive"}


@pytest.mark.parametrize("options", (
    {},
    {"use_content_range": True},
    {"stream_large_files": 200_000},
), ids=("plain", "ranges", "streamed"))
@pytest.mark.parametrize("size", (100, 300_000), ids=("small", "large"))
def test_static_head_sends_the_get_headers(app, tmp_path, options, size):
    """HEAD gets the header fields GET would (RFC 9110 9.3.2) without the
    body: Content-Length, Cache-Control and the rest, for a small file, a
    large one (written by the native host) and a streamed one. It answered
    Content-Length: 0 without Cache-Control."""
    (tmp_path / "data.bin").write_bytes(b"x" * size)
    app.static("/files", tmp_path, **options)

    _, get = app.test_client.get("/files/data.bin")
    _, head = app.test_client.head("/files/data.bin")

    assert get.status == head.status == 200
    assert len(get.body) == size and head.body == b""
    get_headers = {k: v for k, v in get.headers.items() if k not in _PER_RESPONSE}
    head_headers = {k: v for k, v in head.headers.items() if k not in _PER_RESPONSE}
    assert head_headers == get_headers


def test_static_head_is_conditional_and_ignores_ranges(app, tmp_path):
    (tmp_path / "data.bin").write_bytes(b"x" * 1000)
    app.static("/files", tmp_path, use_content_range=True)

    _, get = app.test_client.get("/files/data.bin")
    modified = {"If-Modified-Since": get.headers["Last-Modified"]}
    _, head = app.test_client.head("/files/data.bin", headers=modified)
    assert head.status == 304

    # Range is defined for GET only (RFC 9110 14.2): HEAD describes the
    # whole representation.
    _, head = app.test_client.head("/files/data.bin", headers={"Range": "bytes=0-9"})
    assert head.status == 200
    assert head.headers["Content-Length"] == "1000"


@pytest.mark.parametrize("file_name", ["test.file", "decode me.txt"])
def test_static_content_range_correct(app, file_name, static_file_directory):
    app.static(
        "/testing.file",
        get_file_path(static_file_directory, file_name),
        use_content_range=True,
    )

    headers = {"Range": "bytes=12-19"}
    request, response = app.test_client.get("/testing.file", headers=headers)
    assert response.status == 206
    assert "Content-Length" in response.headers
    assert "Content-Range" in response.headers
    static_content = bytes(get_file_content(static_file_directory, file_name))[
        12:20
    ]
    assert int(response.headers["Content-Length"]) == len(static_content)
    assert response.body == static_content


@pytest.mark.parametrize("file_name", ["test.file", "decode me.txt"])
def test_static_content_range_front(app, file_name, static_file_directory):
    app.static(
        "/testing.file",
        get_file_path(static_file_directory, file_name),
        use_content_range=True,
    )

    headers = {"Range": "bytes=12-"}
    request, response = app.test_client.get("/testing.file", headers=headers)
    assert response.status == 206
    assert "Content-Length" in response.headers
    assert "Content-Range" in response.headers
    static_content = bytes(get_file_content(static_file_directory, file_name))[
        12:
    ]
    assert int(response.headers["Content-Length"]) == len(static_content)
    assert response.body == static_content


@pytest.mark.parametrize("file_name", ["test.file", "decode me.txt"])
def test_static_content_range_back(app, file_name, static_file_directory):
    app.static(
        "/testing.file",
        get_file_path(static_file_directory, file_name),
        use_content_range=True,
    )

    headers = {"Range": "bytes=-12"}
    request, response = app.test_client.get("/testing.file", headers=headers)
    assert response.status == 206
    assert "Content-Length" in response.headers
    assert "Content-Range" in response.headers
    static_content = bytes(get_file_content(static_file_directory, file_name))[
        -12:
    ]
    assert int(response.headers["Content-Length"]) == len(static_content)
    assert response.body == static_content


@pytest.mark.parametrize("use_modified_since", [True, False])
@pytest.mark.parametrize("file_name", ["test.file", "decode me.txt"])
def test_static_content_range_empty(
    app, file_name, static_file_directory, use_modified_since
):
    app.static(
        "/testing.file",
        get_file_path(static_file_directory, file_name),
        use_content_range=True,
        use_modified_since=use_modified_since,
    )

    request, response = app.test_client.get("/testing.file")
    assert response.status == 200
    assert "Content-Length" in response.headers
    assert "Content-Range" not in response.headers
    assert int(response.headers["Content-Length"]) == len(
        get_file_content(static_file_directory, file_name)
    )
    assert response.body == bytes(
        get_file_content(static_file_directory, file_name)
    )


@pytest.mark.parametrize("file_name", ["test.file", "decode me.txt"])
def test_static_content_range_error(app, file_name, static_file_directory):
    app.static(
        "/testing.file",
        get_file_path(static_file_directory, file_name),
        use_content_range=True,
    )

    headers = {"Range": "bytes=1-0"}
    request, response = app.test_client.get("/testing.file", headers=headers)
    assert response.status == 416
    assert "Content-Length" in response.headers
    assert "Content-Range" in response.headers
    assert response.headers["Content-Range"] == "bytes */{}".format(
        len(get_file_content(static_file_directory, file_name)),
    )


@pytest.mark.parametrize("file_name", ["test.file", "decode me.txt"])
def test_static_content_range_invalid_unit(
    app, file_name, static_file_directory
):
    app.static(
        "/testing.file",
        get_file_path(static_file_directory, file_name),
        use_content_range=True,
    )

    unit = "bit"
    headers = {"Range": f"{unit}=1-0"}
    request, response = app.test_client.get("/testing.file", headers=headers)

    assert response.status == 416
    assert f"{unit} is not a valid Range Type" in response.text


@pytest.mark.parametrize("file_name", ["test.file", "decode me.txt"])
def test_static_content_range_invalid_start(
    app, file_name, static_file_directory
):
    app.static(
        "/testing.file",
        get_file_path(static_file_directory, file_name),
        use_content_range=True,
    )

    start = "start"
    headers = {"Range": f"bytes={start}-0"}
    request, response = app.test_client.get("/testing.file", headers=headers)

    assert response.status == 416
    assert f"'{start}' is invalid for Content Range" in response.text


@pytest.mark.parametrize("file_name", ["test.file", "decode me.txt"])
def test_static_content_range_invalid_end(
    app, file_name, static_file_directory
):
    app.static(
        "/testing.file",
        get_file_path(static_file_directory, file_name),
        use_content_range=True,
    )

    end = "end"
    headers = {"Range": f"bytes=1-{end}"}
    request, response = app.test_client.get("/testing.file", headers=headers)

    assert response.status == 416
    assert f"'{end}' is invalid for Content Range" in response.text


@pytest.mark.parametrize("file_name", ["test.file", "decode me.txt"])
def test_static_content_range_invalid_parameters(
    app, file_name, static_file_directory
):
    app.static(
        "/testing.file",
        get_file_path(static_file_directory, file_name),
        use_content_range=True,
    )

    headers = {"Range": "bytes=-"}
    request, response = app.test_client.get("/testing.file", headers=headers)

    assert response.status == 416
    assert "Invalid for Content Range parameters" in response.text


@pytest.mark.parametrize(
    "file_name", ["test.file", "decode me.txt", "python.png"]
)
def test_static_file_specified_host(app, static_file_directory, file_name):
    app.static(
        "/testing.file",
        get_file_path(static_file_directory, file_name),
        host="www.example.com",
    )

    headers = {"Host": "www.example.com"}
    request, response = app.test_client.get("/testing.file", headers=headers)
    assert response.status == 200
    assert response.body == get_file_content(static_file_directory, file_name)
    request, response = app.test_client.get("/testing.file")
    assert response.status == 404


@pytest.mark.parametrize("use_modified_since", [True, False])
@pytest.mark.parametrize("stream_large_files", [True, 1024])
@pytest.mark.parametrize("file_name", ["test.file", "large.file"])
def test_static_stream_large_file(
    app,
    static_file_directory,
    file_name,
    use_modified_since,
    stream_large_files,
    large_file,
):
    app.static(
        "/testing.file",
        get_file_path(static_file_directory, file_name),
        use_modified_since=use_modified_since,
        stream_large_files=stream_large_files,
    )

    request, response = app.test_client.get("/testing.file")

    assert response.status == 200
    assert response.body == get_file_content(static_file_directory, file_name)


@pytest.mark.parametrize(
    "file_name", ["test.file", "decode me.txt", "python.png"]
)
def test_use_modified_since(app, static_file_directory, file_name):
    file_stat = os.stat(get_file_path(static_file_directory, file_name))
    modified_since = strftime(
        "%a, %d %b %Y %H:%M:%S GMT", gmtime(file_stat.st_mtime)
    )

    app.static(
        "/testing.file",
        get_file_path(static_file_directory, file_name),
        use_modified_since=True,
    )

    request, response = app.test_client.get(
        "/testing.file", headers={"If-Modified-Since": modified_since}
    )

    assert response.status == 304


def test_file_not_found(app, static_file_directory):
    app.static("/static", static_file_directory)

    request, response = app.test_client.get("/static/not_found")

    assert response.status == 404
    assert "File not found" in response.text


@pytest.mark.parametrize("static_name", ["_static_name", "static"])
@pytest.mark.parametrize("file_name", ["test.html"])
def test_static_name(app, static_file_directory, static_name, file_name):
    app.static("/static", static_file_directory, name=static_name)

    request, response = app.test_client.get(f"/static/{file_name}")

    assert response.status == 200


def test_nested_dir(app, static_file_directory):
    app.static("/static", static_file_directory)

    request, response = app.test_client.get("/static/nested/dir/foo.txt")

    assert response.status == 200
    assert response.text == "foo\n"


def test_a_directory_is_answered_by_the_directory_handler(app, static_file_directory):
    # A directory without an index is a 404, for GET and HEAD alike; a
    # DirectoryHandler subclass answers directories its own way.
    from oldman_server.handlers.directory import DirectoryHandler

    error_text = "Is a directory. Access denied"

    class DenyDirectories(DirectoryHandler):
        async def handle(self, request, path):
            return text(error_text, status=403)

    app.static("/static", static_file_directory)
    app.static(
        "/denied",
        static_file_directory,
        name="denied",
        directory_handler=DenyDirectories("/denied", Path(static_file_directory)),
    )

    for method in ("get", "head"):
        _, response = getattr(app.test_client, method)("/static/")
        assert response.status == 404
    _, response = app.test_client.get("/denied/")
    assert response.status == 403
    assert response.text == error_text


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads any file")
def test_an_unreadable_file_is_forbidden(app, tmp_path, caplog):
    secret = tmp_path / "secret.txt"
    secret.write_text("secret")
    secret.chmod(0)
    app.static("/files", tmp_path)

    with caplog.at_level(logging.WARNING):
        _, response = app.test_client.get("/files/secret.txt")

    assert response.status == 403
    assert any("not readable" in message for message in caplog.messages)
    assert not any(record.exc_info for record in caplog.records)


def test_stack_trace_on_not_found(app, static_file_directory, caplog):
    app.static("/static", static_file_directory)

    with caplog.at_level(logging.INFO):
        _, response = app.test_client.get("/static/non_existing_file.file")

    counter = Counter([(r[0], r[1]) for r in caplog.record_tuples])

    assert response.status == 404
    # The startup line, the test client's request and "Server Stopped".
    assert counter[("oldman_server.root", logging.INFO)] == 3
    assert counter[("oldman_server.root", logging.ERROR)] == 0
    assert counter[("oldman_server.error", logging.ERROR)] == 0
    assert counter[("oldman_server.server", logging.INFO)] == 4


def test_no_stack_trace_on_not_found(app, static_file_directory, caplog):
    app.static("/static", static_file_directory)

    @app.exception(FileNotFound)
    async def file_not_found(request, exception):
        return text(f"No file: {request.path}", status=404)

    with caplog.at_level(logging.INFO):
        _, response = app.test_client.get("/static/non_existing_file.file")

    counter = Counter([(r[0], r[1]) for r in caplog.record_tuples])

    assert response.status == 404
    # The startup line, the test client's request and "Server Stopped".
    assert counter[("oldman_server.root", logging.INFO)] == 3
    assert counter[("oldman_server.root", logging.ERROR)] == 0
    assert counter[("oldman_server.error", logging.ERROR)] == 0
    assert counter[("oldman_server.server", logging.INFO)] == 4
    assert response.text == "No file: /static/non_existing_file.file"


@pytest.mark.asyncio
async def test_multiple_statics_error(app, static_file_directory):
    app.static("/file", get_file_path(static_file_directory, "test.file"))
    app.static("/png", get_file_path(static_file_directory, "python.png"))

    message = (
        r"Duplicate route names detected: test_multiple_statics_error\.static"
    )
    with pytest.raises(ServerError, match=message):
        await app._startup()


def test_multiple_statics(app, static_file_directory):
    app.static(
        "/file", get_file_path(static_file_directory, "test.file"), name="file"
    )
    app.static(
        "/png", get_file_path(static_file_directory, "python.png"), name="png"
    )

    _, response = app.test_client.get("/file")
    assert response.status == 200
    assert response.body == get_file_content(
        static_file_directory, "test.file"
    )

    _, response = app.test_client.get("/png")
    assert response.status == 200
    assert response.body == get_file_content(
        static_file_directory, "python.png"
    )


@pytest.mark.asyncio
async def test_resource_type_default_error(app, static_file_directory):
    app.static("/static", static_file_directory)
    app.static("/file", get_file_path(static_file_directory, "test.file"))

    message = (
        r"Duplicate route names "
        r"detected: test_resource_type_default_error\.static"
    )
    with pytest.raises(ServerError, match=message):
        await app._startup()


def test_resource_type_default(app, static_file_directory):
    app.static("/static", static_file_directory, name="static")
    app.static(
        "/file", get_file_path(static_file_directory, "test.file"), name="file"
    )

    _, response = app.test_client.get("/static")
    assert response.status == 404

    _, response = app.test_client.get("/file")
    assert response.status == 200
    assert response.body == get_file_content(
        static_file_directory, "test.file"
    )


def test_resource_type_file(app, static_file_directory):
    app.static(
        "/file",
        get_file_path(static_file_directory, "test.file"),
        resource_type="file",
    )

    _, response = app.test_client.get("/file")
    assert response.status == 200
    assert response.body == get_file_content(
        static_file_directory, "test.file"
    )

    with pytest.raises(TypeError):
        app.static("/static", static_file_directory, resource_type="file")


def test_resource_type_dir(app, static_file_directory):
    app.static("/static", static_file_directory, resource_type="dir")

    _, response = app.test_client.get("/static/test.file")
    assert response.status == 200
    assert response.body == get_file_content(
        static_file_directory, "test.file"
    )

    with pytest.raises(TypeError):
        app.static(
            "/file",
            get_file_path(static_file_directory, "test.file"),
            resource_type="dir",
        )


def test_resource_type_unknown(app, static_file_directory, caplog):
    with pytest.raises(ValueError):
        app.static("/static", static_file_directory, resource_type="unknown")


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows does not support double dotted directories",
)
def test_dotted_dir_ok(
    app: OldmanServer, static_file_directory: str, double_dotted_directory_file: Path
):
    app.static("/foo", static_file_directory)
    dot_relative_path = str(
        double_dotted_directory_file.relative_to(static_file_directory)
    )
    _, response = app.test_client.get("/foo/" + dot_relative_path)
    assert response.status == 200
    assert response.body == b"DOT\n"


def test_breakout(app: OldmanServer, static_file_directory: str):
    app.static("/foo", static_file_directory)

    _, response = app.test_client.get("/foo/..%2Ffake/server.py")
    assert response.status == 404

    _, response = app.test_client.get("/foo/..%2Fstatic/test.file")
    assert response.status == 404


@pytest.mark.skipif(
    sys.platform != "win32", reason="Block backslash on Windows only"
)
def test_double_backslash_prohibited_on_win32(
    app: OldmanServer, static_file_directory: str
):
    app.static("/foo", static_file_directory)

    _, response = app.test_client.get("/foo/static/..\\static/test.file")
    assert response.status == 404
    _, response = app.test_client.get("/foo/static\\../static/test.file")
    assert response.status == 404


def test_head_on_a_directory_sends_the_index_headers(app, tmp_path):
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "index.html").write_text("<p>index</p>")
    app.static("/site", tmp_path, index="index.html")

    _, get = app.test_client.get("/site/docs/")
    _, head = app.test_client.head("/site/docs/")
    assert get.status == head.status == 200
    assert head.body == b""
    assert head.headers["content-length"] == get.headers["content-length"] == "12"
    assert head.headers["content-type"] == get.headers["content-type"]
