"""File responses on the native host: file(), file_stream() and app.static()
read on the shards (Seastar's file API) with Sanic's headers and errors."""
import asyncio
from email.utils import formatdate
import json as json_module
import os

from oldman_server import OldmanServer
from oldman_server.compat import read_file_async, stat_async
from oldman_server.response import ResponseStream, file, file_stream, json
import uvloop

from test_http import config
from test_sanic_http1 import read_chunked, read_response


def make_app(name, directory):
    app = OldmanServer(name, configure_logging=False)
    app.config.ACCESS_LOG = False
    app.static("/static", str(directory), use_content_range=True, name="small")
    app.static("/large", str(directory), use_content_range=True, stream_large_files=1024, name="large")
    app.static("/large-chunked", str(directory), stream_large_files=1024, name="chunked")

    @app.get("/file/<name>")
    async def whole(request, name):
        return await file(directory / name, request_headers=request.headers)

    @app.get("/stream/<name>")
    async def stream(request, name):
        return await file_stream(directory / name, chunk_size=int(request.args.get("chunk", 4096)))

    @app.get("/byname")
    async def byname(request):
        # The file name from the request: a NUL in it must not cut the path short.
        name = request.args.get("name")
        if request.args.get("stream"):
            return await file_stream(directory / name)
        return await file(directory / name)

    @app.get("/mixed")
    async def mixed(request):
        # Data before and after a file in one stream, in order.
        async def write(response):
            await response.write(b"A" * 1000)
            await response.response.stream.send_file(directory / "big.bin", 5, 100000, 4096)
            await response.write(b"B" * 70000)
        return ResponseStream(write)

    @app.get("/vanished")
    async def vanished(request):
        # The file passes the open check but is gone when the shard opens it.
        from oldman_server.native.host import _engine
        check = _engine.file_open_stat

        async def present(path):
            _engine.file_open_stat = check
            return os.stat_result((0o100644, 0, 0, 1, 0, 0, 100, 0, 0, 0))
        _engine.file_open_stat = present
        return await file_stream(directory / "vanished.bin")

    @app.get("/stat/<name>")
    async def stat(request, name):
        path = directory / name
        native, real = await stat_async(path), os.stat(path)
        errors = []
        for target in (directory, directory / "missing"):
            try:
                await read_file_async(target)
            except OSError as error:
                errors.append([type(error).__name__, error.errno, error.filename == os.fspath(target)])
        return json({"equal": native == real and native.st_mtime == real.st_mtime
                     and native.st_mtime_ns == real.st_mtime_ns and native.st_blocks == real.st_blocks,
                     "part": (await read_file_async(path, 3, 5)).decode(), "errors": errors})
    return app


def write_files(directory):
    contents = {
        "small.txt": b"0123456789abcdefghijklmnopqrstuvwxyz",
        "big.bin": os.urandom(1 << 20),
        "empty.txt": b"",
    }
    for name, data in contents.items():
        (directory / name).write_bytes(data)
    (directory / "sub").mkdir()
    return contents


def run(tmp_path, *scenarios):
    contents = write_files(tmp_path)

    async def main():
        from oldman_server.native.host import serve
        app = make_app(f"files_{os.getpid()}_{len(scenarios)}", tmp_path)
        cfg = config(application_dispatch_all=True, max_response_bytes=4 << 20)
        results = []
        async with serve(app, cfg) as engine:
            for scenario in scenarios:
                reader, writer = await asyncio.open_connection("127.0.0.1", cfg.server.http_port)
                try:
                    results.append(await asyncio.wait_for(scenario(reader, writer), 10))
                finally:
                    writer.close()
            await asyncio.sleep(0.1)
            # Native file operations made; no stream or file operation left behind.
            results.append((engine._file_id, len(engine._application_streams), len(engine._files)))
        return results
    return contents, uvloop.run(main())


def header(headers, name):
    return next(v for k, v in headers if k.lower() == name)


def get(path, *extra):
    return (f"GET {path} HTTP/1.1\r\nHost: x\r\n" + "".join(f"{h}\r\n" for h in extra) + "\r\n").encode()


def test_file_and_static_responses(tmp_path):
    async def whole(reader, writer):
        writer.write(get("/file/big.bin"))
        return await read_response(reader)

    async def static_range(reader, writer):
        writer.write(get("/static/small.txt", "Range: bytes=12-19") + get("/static/small.txt")
                     + get("/static/missing.txt") + get("/static/sub"))
        return [await read_response(reader) for _ in range(4)]

    async def not_modified(reader, writer):
        stamp = formatdate(os.stat(tmp_path / "small.txt").st_mtime + 10, usegmt=True)
        writer.write(get("/file/small.txt", f"If-Modified-Since: {stamp}")
                     + f"HEAD /static/small.txt HTTP/1.1\r\nHost: x\r\n\r\n".encode())
        first = await read_response(reader)
        head = await reader.readuntil(b"\r\n\r\n")
        return first, head.decode("latin-1")

    async def stat(reader, writer):
        writer.write(get("/stat/small.txt"))
        return await read_response(reader)

    contents, (full, ranges, (cached, head), stats, operations) = run(
        tmp_path, whole, static_range, not_modified, stat)
    status, headers, body = full
    assert status == 200 and body == contents["big.bin"]
    assert header(headers, "last-modified") == formatdate(os.stat(tmp_path / "big.bin").st_mtime, usegmt=True)
    (s206, h206, b206), (s200, h200, b200), (s404, _, _), (sdir, _, _) = ranges
    assert (s206, b206, header(h206, "content-range")) == (206, b"cdefghij", "bytes 12-19/36")
    assert (s200, b200, header(h200, "accept-ranges")) == (200, contents["small.txt"], "bytes")
    # Missing: 404; a directory without directory_view: 500, as with Sanic's server.
    assert (s404, sdir) == (404, 500)
    # validate_file: 304 for a copy at least as new.
    assert cached[0] == 304
    assert head.startswith("HTTP/1.1 200") and "content-length: 36" in head.lower()
    status, _, body = stats
    assert status == 200
    assert json_module.loads(body) == {
        "equal": True, "part": "34567",
        "errors": [["IsADirectoryError", 21, True], ["FileNotFoundError", 2, True]]}
    assert operations[0] > 0 and operations[1:] == (0, 0)  # served by native file operations


def test_file_streams(tmp_path):
    async def length_framed(reader, writer):
        # use_content_range: Content-Length framing (the stream must match it);
        # a Range response has none and is chunked.
        writer.write(get("/large/big.bin") + get("/large/big.bin", "Range: bytes=1000-299999"))
        return [await read_response(reader), await read_chunked(reader)]

    async def chunked(reader, writer):
        writer.write(get("/large-chunked/big.bin"))
        return await read_chunked(reader)

    async def small_chunks(reader, writer):
        # Each read Sanic's file_stream makes is one chunk on the wire.
        writer.write(get("/stream/small.txt?chunk=10"))
        head = await reader.readuntil(b"\r\n\r\n")
        raw = await reader.readuntil(b"0\r\n\r\n")
        return head.split(b"\r\n")[0], raw

    async def missing_and_empty(reader, writer):
        writer.write(get("/stream/missing.bin") + get("/stream/empty.txt"))
        return [await read_response(reader) for _ in range(2)]

    async def disconnect(reader, writer):
        # The client leaves mid-file; the shard stops and the next request works.
        writer.write(get("/stream/big.bin?chunk=1024"))
        await reader.readuntil(b"\r\n\r\n")
        await reader.readexactly(4096)
        writer.transport.abort()
        await asyncio.sleep(0.2)

    async def after(reader, writer):
        writer.write(get("/stream/small.txt"))
        return await read_chunked(reader)

    async def mixed(reader, writer):
        writer.write(get("/mixed"))
        return await read_chunked(reader)

    async def vanished(reader, writer):
        writer.write(get("/vanished") + get("/stream/small.txt"))
        return await read_chunked(reader), await read_chunked(reader)

    contents, (framed, (cstatus, cheaders, cbody), (line, raw), (missing, empty), _, later,
               mixed_body, (vanished, next_one), operations) = run(
        tmp_path, length_framed, chunked, small_chunks, missing_and_empty, disconnect, after,
        mixed, vanished)
    big = contents["big.bin"]
    (s1, h1, b1), (s2, h2, b2) = framed
    assert (s1, b1) == (200, big) and header(h1, "content-length") == str(len(big))
    assert (s2, b2) == (206, big[1000:300000])
    assert header(h2, "content-range") == f"bytes 1000-299999/{len(big)}"
    assert (cstatus, cbody) == (200, big)
    assert line == b"HTTP/1.1 200 OK"
    assert raw == (b"a\r\n0123456789\r\na\r\nabcdefghij\r\na\r\nklmnopqrst\r\n"
                   b"6\r\nuvwxyz\r\n0\r\n\r\n")
    assert missing[0] == 500  # open() fails before the response starts, as in Sanic
    assert empty[:1] == (200,) and empty[2] == b"" and ("content-length", "0") in [
        (k.lower(), v) for k, v in empty[1]]
    assert later[0] == 200 and later[2] == contents["small.txt"]
    assert mixed_body[2] == b"A" * 1000 + big[5:100005] + b"B" * 70000
    # The shard could not open the file: file_stream raises in the handler after
    # the response started, and the stream ends as Sanic ends it (final chunk,
    # connection kept).
    assert vanished[0] == 200 and vanished[2] == b""
    assert next_one[2] == contents["small.txt"]
    assert operations[0] > 0 and operations[1:] == (0, 0)


def test_paths_with_a_nul_byte_are_refused(tmp_path):
    # Python refuses them (os.stat, open: ValueError); a C path ends at the NUL,
    # which would read secret.key for secret.key\0.txt.
    (tmp_path / "secret.key").write_bytes(b"SECRET")

    async def requests(reader, writer):
        results = []
        for query in ("name=secret.key%00.txt", "name=secret.key%00.txt&stream=1"):
            writer.write(get(f"/byname?{query}"))
            results.append(await read_response(reader))
        return results

    async def direct(reader, writer):
        from oldman_server.native.host import _engine
        errors = []
        for operation in (stat_async, read_file_async, _engine.file_open_stat):
            try:
                await operation(tmp_path / "secret.key\0.txt")
            except ValueError as error:
                errors.append(str(error))
        return errors

    _, (responses, errors, _) = run(tmp_path, requests, direct)
    assert [status for status, _, _ in responses] == [500, 500]
    assert all(b"SECRET" not in body for _, _, body in responses)
    assert errors == ["embedded null byte"] * 3
