import json as stdjson

from collections import namedtuple
from sys import version_info

import pytest

from oldman_server.testing.reusable import ReusableClient

from oldman_server import json, text
from oldman_server.app import OldmanServer
from tests.framework.client import RawClient


@pytest.fixture
def test_app(app: OldmanServer):
    app.config.KEEP_ALIVE_TIMEOUT = 1

    @app.get("/")
    async def base_handler(request):
        return text("111122223333444455556666777788889999")

    @app.post("/upload", stream=True)
    async def upload_handler(request):
        data = [part.decode("utf-8") async for part in request.stream]
        return json(data)

    @app.get("/headers")
    async def headers_handler(request):
        return json({"items": list(request.headers.items()),
                     "trail": request.headers.get("x-trail")})

    return app


@pytest.fixture
def runner(test_app: OldmanServer, port):
    client = ReusableClient(test_app, port=port)
    client.run()
    yield client
    client.stop()


@pytest.fixture
def client(runner: ReusableClient):
    client = namedtuple("Client", ("raw", "send", "recv"))

    raw = RawClient(runner.host, runner.port)
    runner._run(raw.connect())

    def send(msg):
        nonlocal runner
        nonlocal raw
        runner._run(raw.send(msg))

    def recv(**kwargs):
        nonlocal runner
        nonlocal raw
        method = raw.recv_until if "until" in kwargs else raw.recv
        return runner._run(method(**kwargs))

    yield client(raw, send, recv)

    runner._run(raw.close())


def _chunked_trailer_payload(
    path: str | None = None, field_name: str = "a"
) -> bytes:
    prefix = (
        b"POST / HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"Connection: keep-alive\r\n"
        b"Transfer-Encoding: chunked\r\n"
        b"\r\n"
        b"1\r\n"
        b"X\r\n"
    )
    if path is None:
        return prefix + b"0\r\n\r\n"
    return (
        prefix
        + b"0\r\n"
        + f"{field_name}:GET {path} HTTP/1.1\r\n".encode()
        + b"Host: localhost\r\n"
        + b"\r\n"
    )


def test_full_message(client):
    client.send(
        """
        GET / HTTP/1.1
        host: localhost:7777

        """
    )
    response = client.recv()

    # AltSvcCheck touchup removes the Alt-Svc header from the
    # response in the Python 3.9+ in this case
    assert len(response) == (151 if version_info < (3, 9) else 140)
    assert b"200 OK" in response


def test_transfer_chunked(client):
    client.send(
        """
        POST /upload HTTP/1.1
        transfer-encoding: chunked

        """
    )
    client.send(b"3\r\nfoo\r\n")
    client.send(b"3\r\nbar\r\n")
    client.send(b"0\r\n\r\n")
    response = client.recv()
    _, body = response.rsplit(b"\r\n\r\n", 1)
    data = stdjson.loads(body)

    assert data == ["foo", "bar"]


def test_url_encoding(client):
    client.send(
        """
        GET /invalid\xa0url HTTP/1.1

        """
    )
    response = client.recv()
    headers, body = response.rsplit(b"\r\n\r\n", 1)

    assert b"400 Bad Request" in headers
    assert b"URL may only contain US-ASCII characters." in body


@pytest.mark.parametrize(
    "content_length",
    (
        b"-50",
        b"+50",
        b"5_0",
        b"50.5",
    ),
)
def test_invalid_content_length(content_length, client):
    body = b"Hello" * 10
    client.send(
        b"POST /upload HTTP/1.1\r\n"
        + b"content-length: "
        + content_length
        + b"\r\n\r\n"
        + body
        + b"\r\n\r\n"
    )

    response = client.recv()
    headers, body = response.rsplit(b"\r\n\r\n", 1)

    assert b"400 Bad Request" in headers
    assert b"Bad content-length" in body


@pytest.mark.parametrize(
    "chunk_length",
    (
        b"-50",
        b"+50",
        b"5_0",
        b"50.5",
    ),
)
def test_invalid_chunk_length(chunk_length, client):
    body = b"Hello" * 10
    client.send(
        b"POST /upload HTTP/1.1\r\n"
        + b"transfer-encoding: chunked\r\n\r\n"
        + chunk_length
        + b"\r\n"
        + body
        + b"\r\n"
        + b"0\r\n\r\n"
    )

    response = client.recv()
    headers, body = response.rsplit(b"\r\n\r\n", 1)

    assert b"400 Bad Request" in headers
    assert b"Bad chunked encoding" in body


def test_smuggle(client):
    client.send(
        """
        POST /upload HTTP/1.1
        Content-Length: 5
        Transfer-Encoding: chunked
        Transfer-Encoding: xchunked

        5
        hello
        0

        GET / HTTP/1.1

        """  # noqa
    )

    response = client.recv()
    num_responses = response.count(b"HTTP/1.1")
    assert num_responses == 1

    headers, body = response.rsplit(b"\r\n\r\n", 1)
    assert b"400 Bad Request" in headers
    assert b"Bad Request" in body


def test_chunked_trailer_baseline(client):
    client.send(_chunked_trailer_payload())

    response = client.recv()
    assert response.count(b"HTTP/1.1") == 1
    assert b"405 Method Not Allowed" in response


def test_chunked_trailer_smuggle_root(client):
    client.send(_chunked_trailer_payload("/"))

    response = client.recv()
    assert response.count(b"HTTP/1.1") == 1
    assert b"405 Method Not Allowed" in response
    assert b"200 OK" not in response
    assert b"111122223333444455556666777788889999" not in response


def test_chunked_trailer_smuggle_404(client):
    client.send(_chunked_trailer_payload("/this-path-should-not-exist"))

    response = client.recv()
    assert response.count(b"HTTP/1.1") == 1
    assert b"405 Method Not Allowed" in response
    assert b"404 Not Found" not in response


def test_chunked_trailer_smuggle_offset_control(client):
    client.send(_chunked_trailer_payload("/", field_name="ab"))

    response = client.recv()
    assert response.count(b"HTTP/1.1") == 1
    assert b"405 Method Not Allowed" in response
    assert b"Method :GET not allowed" not in response


def test_chunked_trailer_rejected(client):
    client.send(
        b"POST /upload HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"Connection: keep-alive\r\n"
        b"Transfer-Encoding: chunked\r\n"
        b"\r\n"
        b"3\r\nfoo\r\n"
        b"0\r\n"
        b"X-Checksum: deadbeef\r\n"
        b"\r\n"
    )

    response = client.recv()
    assert response.count(b"HTTP/1.1") == 1
    assert b"400 Bad Request" in response
    assert b"200 OK" not in response


def test_chunked_trailer_smuggle_chunk_extension(client):
    client.send(
        b"POST / HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"Connection: keep-alive\r\n"
        b"Transfer-Encoding: chunked\r\n"
        b"\r\n"
        b"1\r\nX\r\n"
        b"0;ext=1\r\n"
        b"a:GET / HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"\r\n"
    )

    response = client.recv()
    assert response.count(b"HTTP/1.1") == 1
    assert b"405 Method Not Allowed" in response
    assert b"200 OK" not in response


def test_chunked_trailer_smuggle_no_data_chunk(client):
    client.send(
        b"POST / HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"Connection: keep-alive\r\n"
        b"Transfer-Encoding: chunked\r\n"
        b"\r\n"
        b"0\r\n"
        b"a:GET / HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"\r\n"
    )

    response = client.recv()
    assert response.count(b"HTTP/1.1") == 1
    assert b"405 Method Not Allowed" in response
    assert b"200 OK" not in response


def test_chunked_trailer_smuggle_fragmented(client):
    client.send(
        b"POST / HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"Connection: keep-alive\r\n"
        b"Transfer-Encoding: chunked\r\n"
        b"\r\n"
        b"1\r\nX\r\n"
    )
    client.send(b"0\r\n")
    client.send(b"a:GET / HTTP/1.1\r\n")
    client.send(b"Host: localhost\r\n\r\n")

    response = client.recv()
    assert response.count(b"HTTP/1.1") == 1
    assert b"405 Method Not Allowed" in response
    assert b"200 OK" not in response


def test_legit_pipelining_preserved(client):
    client.send(
        b"POST / HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"Connection: keep-alive\r\n"
        b"Transfer-Encoding: chunked\r\n"
        b"\r\n"
        b"1\r\nX\r\n"
        b"0\r\n\r\n"
        b"GET / HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"\r\n"
    )

    response = client.recv()
    assert response.count(b"HTTP/1.1") == 2
    assert b"405 Method Not Allowed" in response
    assert b"200 OK" in response
    assert b"111122223333444455556666777788889999" in response


def test_header_names_and_values_through_either_server(client):
    # Iteration gives lowercase names; values lose the optional whitespace on
    # both sides (RFC 9110 OWS). The same through the asyncio server and the
    # native host.
    client.send(
        b"GET /headers HTTP/1.1\r\nHost: localhost\r\nX-Trail: v  \r\n"
        b"X-Lead:   w\r\nConnection: close\r\n\r\n"
    )
    response = client.recv()
    body = stdjson.loads(response.split(b"\r\n\r\n", 1)[1])
    assert body == {"trail": "v", "items": [
        ["host", "localhost"], ["x-trail", "v"], ["x-lead", "w"], ["connection", "close"]]}


@pytest.mark.parametrize("chunk", [
    b"0x5\r\nhello\r\n0\r\n\r\n",    # not a hex number (int("0x5", 16) would take it)
    b" 5\r\nhello\r\n0\r\n\r\n",     # whitespace around it
    b"5\r\nhelloXY0\r\n\r\n",        # the data not followed by CRLF
])
def test_malformed_chunks_are_rejected_on_either_server(client, chunk):
    # RFC 9112 7.1: the chunk size is hex digits and the data ends with CRLF;
    # the native host rejects the rest, the asyncio server too.
    client.send(
        b"POST /upload HTTP/1.1\r\nHost: localhost\r\n"
        b"Transfer-Encoding: chunked\r\n\r\n" + chunk
    )
    response = client.recv()
    assert response.startswith(b"HTTP/1.1 400"), response
