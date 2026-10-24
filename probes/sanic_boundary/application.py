"""Matched complete-response fixtures, not a general ASGI product adapter."""
import asyncio
from urllib.parse import unquote

INPUT = bytes(range(256)) * 4
# /sleep: an I/O-bound handler without a client library's own cost (which is
# the same for every framework): suspended for this long, as a handler waiting
# on a database or another service is.
SLEEP_SECONDS = 0.001
EXPECTED = {
    "small": b'{"message":"Hello, World!"}',
    "body": b"127.0.0.1:application/octet-stream:" + INPUT,
    "sleep": b'{"message":"Hello, World!"}',
}


def concurrency(workload):
    """Closed-loop connections: enough that /sleep's wait does not cap the
    throughput (connections / SLEEP_SECONDS = 512k RPS)."""
    return 512 if workload == "sleep" else 128


def make_app(stock=False):
    """The fixture app, the same handlers built with stock Sanic (its source
    must be importable as `sanic`) or with oldman_server."""
    if stock:
        from sanic import Sanic as App
        from sanic.response import json as json_response, raw
    else:
        from oldman_server import OldmanServer as App
        from oldman_server.response import json as json_response, raw

    app = App("boundary_cost", configure_logging=False)
    app.config.ACCESS_LOG = False

    # Each framework's own json() helper (stock: ujson when installed;
    # oldman_server: orjson) is how its users write this; both run the
    # identical handler.
    @app.get("/small")
    async def small(request):
        return json_response({"message": "Hello, World!"})

    @app.post("/body")
    async def body(request):
        prefix = request.headers["host"] + ":" + request.headers["content-type"] + ":"
        return raw(prefix.encode() + request.body, content_type="application/octet-stream")

    @app.get("/sleep")
    async def sleep(request):
        await asyncio.sleep(SLEEP_SECONDS)
        return json_response({"message": "Hello, World!"})

    return app


def buffered_asgi(app, max_body):
    """Use actual owned metadata and the native adapter's complete-body limits."""
    async def handle(request):
        if not request.http_version or request.client is None or request.server is None:
            raise ValueError("native request metadata is required")
        path, _, query = request.target.partition(b"?")
        scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.0"},
                 "http_version": request.http_version, "method": request.method,
                 "scheme": "http", "path": unquote(path.decode("ascii")), "raw_path": path,
                 "query_string": query, "root_path": "", "client": request.client,
                 "server": request.server,
                 "headers": [(k.lower(), v) for k, v in request.headers]}
        received = False
        response = body = failure = None

        async def receive():
            nonlocal received
            if not received:
                received = True
                return {"type": "http.request", "body": request.body, "more_body": False}
            await asyncio.Future()

        async def send(message):
            nonlocal response, body, failure
            try:
                if failure is not None:
                    raise failure
                if message["type"] == "http.response.start":
                    if response is not None or message.get("trailers"):
                        raise ValueError("duplicate start or unsupported trailers")
                    response = message
                elif message["type"] == "http.response.body":
                    if response is None or body is not None or message.get("more_body", False):
                        raise ValueError("streaming or invalid response order")
                    body = message.get("body", b"")
                    if type(body) is not bytes or len(body) > max_body:
                        raise ValueError("response body exceeds the native limit")
                else:
                    raise ValueError("unsupported response event")
            except Exception as error:
                failure = error
                raise

        await app(scope, receive, send)
        if failure is not None:
            raise failure
        if response is None or body is None:
            raise ValueError("incomplete response")
        headers, lengths, names = [], [], set()
        header_bytes = 0
        for name, value in response.get("headers", []):
            lower = name.lower()
            if lower in names:
                raise ValueError("duplicate response headers are not supported")
            names.add(lower)
            if lower == b"content-length":
                lengths.append(value)
            else:
                header_bytes += len(name) + len(value) + 4
                if len(headers) == 128 or header_bytes > 8192:
                    raise ValueError("response headers exceed the native limit")
                headers.append((name.decode("ascii"), value.decode("latin-1")))
        if lengths and (not lengths[0].isdigit() or int(lengths[0]) != len(body)):
            raise ValueError("inconsistent content-length")
        return response["status"], body, tuple(headers)
    return handle
