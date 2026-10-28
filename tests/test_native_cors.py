"""CORS on the native host where only it answers: the overload 503, a protocol
error, and a preflight it answers itself (the exact bytes). The behavior
shared with the asyncio server is in tests/framework/test_cors.py.

Each scenario runs in its own interpreter: a process starts one native runtime.
"""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import textwrap


ROOT = Path(__file__).resolve().parents[1]

APP = textwrap.dedent('''
    import asyncio, json, socket, threading, time
    from oldman_server import OldmanServer
    from oldman_server.response import text

    app = OldmanServer("native_cors", configure_logging=False)
    app.config.NATIVE_SERVER = True
    app.config.NATIVE_SHARDS = 1
    app.config.CORS_ORIGINS = "https://a.example"
    seen = []
    loop = []

    @app.before_server_start
    def keep_loop(app):
        loop.append(asyncio.get_running_loop())

    @app.on_request
    def middleware(request):
        seen.append([request.method, request.path])

    def exchange(sock, raw):
        sock.sendall(raw)
        data = b""
        while b"\\r\\n\\r\\n" not in data:
            chunk = sock.recv(65536)
            if not chunk:
                break
            data += chunk
        head, _, rest = data.partition(b"\\r\\n\\r\\n")
        lines = head.decode().split("\\r\\n")
        headers = [line.split(": ", 1) for line in lines[1:] if line]
        length = int(dict((k.lower(), v) for k, v in headers).get("content-length", 0))
        while len(rest) < length:
            rest += sock.recv(65536)
        return [lines[0], headers, rest.decode()]

    def connect():
        return socket.create_connection(("127.0.0.1", PORT), timeout=10)

    def finish(results):
        print("RESULTS " + json.dumps(results), flush=True)
        print("SEEN " + json.dumps(seen), flush=True)
        loop[0].call_soon_threadsafe(app.stop)
''')


def run(script, mode="native"):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    source = APP.replace("PORT", str(port)) + textwrap.dedent(script)
    source += f'\napp.run(host="127.0.0.1", port={port}, single_process=True)\n'
    result = subprocess.run([sys.executable, "-c", source, mode], capture_output=True, text=True,
                            timeout=60, cwd=ROOT,
                            env={k: v for k, v in os.environ.items() if k != "OLDMAN_SERVER_NATIVE_SERVER"})
    assert result.returncode == 0, result.stderr[-3000:]
    lines = {line.split(" ", 1)[0]: line.split(" ", 1)[1]
             for line in result.stdout.splitlines() if " " in line}
    return json.loads(lines["RESULTS"]), json.loads(lines["SEEN"])


def header(result, name):
    return [value for key, value in result[1] if key.lower() == name]


PREFLIGHT = '''
    @app.post("/items")
    async def items(request):
        return text("created")

    @app.after_server_start
    def drive(app):
        def client():
            with connect() as sock:
                results = [exchange(sock, b"OPTIONS /items HTTP/1.1\\r\\nHost: x\\r\\n"
                                          b"Origin: https://a.example\\r\\n"
                                          b"Access-Control-Request-Method: POST\\r\\n"
                                          b"Access-Control-Request-Headers: x-csrf-token\\r\\n\\r\\n"),
                           exchange(sock, b"OPTIONS /items HTTP/1.1\\r\\nHost: x\\r\\n"
                                          b"Origin: https://evil.example\\r\\n"
                                          b"Access-Control-Request-Method: POST\\r\\n\\r\\n")]
            finish(results)
        threading.Thread(target=client).start()
'''


def test_the_host_answers_a_default_policy_preflight_itself():
    results, seen = run(PREFLIGHT)
    answered, refused = results
    assert answered == ["HTTP/1.1 204 No Content", [
        ["Access-Control-Allow-Origin", "https://a.example"],
        ["Vary", "Origin"],
        ["Access-Control-Allow-Methods", "GET, HEAD, POST, PUT, PATCH, DELETE"],
        ["Access-Control-Max-Age", "600"],
        ["Access-Control-Allow-Headers", "x-csrf-token"],
        ["Vary", "Access-Control-Request-Headers"],
        ["Connection", "keep-alive"]], ""]
    # A preflight from another origin is an ordinary OPTIONS request: Python
    # answers it (204 and Allow: HTTP_AUTO_OPTIONS), with the default policy's
    # Vary.
    assert refused[0] == "HTTP/1.1 204 No Content"
    assert header(refused, "allow") == ["OPTIONS, POST"]
    assert header(refused, "access-control-allow-origin") == []
    assert header(refused, "vary") == ["Origin"]
    assert seen == [["OPTIONS", "/items"]]


OVERLOAD = '''
    app.config.NATIVE_MAX_REQUESTS = 1

    @app.get("/block")
    def block(request):
        time.sleep(1.0)  # holds the event loop: new requests wait in the queue
        return text("blocked")

    @app.get("/x")
    async def x(request):
        return text("x")

    @app.after_server_start
    def drive(app):
        def client():
            get = b"GET %s HTTP/1.1\\r\\nHost: x\\r\\nOrigin: https://a.example\\r\\n\\r\\n"
            first, second = connect(), connect()
            first.sendall(get % b"/block")
            time.sleep(0.3)
            # Python has its one request (NATIVE_MAX_REQUESTS): the host refuses.
            results = [exchange(second, get % b"/x"), exchange(first, b"")]
            for sock in (first, second):
                sock.close()
            finish(results)
        threading.Thread(target=client).start()
'''


def test_the_overload_503_carries_the_default_policy():
    results, _ = run(OVERLOAD)
    refused, blocked = results
    assert refused[0] == "HTTP/1.1 503 Service Unavailable"
    assert header(refused, "access-control-allow-origin") == ["https://a.example"]
    assert header(refused, "vary") == ["Origin"]
    assert blocked[2] == "blocked"


PROTOCOL_ERROR = '''
    @app.get("/")
    async def index(request):
        return text("index")

    @app.after_server_start
    def drive(app):
        def client():
            with connect() as sock:
                results = [exchange(sock, b"GET / HTTP/1.1\\r\\nHost: x\\r\\n"
                                          b"Origin: https://a.example\\r\\nbad header\\r\\n\\r\\n")]
            finish(results)
        threading.Thread(target=client).start()
'''


def test_a_protocol_error_knows_no_origin():
    results, _ = run(PROTOCOL_ERROR)
    (bad,) = results
    assert bad[0] == "HTTP/1.1 400 Bad Request"
    assert header(bad, "access-control-allow-origin") == []
    assert header(bad, "vary") == ["Origin"]


def test_cors_checks_are_compiled_in_only_where_they_apply():
    """OptionalCors leaves the request path's CORS checks out of the methods
    TouchUp compiles unless they apply: none at all with CORS off, and the
    native fast path's own only with route policies."""
    from ast import parse, unparse
    from inspect import getsource
    from textwrap import dedent

    from oldman_server import CORS, OldmanServer
    from oldman_server.http.http1 import Http
    from oldman_server.native.host import NativeApplication
    from oldman_server.response import text
    from oldman_server.touchup.schemes.cors import OptionalCors

    def compiled(method, app):
        node = parse(dedent(getsource(method)))
        for visitor in OptionalCors(app).visitors():
            node = visitor.visit(node)
        return unparse(node)

    methods = (NativeApplication._call_flat, OldmanServer.handle_request, Http.http1_response_header)
    off = OldmanServer("cors_compiled_off", configure_logging=False)
    default_only = OldmanServer("cors_compiled_default", configure_logging=False)
    default_only.config.CORS_ORIGINS = "https://a.example"
    routes = OldmanServer("cors_compiled_routes", configure_logging=False)
    routes.config.CORS_ORIGINS = "https://a.example"
    for app in (off, default_only, routes):
        @app.get("/")
        async def index(request):
            return text("x")

    @routes.get("/strict", cors=CORS(credentials=True))
    async def strict(request):
        return text("strict")

    try:
        off.finalize()
        # Only the off application in the process: no CORS code anywhere.
        registry = OldmanServer._app_registry
        OldmanServer._app_registry = {off.name: off}
        try:
            assert not any("_cors" in compiled(method, off) or "_Answered" in compiled(method, off)
                           for method in methods)
        finally:
            OldmanServer._app_registry = registry
        default_only.finalize()
        routes.finalize()
        flat = compiled(NativeApplication._call_flat, default_only)
        assert "_cors" not in flat and "_Answered" not in flat
        assert "_cors_table" in compiled(OldmanServer.handle_request, default_only)
        assert "_cors_table" in compiled(Http.http1_response_header, default_only)
        flat = compiled(NativeApplication._call_flat, routes)
        assert flat.count("self._cors_codes") == 2 and "except _Answered" in flat
    finally:
        for app in (off, default_only, routes):
            OldmanServer.unregister_app(app)
