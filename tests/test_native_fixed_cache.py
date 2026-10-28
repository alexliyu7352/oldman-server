"""app.add_fixed_response on both servers, and native_cache on the native host.

Each scenario runs in its own interpreter: a process starts one native runtime.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import textwrap

import pytest

ROOT = Path(__file__).resolve().parents[1]

APP = textwrap.dedent('''
    import asyncio, json, sys
    from oldman_server import OldmanServer
    from oldman_server.native import native_cache
    from oldman_server.response import text

    native = sys.argv[1] == "native"
    app = OldmanServer("fixed_cache", configure_logging=False)
    app.config.NATIVE_SERVER = native
    app.config.NATIVE_SHARDS = 1
    app.config.ACCESS_LOG = False
    seen = []

    @app.on_request
    def middleware(request):
        seen.append(("middleware", request.method, request.path))

    @app.signal("http.lifecycle.handle")
    async def handle(request):
        seen.append(("signal", request.method, request.path))

    async def exchange(port, raw):
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(raw)
        head = (await reader.readuntil(b"\\r\\n\\r\\n")).decode()
        lines = head.split("\\r\\n")
        headers = sorted(tuple(line.lower().split(": ", 1)) for line in lines[1:] if line)
        length = int(dict(headers).get("content-length", 0))
        body = b"" if raw.startswith(b"HEAD") else await reader.readexactly(length)
        writer.close()
        return [lines[0], headers, body.decode()]

    def drive(port, requests):
        @app.after_server_start
        async def run(app):
            results = []
            for raw in requests:
                results.append(await exchange(port, raw))
                await asyncio.sleep(float(raw.split(b"X-Wait: ", 1)[1].split(b"\\r\\n", 1)[0])
                                    if b"X-Wait: " in raw else 0)
            print("RESULTS " + json.dumps(results), flush=True)
            print("SEEN " + json.dumps(seen), flush=True)
            app.stop()
''')


def run(mode, script):
    import socket
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    source = APP + textwrap.dedent(script).replace("PORT", str(port))
    source += f'\napp.run(host="127.0.0.1", port={port}, single_process=True)\n'
    result = subprocess.run([sys.executable, "-c", source, mode], capture_output=True, text=True,
                            timeout=60, cwd=ROOT,
                            env={k: v for k, v in os.environ.items() if k != "OLDMAN_SERVER_NATIVE_SERVER"})
    assert result.returncode == 0, result.stderr[-3000:]
    lines = {line.split(" ", 1)[0]: line.split(" ", 1)[1]
             for line in result.stdout.splitlines() if " " in line}
    return json.loads(lines["RESULTS"]), json.loads(lines["SEEN"])


FIXED = '''
    app.add_fixed_response("/health", "ok", headers={"X-Fixed": "1"})
    app.add_fixed_response("/gone", b"", status=410, content_type="application/json")

    @app.route("/health", methods=["GET", "POST"])
    async def health(request):
        return text("routed " + request.method)

    drive(PORT, [
        b"GET /health HTTP/1.1\\r\\nHost: x\\r\\n\\r\\n",
        b"GET /health?probe=1 HTTP/1.1\\r\\nHost: x\\r\\n\\r\\n",
        b"HEAD /health HTTP/1.1\\r\\nHost: x\\r\\n\\r\\n",
        b"GET /gone HTTP/1.1\\r\\nHost: x\\r\\n\\r\\n",
        b"POST /health HTTP/1.1\\r\\nHost: x\\r\\nContent-Length: 0\\r\\n\\r\\n",
        b"GET /health HTTP/1.1\\r\\nHost: x\\r\\nContent-Length: 2\\r\\n\\r\\nhi",
        b"GET /healthz HTTP/1.1\\r\\nHost: x\\r\\n\\r\\n",
    ])
'''


@pytest.mark.parametrize("mode", ["asyncio", "native"])
def test_fixed_responses_skip_everything_on_both_servers(mode):
    results, seen = run(mode, FIXED)
    fixed = ["HTTP/1.1 200 OK", [["connection", "keep-alive"], ["content-length", "2"],
                                 ["content-type", "text/plain; charset=utf-8"], ["x-fixed", "1"]], "ok"]
    assert results[0] == fixed and results[1] == fixed
    assert results[2] == [fixed[0], fixed[1], ""]  # HEAD: the GET's headers, no body
    assert results[3] == ["HTTP/1.1 410 Gone", [["connection", "keep-alive"], ["content-length", "0"],
                                                ["content-type", "application/json"]], ""]
    # Other methods, a request with a body and another path are routed.
    assert [result[2] for result in results[4:6]] == ["routed POST", "routed GET"]
    assert results[6][0] == "HTTP/1.1 404 Not Found"
    # Middleware and signals run for every request that reaches the application
    # (the error handler runs request middleware for the 404 too).
    assert [entry[1:] for entry in seen if entry[0] == "middleware"] == [
        ["POST", "/health"], ["GET", "/health"], ["GET", "/healthz"]]
    assert [entry[1:] for entry in seen if entry[0] == "signal"] == [
        ["POST", "/health"], ["GET", "/health"], ["GET", "/healthz"]]


def test_fixed_responses_are_the_same_on_both_servers():
    assert run("asyncio", FIXED) == run("native", FIXED)


def test_fixed_response_arguments_are_checked():
    from oldman_server import OldmanServer
    app = OldmanServer("fixed_arguments", configure_logging=False)
    for path in ("health", "/health?x=1"):
        with pytest.raises(ValueError):
            app.add_fixed_response(path, "ok")
    with pytest.raises(ValueError):
        app.add_fixed_response("/health", "ok", status=101)
    with pytest.raises(ValueError):
        app.add_fixed_response("/health", "ok", headers={"X-A": "a\r\nb"})


CACHED = '''
    calls = {"news": 0, "cookie": 0}

    @app.get("/news")
    @native_cache(0.3)
    async def news(request):
        calls["news"] += 1
        return text(f"news {calls['news']} {request.args.get('page', '-')}")

    @app.get("/cookie")
    @native_cache(1)
    async def cookie(request):
        calls["cookie"] += 1
        response = text(f"cookie {calls['cookie']}")
        response.add_cookie("k", "v")
        return response

    drive(PORT, [
        b"GET /news HTTP/1.1\\r\\nHost: x\\r\\n\\r\\n",
        b"GET /news HTTP/1.1\\r\\nHost: x\\r\\n\\r\\n",
        b"GET /news?page=2 HTTP/1.1\\r\\nHost: x\\r\\n\\r\\n",
        b"GET /news HTTP/1.1\\r\\nHost: x\\r\\nCookie: a=b\\r\\n\\r\\n",
        b"HEAD /news HTTP/1.1\\r\\nHost: x\\r\\nX-Wait: 0.4\\r\\n\\r\\n",
        b"GET /news HTTP/1.1\\r\\nHost: x\\r\\n\\r\\n",
        b"GET /cookie HTTP/1.1\\r\\nHost: x\\r\\n\\r\\n",
        b"GET /cookie HTTP/1.1\\r\\nHost: x\\r\\n\\r\\n",
    ])
'''


def test_native_cache_answers_repeats_until_it_expires():
    results, seen = run("native", CACHED)
    bodies = [result[2] for result in results]
    # The repeat is answered from the cache (no middleware); another query, a
    # request with Cookie, one after expiry reach the handler; a HEAD reaches
    # it too (answered by the GET route, HTTP_AUTO_HEAD), the cache answering
    # and keeping GET only; a response with Set-Cookie is never kept.
    assert bodies == ["news 1 -", "news 1 -", "news 2 2", "news 3 -", "", "news 5 -",
                      "cookie 1", "cookie 2"]
    assert results[4][0] == "HTTP/1.1 200 OK"
    assert results[1][1] == results[0][1]
    assert len([entry for entry in seen if entry[0] == "middleware"]) == 7


def test_native_cache_is_ignored_by_the_asyncio_server():
    results, _ = run("asyncio", CACHED)
    assert [result[2] for result in results][:2] == ["news 1 -", "news 2 -"]


def test_native_cache_arguments_are_checked():
    from oldman_server.native import native_cache
    for seconds in (0, -1, 3601, True, "1"):
        with pytest.raises(ValueError):
            native_cache(seconds)
