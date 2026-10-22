"""Native multipart parsing (native/multipart.hh) against Sanic's parser.

Whenever the native parser answers, the answer must equal Sanic's; when Sanic's
parser raises or logs, the native one must decline (None) so Sanic's runs.
"""
import asyncio
import logging
import random

import pytest
from oldman_server import OldmanServer
from oldman_server.request import form
from oldman_server.response import json
import uvloop

from oldman_server.native import _native as _seastar_probe
from test_http import config
from test_sanic_http1 import read_response


def python_parse(body, boundary):
    """Sanic's own parser: its result, or the exception it raises, and whether it logged."""
    saved, form.native_multipart = form.native_multipart, None
    records = []
    handler = logging.Handler(logging.DEBUG)
    handler.emit = records.append
    logger = logging.getLogger("oldman_server.root")
    level = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    try:
        fields, files = form.parse_multipart_form(body, boundary)
        return (dict(fields), dict(files)), None, bool(records)
    except Exception as error:
        return None, type(error), bool(records)
    finally:
        logger.removeHandler(handler)
        logger.setLevel(level)
        form.native_multipart = saved


def check(body, boundary):
    """Compares both parsers: 0 declined, 1 parsed natively, 2 parsed with content."""
    native = _seastar_probe.parse_multipart(body, boundary, form.File)
    expected, error, logged = python_parse(body, boundary)
    if native is None:
        return 0
    assert error is None and not logged, (body, boundary)
    assert native == expected, (body, boundary)
    return 2 if native[0] or native[1] else 1


DISPOSITIONS = [
    'form-data; name="{n}"', "form-data; name={n}", 'form-data;name="{n}"', 'form-data;  name = "{n}"',
    'form-data; name="{n}"; filename="{f}"', "form-data; name={n}; filename={f}",
    'form-data; filename="{f}"; name="{n}"', 'FORM-DATA; NAME="{n}"', 'form-data; name=""',
    "form-data", 'form-data; name="{n}"; filename=""', "form-data; name=\"{n}\"; filename*=UTF-8''%E2%82%AC.txt",
    "form-data; name=\"{n}\"; filename*=", 'form-data; name="{n}"; name="other"', 'form-data; name="unterminated',
    'form-data; name="a%22b%0D%0Ac"', "form-data; ; name={n}; =x; name2", 'form-data; name="{n}"\x1c',
    '\x1c form-data \x1f; name="{n}"', 'form-data; name="{n}"; filename="résumé.pdf"',
    'form-data; name="{n}"; filename="é.txt"',
]
TYPES = ["text/plain", "text/plain; charset=utf-8", "text/plain; charset=UTF-8", "text/plain; charset=utf8",
         "text/plain; charset=latin-1", 'text/plain; charset=""', "application/octet-stream",
         " Application/JSON ; charset=utf_8", "image/png; foo=bar"]


def random_part(rng, realistic=False):
    name = rng.choice(["a", "field", "x-y", "dup", "dup", "na%22me", "näme"])
    filename = rng.choice(["f.txt", "my file.bin", "a,b.txt", "q\"x"])
    lines = []
    if realistic:
        # Mostly what clients send: a named disposition, sometimes a type.
        name = rng.choice(["a", "field", "x-y", "dup", "dup", "na%22me"])
        lines.append("Content-Disposition: " + rng.choice(DISPOSITIONS[:7]).format(n=name, f=filename))
    for _ in range(rng.choice([0, 0, 1] if realistic else [0, 1, 1, 1, 2, 2, 3])):
        kind = rng.random()
        if kind < 0.55:
            header = rng.choice(["Content-Disposition", "content-disposition", "CONTENT-DISPOSITION"])
            value = rng.choice(DISPOSITIONS).format(n=name, f=filename)
        elif kind < 0.85:
            header, value = rng.choice(["Content-Type", "content-type"]), rng.choice(TYPES)
        elif kind < 0.9:
            header, value = "X-Other", "whatever; name=ignored"
        elif kind < 0.95:
            lines.append("no colon here")
            continue
        else:
            header, value = "Content-Disposition", 'form-data; name="z"'
        separator = rng.choice([": ", ": ", ":", ":  "])
        lines.append(f"{header}{separator}{value}")
    data_kind = rng.random() * (0.6 if realistic else 1)
    if data_kind < 0.4:
        data = rng.choice(["value", "", "with\r\nnewline", "x" * 50, "ünicode €", "--almost"]).encode()
    elif data_kind < 0.6:
        data = bytes(rng.randrange(256) for _ in range(rng.randrange(0, 40)))
    else:
        data = rng.choice([b"\xff\xfe invalid", b"ok", b"\r\n", b"", b"tail--"])
    head = "".join(line + "\r\n" for line in lines).encode("utf-8")
    if not realistic and rng.random() < 0.05:
        return head[:-1]  # a part cut short
    return head + b"\r\n" + data


def random_body(rng, realistic=False):
    boundary = rng.choice([b"----WebKitFormBoundary7MA4", b"b", b"--", b"xyz123"])
    parts = [random_part(rng, realistic) for _ in range(rng.randrange(0, 5))]
    preamble = rng.choice([b"", b"preamble\r\n", b"\r\n"])
    body = preamble + b"".join(b"--" + boundary + b"\r\n" + part + b"\r\n" for part in parts)
    body += rng.choice([b"--" + boundary + b"--\r\n", b"--" + boundary + b"--", b"", b"--" + boundary])
    return body, boundary


def test_native_parser_matches_sanic_on_random_forms():
    rng = random.Random(20261021)
    for realistic, least in ((False, 150), (True, 800)):
        outcomes = [check(*random_body(rng, realistic)) for _ in range(6000)]
        # A real share of the forms is parsed natively and compared with content.
        assert outcomes.count(2) > least


@pytest.mark.parametrize("body,boundary,parsed", [
    (b"--b\r\nContent-Disposition: form-data; name=\"a\"\r\n\r\nvalue\r\n--b--\r\n", b"b", True),
    (b"--b\r\nContent-Disposition: form-data; name=\"f\"; filename=\"x.png\"\r\n"
     b"Content-Type: image/png\r\n\r\n\x89PNG\r\n--b--\r\n", b"b", True),
    (b"--b\r\nContent-Disposition: form-data; name=\"a\"\r\n\r\n\xff\r\n--b--\r\n", b"b", False),
    (b"--b\r\nContent-Disposition: form-data; name=\"a\"\r\nContent-Type: text/plain; charset=latin-1"
     b"\r\n\r\n\xe9\r\n--b--\r\n", b"b", False),
    (b"--b\r\nContent-Disposition: form-data; name=\"f\"; filename*=UTF-8''%E2%82%AC\r\n\r\nx\r\n--b--",
     b"b", False),
    (b"--b\r\nno colon\r\n\r\nx\r\n--b--", b"b", False),
    (b"anything", b"", False),
])
def test_known_forms(body, boundary, parsed):
    assert bool(check(body, boundary)) is parsed


def test_request_form_on_the_native_host():
    app = OldmanServer("multipart_native", configure_logging=False)
    app.config.ACCESS_LOG = False

    @app.post("/upload")
    async def upload(request):
        upload = request.files.get("upload")
        return json({"fields": request.form, "file": [upload.type, upload.name, len(upload.body)]})

    boundary = "----omtest"
    payload = bytes(range(256)) * 400
    body = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"a\"\r\n\r\n1\r\n"
            f"--{boundary}\r\nContent-Disposition: form-data; name=\"a\"\r\n\r\n2\r\n"
            f"--{boundary}\r\nContent-Disposition: form-data; name=\"upload\"; filename=\"p.bin\"\r\n"
            f"Content-Type: application/octet-stream\r\n\r\n").encode() + payload + \
           f"\r\n--{boundary}--\r\n".encode()

    async def main():
        from oldman_server.native.host import serve
        cfg = config(application_dispatch_all=True)
        async with serve(app, cfg):
            reader, writer = await asyncio.open_connection("127.0.0.1", cfg.server.http_port)
            writer.write(f"POST /upload HTTP/1.1\r\nHost: x\r\nContent-Length: {len(body)}\r\n"
                         f"Content-Type: multipart/form-data; boundary={boundary}\r\n\r\n".encode() + body)
            response = await read_response(reader)
            writer.close()
            return response
    status, _, data = uvloop.run(main())
    assert status == 200
    import json as json_module
    assert json_module.loads(data) == {"fields": {"a": ["1", "2"]},
                                       "file": ["application/octet-stream", "p.bin", len(payload)]}
    assert form.native_multipart is _seastar_probe.parse_multipart
