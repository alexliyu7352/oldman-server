"""TRUSTED_PROXIES: forwarded headers count only on connections from these
addresses, networks, or (with "unix") a unix socket. Unset, Sanic's rules
apply to every connection."""
import os

from pathlib import Path

import httpx
import pytest

from oldman_server.headers import trusted_networks, trusted_peer
from oldman_server.models.server_types import ConnInfo
from oldman_server.response import json


FORWARDED = {"X-Forwarded-For": "203.0.113.9", "X-Forwarded-Proto": "https"}


def proxied_app(app, trusted):
    app.config.PROXIES_COUNT = 1
    app.config.TRUSTED_PROXIES = trusted

    @app.get("/")
    async def index(request):
        return json({"remote_addr": request.remote_addr,
                     "client_ip": request.client_ip, "scheme": request.scheme})

    return app


def test_unset_trusts_every_connection(app):
    proxied_app(app, None)
    _, response = app.test_client.get("/", headers=FORWARDED)
    assert response.json == {"remote_addr": "203.0.113.9",
                             "client_ip": "203.0.113.9", "scheme": "https"}


@pytest.mark.parametrize("trusted", ["127.0.0.1", ["10.0.0.0/8", "127.0.0.0/8"],
                                     "10.0.0.0/8, 127.0.0.0/8", ("::1", "127.0.0.1")])
def test_a_trusted_peer_forwards(app, trusted):
    proxied_app(app, trusted)
    _, response = app.test_client.get("/", headers=FORWARDED)
    assert response.json["remote_addr"] == "203.0.113.9"
    assert response.json["scheme"] == "https"


@pytest.mark.parametrize("trusted", ["10.0.0.0/8", [], "unix"])
def test_an_untrusted_peer_is_the_client(app, trusted):
    proxied_app(app, trusted)
    _, response = app.test_client.get("/", headers=FORWARDED)
    assert response.json == {"remote_addr": "", "client_ip": "127.0.0.1",
                             "scheme": "http"}


def test_forwarded_with_a_secret_is_also_gated(app):
    app.config.FORWARDED_SECRET = "s3cret"
    app.config.TRUSTED_PROXIES = "10.0.0.0/8"

    @app.get("/")
    async def index(request):
        return json(request.remote_addr)

    headers = {"Forwarded": "for=203.0.113.9;secret=s3cret"}
    _, response = app.test_client.get("/", headers=headers)
    assert response.json == ""
    app.config.TRUSTED_PROXIES = "127.0.0.1"
    _, response = app.test_client.get("/", headers=headers)
    assert response.json == "203.0.113.9"


@pytest.mark.parametrize("client,trusted,expected", [
    (("::ffff:127.0.0.1", 5000), "127.0.0.0/8", True),  # dual-stack listener
    (("::1", 5000), "::1", True),
    (("192.0.2.1", 5000), "127.0.0.1", False),
    ("", "unix", True),
    ("", "127.0.0.1", False),
])
def test_trusted_peer(client, trusted, expected):
    server = "/tmp/app.sock" if client == "" else ("::", 8000)
    conn_info = ConnInfo._from_addresses(client, server)
    assert trusted_peer(conn_info, trusted) is expected


def test_a_malformed_entry_is_rejected():
    with pytest.raises(ValueError):
        trusted_networks("10.0.0.0/33")
    with pytest.raises(ValueError):
        trusted_networks(("proxy.example",))


def test_a_malformed_entry_fails_startup(app):
    app.config.TRUSTED_PROXIES = "not-an-address"

    @app.get("/")
    async def index(request):
        return json("unreachable")

    with pytest.raises(ValueError):
        app.test_client.get("/")


SOCKPATH = Path("/tmp/oldman-server-trusted.sock")


@pytest.mark.parametrize("trusted,expected", [("unix", "203.0.113.9"),
                                              ("127.0.0.1", "")])
def test_unix_socket_connections(app, trusted, expected):
    proxied_app(app, trusted)
    results = []

    @app.after_server_start
    async def fetch(app):
        try:
            transport = httpx.AsyncHTTPTransport(uds=str(SOCKPATH))
            async with httpx.AsyncClient(transport=transport) as client:
                response = await client.get("http://proxy.invalid/", headers=FORWARDED)
                results.append(response.json())
        finally:
            app.stop()

    try:
        app.run(unix=str(SOCKPATH), single_process=True, access_log=False)
    finally:
        if os.path.exists(SOCKPATH):
            os.unlink(SOCKPATH)
    assert results[0]["remote_addr"] == expected


def test_the_request_signal_sees_the_forwarded_client(app):
    # http.lifecycle.request runs as soon as the request exists: the trust
    # decision needs the connection then (it was read as None, and the empty
    # result kept for the whole request).
    proxied_app(app, "127.0.0.1")
    seen = []

    @app.signal("http.lifecycle.request")
    async def early(request):
        seen.append([request.client_ip, request.scheme])

    _, response = app.test_client.get("/", headers=FORWARDED)
    assert seen == [["203.0.113.9", "https"]]
    assert response.json == {"remote_addr": "203.0.113.9",
                             "client_ip": "203.0.113.9", "scheme": "https"}


def test_a_lone_number_from_the_environment_is_read_as_text(monkeypatch):
    # The environment loader reads "127" as the int 127: a malformed entry,
    # reported as such (ValueError), not a TypeError about an int.
    from oldman_server.config import Config
    from oldman_server.headers import trusted_sources
    monkeypatch.setenv("OLDMAN_SERVER_TRUSTED_PROXIES", "127")
    trusted = Config().TRUSTED_PROXIES
    assert trusted == 127 and trusted_sources(trusted) == "127"
    with pytest.raises(ValueError, match="'127' does not appear"):
        trusted_networks(trusted_sources(trusted))
    assert trusted_networks(trusted_sources(["unix", "10.0.0.0/8"]))[0] is True
