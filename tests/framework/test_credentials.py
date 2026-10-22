"""Authorization headers: the scheme is the header's first word, matched
case-insensitively (RFC 9110 11.1), never a substring of the credentials."""
from base64 import b64encode

import pytest

from oldman_server.headers import parse_credentials
from oldman_server.models.http_types import Credentials
from oldman_server.response import text


def basic(value: str) -> str:
    return b64encode(value.encode()).decode()


@pytest.mark.parametrize(
    "header,prefixes,expected",
    (
        ("Bearer abc", None, ("Bearer", "abc")),
        ("bearer abc", None, ("Bearer", "abc")),
        ("BEARER   abc ", None, ("Bearer", "abc")),
        ("Token abc", ("Bearer", "Token"), ("Token", "abc")),
        # A scheme that is not asked for is not a token.
        ("Basic abc", ("Bearer", "Token"), (None, None)),
        ("Digest abc", None, (None, None)),
        # "Bearer" inside Basic credentials is not the scheme.
        (f"Basic {basic('Bearer:x')}", None, ("Basic", basic("Bearer:x"))),
        (f"Basic {basic('Bearer:x')}", ("Bearer",), (None, None)),
        # A bare token (no scheme) is the token, as before.
        ("abc", ("Bearer", "Token"), (None, "abc")),
        # A scheme without credentials has no token (it is not one itself).
        ("Bearer", None, ("Bearer", None)),
        ("bearer  ", ("Bearer",), ("Bearer", None)),
        # Whitespace other than a space separates too.
        ("Bearer\tabc", None, ("Bearer", "abc")),
        (None, None, (None, None)),
    ),
)
def test_parse_credentials(header, prefixes, expected):
    assert parse_credentials(header, prefixes) == expected


def test_basic_password_may_contain_colons():
    credentials = Credentials("Basic", basic("user:pa:ss"))
    assert (credentials.username, credentials.password) == ("user", "pa:ss")


@pytest.mark.parametrize("token", ("not base64!", basic("no colon"), b64encode(b"\xff:x").decode()))
def test_malformed_basic_credentials_are_rejected(token):
    with pytest.raises(ValueError):
        Credentials("Basic", token)


@pytest.mark.parametrize(
    "header,token,credentials",
    (
        ("bearer abc", "abc", ("Bearer", "abc")),
        (f"Basic {basic('u:p:w')}", None, ("Basic", basic("u:p:w"))),
        ("Basic %%%", None, None),
        ("Digest abc", None, None),
    ),
)
def test_request_token_and_credentials(app, header, token, credentials):
    @app.get("/")
    async def handler(request):
        return text("ok")

    request, _ = app.test_client.get("/", headers={"Authorization": header})
    assert request.token == token
    if credentials is None:
        assert request.credentials is None
    else:
        assert (request.credentials.auth_type, request.credentials.token) == credentials
