"""CORS (oldman_server.cors): settings for the default, cors= on routes,
blueprints, groups, static routes and fixed responses, collected at startup
and applied by the server to every response, without middleware. The same
tests run on the asyncio server and the native host (three modes)."""

from pathlib import Path

import pytest

from oldman_server import Blueprint, OldmanServer
from oldman_server.cors import CORS
from oldman_server.exceptions import Unauthorized
from oldman_server.native import native_cache, native_server
from oldman_server.response import text
from oldman_server.testing.reusable import ReusableClient


A = "https://a.example"
B = "https://b.example"
PREFLIGHT = {"Access-Control-Request-Method": "POST"}


def origin(value, **more):
    return {"Origin": value, **more}


def allow_origin(response):
    return response.headers.get("access-control-allow-origin")


def vary(response):
    return [v.strip() for v in response.headers.get_list("vary") for v in v.split(",")]


@pytest.fixture
def cors_app(app: OldmanServer):
    app.config.CORS_ORIGINS = f"{A}, https://*.c.example"
    calls = []

    @app.on_request
    async def record(request):
        calls.append(("middleware", request.method, request.path))

    @app.get("/plain")
    async def plain(request):
        calls.append(("handler", request.path))
        return text("plain")

    @app.post("/plain")
    async def plain_post(request):
        calls.append(("handler", request.path))
        return text("posted")

    @app.get("/auth")
    async def auth(request):
        raise Unauthorized("sign in")

    @app.get("/varies")
    async def varies(request):
        return text("varies", headers={"Vary": "Accept-Encoding"})

    app.ctx.calls = calls
    return app


def test_off_by_default_adds_nothing(app: OldmanServer):
    @app.get("/")
    async def index(request):
        return text("x")

    _, response = app.test_client.get("/", headers=origin(A))
    assert allow_origin(response) is None
    assert "vary" not in response.headers
    assert app._cors_table is None


def test_settings_apply_to_routes_errors_and_unknown_paths(cors_app):
    for path, status in (("/plain", 200), ("/auth", 401), ("/missing", 404)):
        _, response = cors_app.test_client.get(path, headers=origin(A))
        assert response.status == status
        assert allow_origin(response) == A
        assert "Origin" in vary(response)


def test_a_disallowed_origin_gets_no_allow_origin(cors_app):
    _, response = cors_app.test_client.get("/plain", headers=origin("https://evil.example"))
    assert response.status == 200
    assert allow_origin(response) is None
    assert "Origin" in vary(response)


def test_an_echoing_policy_varies_by_origin_without_one(cors_app):
    _, response = cors_app.test_client.get("/plain")
    assert allow_origin(response) is None
    assert vary(response) == ["Origin"]


def test_vary_joins_the_response_s_own(cors_app):
    _, response = cors_app.test_client.get("/varies", headers=origin(A))
    assert allow_origin(response) == A
    assert set(vary(response)) == {"Accept-Encoding", "Origin"}


def test_a_subdomain_wildcard(cors_app):
    _, response = cors_app.test_client.get("/plain", headers=origin("https://api.c.example"))
    assert allow_origin(response) == "https://api.c.example"
    for other in ("https://c.example", "http://api.c.example", "https://api.c.example.evil"):
        _, response = cors_app.test_client.get("/plain", headers=origin(other))
        assert allow_origin(response) is None, other


def test_any_origin_without_credentials(app: OldmanServer):
    app.config.CORS_ORIGINS = "*"
    app.config.CORS_ALLOW_HEADERS = "*"

    @app.get("/")
    async def index(request):
        return text("x")

    _, response = app.test_client.get("/")
    assert allow_origin(response) == "*"
    assert "vary" not in response.headers
    _, response = app.test_client.options("/", headers=origin(A, **PREFLIGHT))
    assert response.status == 204
    assert allow_origin(response) == "*"
    assert response.headers["access-control-allow-headers"] == "*"


def test_credentials(app: OldmanServer):
    app.config.CORS_ORIGINS = A
    app.config.CORS_ALLOW_CREDENTIALS = True
    app.config.CORS_EXPOSE_HEADERS = "X-Total, X-Page"

    @app.get("/")
    async def index(request):
        return text("x")

    _, response = app.test_client.get("/", headers=origin(A))
    assert allow_origin(response) == A
    assert response.headers["access-control-allow-credentials"] == "true"
    assert response.headers["access-control-expose-headers"] == "X-Total, X-Page"


@pytest.mark.parametrize("allow_headers", [None, "*"])
def test_a_credentialed_preflight_never_answers_with_a_star(app: OldmanServer, allow_headers):
    app.config.CORS_ORIGINS = A
    app.config.CORS_ALLOW_CREDENTIALS = True
    app.config.CORS_ALLOW_HEADERS = allow_headers

    @app.post("/")
    async def index(request):
        return text("x")

    _, response = app.test_client.options(
        "/", headers=origin(A, **PREFLIGHT, **{"Access-Control-Request-Headers": "x-csrf-token"}))
    assert response.status == 204
    assert allow_origin(response) == A
    assert response.headers["access-control-allow-headers"] == "x-csrf-token"
    assert response.headers["access-control-allow-methods"] == "GET, HEAD, POST, PUT, PATCH, DELETE"
    assert response.headers["access-control-allow-credentials"] == "true"
    assert not any("*" in value for name, value in response.headers.items()
                   if name.lower().startswith("access-control-"))


@pytest.mark.parametrize("settings", [
    {"CORS_ORIGINS": "*", "CORS_ALLOW_CREDENTIALS": True},
    {"CORS_ORIGINS": A, "CORS_ALLOW_CREDENTIALS": True, "CORS_EXPOSE_HEADERS": "*"},
    {"CORS_ORIGINS": A, "CORS_ALLOW_CREDENTIALS": True, "CORS_ALLOW_METHODS": "*"},
    {"CORS_ORIGINS": "a.example"},
    {"CORS_ORIGINS": "https://a.example/path"},
])
def test_invalid_settings_fail_at_startup(app: OldmanServer, settings):
    app.config.update(settings)

    @app.get("/")
    async def index(request):
        return text("x")

    with pytest.raises(ValueError):
        app.finalize()


def test_a_route_policy_needs_origins(app: OldmanServer):
    @app.get("/", cors=CORS(credentials=True))
    async def index(request):
        return text("x")

    with pytest.raises(ValueError, match="no origins"):
        app.finalize()


def test_cors_option_types_are_checked(app: OldmanServer):
    with pytest.raises(TypeError):
        app.get("/", cors="yes")
    with pytest.raises(TypeError, match="WebSocket"):
        app.websocket("/ws", cors=CORS(A))
    with pytest.raises(TypeError):
        app.add_fixed_response("/health", "ok", cors="yes")


def test_route_policies(app: OldmanServer):
    app.config.CORS_ORIGINS = A

    @app.get("/off", cors=False)
    async def off(request):
        return text("off")

    @app.get("/credentialed", cors=CORS(credentials=True))
    async def credentialed(request):
        return text("credentialed")

    @app.get("/other", cors=CORS(B))
    async def other(request):
        return text("other")

    _, response = app.test_client.get("/off", headers=origin(A))
    assert allow_origin(response) is None
    assert "vary" not in response.headers
    _, response = app.test_client.get("/credentialed", headers=origin(A))
    assert allow_origin(response) == A  # origins from the settings
    assert response.headers["access-control-allow-credentials"] == "true"
    _, response = app.test_client.get("/other", headers=origin(A))
    assert allow_origin(response) is None
    _, response = app.test_client.get("/other", headers=origin(B))
    assert allow_origin(response) == B


def test_a_route_policy_without_settings(app: OldmanServer):
    @app.get("/open", cors=CORS("*"))
    async def opened(request):
        return text("open")

    @app.get("/closed")
    async def closed(request):
        return text("closed")

    _, response = app.test_client.get("/open", headers=origin(A))
    assert allow_origin(response) == "*"
    _, response = app.test_client.get("/closed", headers=origin(A))
    assert allow_origin(response) is None
    assert "vary" not in response.headers


def test_blueprint_group_and_route_precedence(app: OldmanServer):
    app.config.CORS_ORIGINS = A
    inherits = Blueprint("inherits", url_prefix="/inherits")
    own = Blueprint("own", url_prefix="/own", cors=CORS(B))
    group = Blueprint.group(inherits, own, url_prefix="/g", cors=False)
    direct = Blueprint("direct", url_prefix="/direct")

    @inherits.get("/")
    async def from_group(request):
        return text("group")

    @inherits.get("/route", cors=CORS(credentials=True))
    async def route_wins(request):
        return text("route")

    @own.get("/")
    async def from_blueprint(request):
        return text("blueprint")

    @direct.get("/")
    async def from_registration(request):
        return text("registration")

    app.blueprint(group)
    app.blueprint(direct, cors=CORS(B))

    def allowed(path, who):
        _, response = app.test_client.get(path, headers=origin(who))
        return allow_origin(response)

    assert allowed("/g/inherits/", A) is None  # the group's False
    assert allowed("/g/inherits/route", A) == A  # the route's own, origins from the settings
    assert allowed("/g/own/", B) == B  # the blueprint's own over the group's
    assert allowed("/direct/", B) == B  # the registration's
    assert allowed("/direct/", A) is None


def test_a_static_route_and_a_fixed_response(app: OldmanServer, tmp_path: Path):
    app.config.CORS_ORIGINS = A
    (tmp_path / "a.txt").write_text("file")
    app.static("/files", tmp_path, cors=CORS(B), name="files")
    app.add_fixed_response("/health", "ok")
    app.add_fixed_response("/private", "no", cors=False)

    _, response = app.test_client.get("/files/a.txt", headers=origin(B))
    assert response.text == "file"
    assert allow_origin(response) == B
    _, response = app.test_client.get("/health", headers=origin(A))
    assert response.text == "ok"
    assert allow_origin(response) == A
    _, response = app.test_client.get("/private", headers=origin(A))
    assert allow_origin(response) is None


def test_preflight_runs_no_middleware_or_handler(cors_app):
    _, response = cors_app.test_client.options(
        "/plain", headers=origin(A, **PREFLIGHT, **{"Access-Control-Request-Headers": "content-type"}))
    assert response.status == 204
    assert allow_origin(response) == A
    assert response.headers["access-control-allow-methods"] == "GET, HEAD, POST, PUT, PATCH, DELETE"
    assert response.headers["access-control-allow-headers"] == "content-type"
    assert response.headers["access-control-max-age"] == "600"
    assert cors_app.ctx.calls == []


def test_preflight_through_python_with_route_policies(cors_app):
    @cors_app.put("/strict", cors=CORS(methods=["PUT"], max_age=0))
    async def strict(request):
        return text("strict")

    _, response = cors_app.test_client.options(
        "/strict", headers=origin(A, **{"Access-Control-Request-Method": "PUT"}))
    assert response.status == 204
    assert allow_origin(response) == A
    assert response.headers["access-control-allow-methods"] == "PUT"
    assert "access-control-max-age" not in response.headers
    assert cors_app.ctx.calls == []


def test_a_disallowed_preflight_is_an_ordinary_options_request(cors_app):
    @cors_app.options("/plain")
    async def options(request):
        return text("own options")

    _, response = cors_app.test_client.options(
        "/plain", headers=origin("https://evil.example", **PREFLIGHT))
    assert response.text == "own options"
    assert allow_origin(response) is None
    assert ("middleware", "OPTIONS", "/plain") in cors_app.ctx.calls


def test_a_rewritten_path_is_matched(cors_app):
    @cors_app.rewrite_path
    def strip_language(request):
        if request.path.startswith("/zh/"):
            return request.path[3:]
        return None

    _, response = cors_app.test_client.options("/zh/plain", headers=origin(A, **PREFLIGHT))
    assert response.status == 204
    assert allow_origin(response) == A


def test_a_full_path_request_class(app: OldmanServer):
    from oldman_server import Request

    class Custom(Request):
        pass

    app = OldmanServer(app.name + "-custom", request_class=Custom)
    app.config.CORS_ORIGINS = A

    @app.get("/", cors=CORS(credentials=True))
    async def index(request):
        return text("x")

    @app.get("/stream")
    async def stream(request):
        response = await request.respond(content_type="text/plain")
        await response.send("a")
        await response.eof()

    _, response = app.test_client.get("/", headers=origin(A))
    assert allow_origin(response) == A
    assert response.headers["access-control-allow-credentials"] == "true"
    _, response = app.test_client.get("/stream", headers=origin(A))
    assert response.text == "a"
    assert allow_origin(response) == A


def test_a_streamed_response_of_a_route_policy(app: OldmanServer):
    app.config.CORS_ORIGINS = A

    @app.get("/stream", cors=CORS(credentials=True))
    async def stream(request):
        response = await request.respond(content_type="text/plain")
        await response.send("a")
        await response.eof()

    _, response = app.test_client.get("/stream", headers=origin(A))
    assert response.text == "a"
    assert allow_origin(response) == A
    assert response.headers["access-control-allow-credentials"] == "true"


def test_cached_responses_get_each_origin_its_own(app: OldmanServer):
    app.config.CORS_ORIGINS = f"{A}, {B}"
    calls = []

    @app.get("/news")
    @native_cache(30)
    async def news(request):
        calls.append(request.headers.get("origin"))
        return text("news", headers={"Cache-Control": "public"})

    seen = {}
    with ReusableClient(app) as client:  # one server: the cache lives in it
        for who in (A, B, None, A):
            _, response = client.get("/news", headers=origin(who) if who else {})
            seen.setdefault(who, []).append(allow_origin(response))
            assert "Origin" in vary(response)
    assert seen == {A: [A, A], B: [B], None: [None]}
    # The native host keeps the response without CORS headers: the Vary it
    # appends does not stop it caching (the asyncio server has no cache).
    assert len(calls) == (1 if native_server(app.config) else 4)
