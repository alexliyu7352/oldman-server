"""Native request contracts without starting a Seastar runtime."""
import asyncio
import cProfile
from collections import Counter, defaultdict
import pstats

import pytest
from oldman_server import OldmanServer
from oldman_server.compat import Header, NativeHeaders
from oldman_server.exceptions import URITooLong
from oldman_server.models.server_types import ConnInfo, _ConnectionInfo
from oldman_server.request import Request
from oldman_server.request.parameters import RequestParameters
import oldman_server.request.types as request_types
from oldman_server.response import text
from oldman_server.router import Router
import uvloop

from oldman_server.native.engine import ApplicationRequest
from oldman_server.native.host import NativeApplication


class ObservedRequest:
    """Count boundary reads; values/header lookup come from the real binding."""
    def __init__(self, target=b"/", headers=((b"Host", b"original.example"), (b"X-Text", b"caf\xc3\xa9\xff"))):
        self.request = ApplicationRequest("GET", target, headers, b"body")
        self.reads = Counter()

    def __getattr__(self, name):
        self.reads[name] += 1
        if name == "http_version":
            return "1.1"
        if name == "client":
            return "127.0.0.1", 4321
        if name == "server":
            return "127.0.0.1", 8000
        return getattr(self.request, name)

    def validate_sanic_request(self):
        # The synthetic binding has no socket metadata. Real runtime validation
        # is covered by the socket test; this source supplies the addresses above.
        self.reads["validate"] += 1

    def sanic_request(self):
        # The flat adapter's single validating read, with the same metadata.
        self.reads["sanic_request"] += 1
        return self.request.target, "1.1", self.request.method, self.request.body, False


@pytest.fixture
def query_cache_constructions(monkeypatch):
    created = []

    class ObservedDefaultDict(defaultdict):
        def __init__(self, factory):
            super().__init__(factory)
            created.append(self)

    monkeypatch.setattr(request_types, "defaultdict", ObservedDefaultDict)
    return created


@pytest.fixture(scope="module")
def query_app():
    return OldmanServer("query_cache_contracts", configure_logging=False)


def test_native_query_caches_allocate_independently_on_first_use(query_app, query_cache_constructions):
    request = Request._from_native(ObservedRequest(target=b"/?q=1&q=2"), query_app)
    assert query_cache_constructions == []
    grouped = request.parsed_args
    assert query_cache_constructions == [grouped]
    assert grouped.default_factory is RequestParameters
    assert request.args == {"q": ["1", "2"]}
    assert request.parsed_args is grouped and len(query_cache_constructions) == 1
    flat = request.parsed_not_grouped_args
    assert len(query_cache_constructions) == 2
    assert flat.default_factory is list
    assert request.query_args == [("q", "1"), ("q", "2")]
    assert request.parsed_not_grouped_args is flat
    assert len(query_cache_constructions) == 2
    other = Request._from_native(ObservedRequest(), request.app)
    assert len(query_cache_constructions) == 2
    assert other.args == {} and other.query_args == []
    assert other.parsed_args is not grouped and other.parsed_not_grouped_args is not flat
    assert len(query_cache_constructions) == 4


@pytest.mark.parametrize("target,options,grouped,flat", [
    (b"/", {}, {}, []),
    (b"/?q=1&q=2&blank=", {}, {"q": ["1", "2"]}, [("q", "1"), ("q", "2")]),
    (b"/?q=1&blank=", {"keep_blank_values": True}, {"q": ["1"], "blank": [""]}, [("q", "1"), ("blank", "")]),
    (b"/?q=%FF", {}, {"q": ["\ufffd"]}, [("q", "\ufffd")]),
    (b"/?q=%FF", {"encoding": "latin-1"}, {"q": ["ÿ"]}, [("q", "ÿ")]),
    (b"/?q=%FF", {"errors": "ignore"}, {"q": [""]}, [("q", "")]),
])
def test_native_query_options_and_cached_identity(query_app, target, options, grouped, flat):
    request = Request._from_native(ObservedRequest(target=target), query_app)
    args = request.get_args(**options)
    pairs = request.get_query_args(**options)
    assert args == grouped and pairs == flat
    assert request.get_args(**options) is args
    assert request.get_query_args(**options) is pairs
    request.parsed_args.clear()
    request.parsed_not_grouped_args.clear()
    assert request.get_args(**options) == grouped
    assert request.get_query_args(**options) == flat


@pytest.mark.parametrize("method", ["get_args", "get_query_args"])
@pytest.mark.parametrize("target,options,error", [
    (b"/?broken", {"strict_parsing": True}, ValueError),
    (b"/?q=%FF", {"errors": "strict"}, UnicodeDecodeError),
])
def test_native_query_parse_errors_are_not_cached(query_app, method, target, options, error):
    request = Request._from_native(ObservedRequest(target=target), query_app)
    parse = getattr(request, method)
    for _ in range(2):
        with pytest.raises(error):
            parse(**options)


@pytest.mark.parametrize("attribute,method,expected,factory", [
    ("parsed_args", "get_args", {"q": ["1"]}, RequestParameters),
    ("parsed_not_grouped_args", "get_query_args", [("q", "1")], list),
])
def test_native_query_public_cache_assignment_and_deletion(query_app, attribute, method, expected, factory):
    request = Request._from_native(ObservedRequest(target=b"/?q=1"), query_app)
    key = (False, False, "utf-8", "replace")
    replacement = {key: expected}
    setattr(request, attribute, replacement)
    assert getattr(request, attribute) is replacement
    assert getattr(request, method)() is expected
    for invalid in (None, 42):
        setattr(request, attribute, invalid)
        assert getattr(request, attribute) is invalid
        with pytest.raises(TypeError):
            getattr(request, method)()
    delattr(request, attribute)
    with pytest.raises(AttributeError):
        getattr(request, attribute)
    with pytest.raises(AttributeError):
        getattr(request, method)()
    replacement = defaultdict(factory)
    setattr(request, attribute, replacement)
    assert getattr(request, method)() == expected
    assert getattr(request, attribute) is replacement


@pytest.mark.parametrize("native", [False, True])
def test_custom_query_cache_setters_keep_initialization_timing(query_app, native, query_cache_constructions):
    events = []

    class Custom(Request):
        @property
        def parsed_args(self):
            return self.grouped

        @parsed_args.setter
        def parsed_args(self, value):
            events.append(("grouped", hasattr(self, "parsed_cookies"), hasattr(self, "parsed_json")))
            self.grouped = value

        @property
        def parsed_not_grouped_args(self):
            return self.flat

        @parsed_not_grouped_args.setter
        def parsed_not_grouped_args(self, value):
            events.append(("flat", hasattr(self, "parsed_cookies"), hasattr(self, "parsed_json")))
            self.flat = value

    app = query_app
    request = (Custom._from_native(ObservedRequest(target=b"/?q=1"), app) if native else
               Custom(b"/?q=1", Header(), "1.1", "GET", None, app))
    assert events == [("grouped", False, False), ("flat", True, True)]
    assert len(query_cache_constructions) == 2
    assert request.args == {"q": ["1"]} and request.query_args == [("q", "1")]
    if not native:
        ordinary = Request(b"/", Header(), "1.1", "GET", None, app)
        assert len(query_cache_constructions) == 4
        assert ordinary.args == {} and ordinary.query_args == []


def test_native_header_bytes_preserves_encoding_and_validation():
    source = ApplicationRequest("GET", b"/", ((b"HoSt", b"caf\xc3\xa9\xff"),), b"")
    assert source.header_bytes("host") == b"caf\xc3\xa9\xff"
    assert source.header("host") == "caf\xc3\xa9\xff"
    fallback = object()
    assert source.header_bytes("missing", fallback) is fallback
    with pytest.raises(TypeError):
        source.header_bytes(b"host")
    with pytest.raises(UnicodeEncodeError):
        source.header_bytes("höst")
    with pytest.raises(ValueError, match="metadata"):
        source.validate_sanic_request()


def test_request_native_fields_materialize_only_when_used():
    source = ObservedRequest()
    app = OldmanServer("native_fields", configure_logging=False)
    request = Request._from_native(source, app)
    assert type(request) is Request
    assert request.path == "/" and request.method == "GET"
    assert source.reads == {"validate": 1, "target": 1, "http_version": 1, "method": 1}
    assert request._get_routing_host() == "original.example"
    assert source.reads["sanic_headers"] == source.reads["client"] == source.reads["server"] == 0
    headers = request.headers
    assert isinstance(headers, NativeHeaders)
    assert headers["x-text"] == "café\udcff"
    # A lookup queries the one header; the whole list is read only for
    # whole-mapping operations, once.
    assert request.headers is headers and source.reads["sanic_header"] == 1
    assert source.reads["sanic_headers"] == 0
    # Lowercase names, as the asyncio server's parser gives them.
    assert dict(headers.items()) == {"host": "original.example", "x-text": "café\udcff"}
    assert len(headers) == 2 and source.reads["sanic_headers"] == 1
    assert request.ip == "127.0.0.1" and request.port == 4321
    info = request.conn_info
    assert info.server_port == 8000 and request.conn_info is info
    assert source.reads["client"] == source.reads["server"] == 1
    assert request.transport.get_extra_info("peername") == ("127.0.0.1", 4321)


def test_request_public_assignments_and_legacy_constructor():
    app = OldmanServer("native_assignments", configure_logging=False)
    source = ObservedRequest()
    request = Request._from_native(source, app)
    replacement = Header({"Host": "replacement.example"})
    request.headers = replacement
    assert request.headers is replacement
    assert request._get_routing_host() == "replacement.example"
    assert source.reads["sanic_headers"] == 0
    request.headers = request.conn_info = request.transport = None
    assert request.headers is request.conn_info is request.transport is None
    assert source.reads["client"] == source.reads["server"] == 0
    transport = _ConnectionInfo(("127.0.0.1", 1), ("127.0.0.1", 80))
    original = Header({"Host": "legacy.example"})
    legacy = Request(b"/legacy?q=1", original, "1.1", "GET", transport, app)
    original["Host"] = "outside.example"
    assert legacy.headers["Host"] == legacy._get_routing_host() == "legacy.example"
    assert legacy.path == "/legacy" and legacy.args["q"] == ["1"]
    assert legacy.transport is transport and legacy.conn_info is None


def test_request_native_host_matches_mutable_headers():
    request = Request._from_native(ObservedRequest(), OldmanServer("native_host", configure_logging=False))
    request.headers["HOST"] = "changed.example"
    assert request._get_routing_host() == "changed.example"
    del request.headers["host"]
    assert request._get_routing_host() is None
    with pytest.raises(URITooLong):
        Request._from_native(ObservedRequest(target=b"/" * 65536), request.app)


def test_custom_request_constructor_keeps_property_setters():
    class Custom(Request):
        @property
        def headers(self):
            return self.custom_headers

        @headers.setter
        def headers(self, value):
            self.custom_headers = value
            value["host"] = "setter.example"

    request = Custom(b"/", Header({"host": "input.example"}), "1.1", "GET", None,
                     OldmanServer("setter_request", configure_logging=False))
    assert request.headers["host"] == request._get_routing_host() == "setter.example"


@pytest.mark.parametrize("client,server,expected", [
    (("127.0.0.1", 1234), ("127.0.0.2", 80), ("127.0.0.1", "127.0.0.2")),
    (("::1", 1234, 0, 0), ("::2", 8080, 0, 0), ("[::1]", "[::2]:8080")),
    # Native IPv6 (host, port) pairs read like the socket module's 4-tuples.
    (("::1", 1234), ("::2", 8080), ("[::1]", "[::2]:8080")),
])
def test_native_conn_info_matches_transport_metadata(client, server, expected):
    direct = ConnInfo._from_addresses(client, server)
    ordinary = ConnInfo(_ConnectionInfo(client, server))
    assert (direct.client, direct.server) == expected
    for name in ConnInfo.__slots__:
        assert getattr(direct, name) == getattr(ordinary, name)
    assert direct.ctx is not ordinary.ctx


@pytest.mark.parametrize("custom", [False, True])
@pytest.mark.parametrize("touchup", [False, True])
def test_native_sanic_host_signal_and_custom_constructor(custom, touchup):
    async def scenario():
        constructed, seen = [], []
        class CustomRequest(Request):
            def __init__(self, url, headers, *args, **kwargs):
                assert isinstance(headers, Header)
                constructed.append(headers["host"])
                super().__init__(url, headers, *args, **kwargs)
        app = OldmanServer(f"native_host_{custom}_{touchup}", request_class=CustomRequest if custom else Request,
                    configure_logging=False)
        app.config.TOUCHUP = touchup
        app.config.ACCESS_LOG = False
        app.state.native_loop = asyncio.get_running_loop()
        @app.signal("http.routing.before")
        async def change_host(request):
            request.headers["host"] = "changed.example"
        @app.get("/", host="changed.example")
        async def handler(request):
            await asyncio.sleep(0)
            seen.append(request)
            assert Request.get_current() is request
            return text("routed")
        await app._startup()
        result = await NativeApplication(app, 64)(ObservedRequest())
        assert result[:2] == (200, b"routed")
        assert len(seen) == 1
        assert type(seen[0]) is (CustomRequest if custom else Request)
        assert constructed == (["original.example"] if custom else [])
    uvloop.run(scenario())


@pytest.mark.parametrize("touchup", [False, True])
def test_hostless_native_routing_skips_lookup_and_keeps_public_host(touchup, query_cache_constructions):
    async def scenario():
        retained = []
        app = OldmanServer(f"hostless_{touchup}", configure_logging=False)
        app.config.TOUCHUP = touchup
        app.state.native_loop = asyncio.get_running_loop()
        @app.middleware("request")
        async def before(request):
            assert query_cache_constructions == []
        @app.middleware("response")
        async def after(request, response):
            assert query_cache_constructions == []
        @app.get("/")
        async def handler(request):
            retained.append(request)
            return text("ok")
        await app._startup()
        source = ObservedRequest()
        assert (await NativeApplication(app, 64)(source))[:2] == (200, b"ok")
        assert query_cache_constructions == []
        assert source.reads["header_bytes"] == source.reads["sanic_headers"] == 0
        assert retained[0].headers["host"] == retained[0].host == "original.example"
        # Header lookups, without building the whole list.
        assert source.reads["sanic_headers"] == 0 and source.reads["sanic_header"] == 2
    uvloop.run(scenario())


@pytest.mark.parametrize("mixed", [False, True])
def test_host_constraints_keep_native_lookup_for_the_entire_router(mixed):
    async def scenario():
        app = OldmanServer(f"host_required_{mixed}", configure_logging=False)
        app.state.native_loop = asyncio.get_running_loop()
        @app.get("/", host=None if mixed else "original.example")
        async def handler(request): return text("ok")
        if mixed:
            app.add_route(handler, "/restricted", host="other.example", name="restricted")
        await app._startup()
        source = ObservedRequest()
        assert (await NativeApplication(app, 64)(source))[:2] == (200, b"ok")
        assert source.reads["header_bytes"] == 1
        assert source.reads["sanic_headers"] == 0
    uvloop.run(scenario())


@pytest.mark.parametrize("path,method,status,body", [
    (b"/item/7", "GET", 200, b"int:7"),
    (b"/item/7", "HEAD", 200, b"int:7"),
    (b"/item/bad", "GET", 404, None),
    (b"/item/7", "POST", 405, None),
    (b"/missing", "GET", 404, None),
])
def test_hostless_routing_preserves_dynamic_types_and_method_errors(path, method, status, body):
    async def scenario():
        app = OldmanServer(f"hostless_dynamic_{path.decode().replace('/', '_')}_{method}", configure_logging=False)
        app.state.native_loop = asyncio.get_running_loop()
        @app.route("/item/<number:int>", methods=["GET", "HEAD"])
        async def handler(request, number):
            assert type(number) is int
            return text(f"int:{number}")
        await app._startup()
        source = ObservedRequest(target=path)
        source.request = ApplicationRequest(method, path, ((b"Host", b"original.example"),), b"")
        result = await NativeApplication(app, 65536)(source)
        assert result[0] == status
        if body is not None:
            assert result[1] == body
        assert source.reads["header_bytes"] == 0
        # Sanic's error formatter may read Accept from public headers.
        if status == 200:
            assert source.reads["sanic_headers"] == 0
    uvloop.run(scenario())


@pytest.mark.parametrize("change", ["read", "modify", "replace", "invalid"])
@pytest.mark.parametrize("touchup", [False, True])
def test_hostless_routing_before_header_effects_keep_the_original_path(change, touchup):
    async def scenario():
        seen = []
        app = OldmanServer(f"hostless_header_{change}_{touchup}", configure_logging=False)
        app.config.TOUCHUP = touchup
        app.state.native_loop = asyncio.get_running_loop()
        @app.signal("http.routing.before")
        async def before(request):
            if change == "read":
                seen.append(request.headers["host"])
            elif change == "modify":
                request.headers["host"] = "changed.example"
            elif change == "replace":
                request.headers = Header({"host": "changed.example"})
            else:
                request.headers = None
        @app.exception(AttributeError)
        async def invalid_header(request, error): return text("invalid", status=418)
        @app.get("/")
        async def handler(request):
            return text(request.headers["host"])
        await app._startup()
        source = ObservedRequest()
        result = await NativeApplication(app, 65536)(source)
        assert result[:2] == ((418, b"invalid") if change == "invalid" else
                             (200, b"original.example" if change == "read" else b"changed.example"))
        assert seen == (["original.example"] if change == "read" else [])
        assert source.reads["header_bytes"] == 0
        # A change builds the whole Header; reads look the header up: the
        # signal's, routing's (headers were read before it) and the handler's.
        assert source.reads["sanic_headers"] == (1 if change == "modify" else 0)
        assert source.reads["sanic_header"] == (3 if change == "read" else 0)
    uvloop.run(scenario())


@pytest.mark.parametrize("custom", [
    "request", "router", "get", "_get", "resolve", "class_get", "class__get", "class_resolve",
])
def test_custom_routing_receives_actual_host(custom, monkeypatch):
    async def scenario():
        hosts = []
        class CustomRequest(Request):
            def _get_routing_host(self):
                value = super()._get_routing_host()
                hosts.append(value)
                return value
        class CustomRouter(Router):
            def get(self, path, method, host):
                hosts.append(host)
                return super().get(path, method, host)
        app = OldmanServer(f"hostless_custom_{custom}", configure_logging=False,
                    request_class=CustomRequest if custom == "request" else Request,
                    router=CustomRouter() if custom == "router" else None)
        app.state.native_loop = asyncio.get_running_loop()
        @app.get("/")
        async def handler(request): return text("ok")
        await app._startup()
        # Change the method AFTER finalization: a frozen capability cannot
        # silently feed None into a subsequently installed custom matcher.
        method = custom.removeprefix("class_")
        if method in ("get", "_get", "resolve"):
            original = getattr(app.router, method)
            def observe(*args, **kwargs):
                if custom.startswith("class_"):
                    args = args[1:]
                hosts.append(kwargs.get("extra", {}).get("host") if method == "resolve" else args[2])
                return original(*args, **kwargs)
            monkeypatch.setattr(Router if custom.startswith("class_") else app.router, method, observe)
        source = ObservedRequest()
        assert (await NativeApplication(app, 64)(source))[:2] == (200, b"ok")
        assert hosts == ["original.example"]
        assert source.reads["header_bytes"] == (0 if custom == "request" else 1)
    uvloop.run(scenario())


def test_router_reset_recomputes_host_demand_before_reusing_native_requests():
    async def scenario():
        app = OldmanServer("hostless_reset", configure_logging=False)
        app.state.native_loop = asyncio.get_running_loop()
        @app.get("/")
        async def handler(request): return text("ok")
        await app._startup()
        adapter = NativeApplication(app, 64)
        first = ObservedRequest()
        assert (await adapter(first))[:2] == (200, b"ok")
        assert first.reads["header_bytes"] == 0
        app.router.reset()
        unfinished = ObservedRequest()
        assert Request._from_native(unfinished, app)._get_routing_host() == "original.example"
        app.router.add("/restricted", ["GET"], handler, host="original.example", name="restricted")
        app.finalize()
        second = ObservedRequest(target=b"/restricted")
        assert (await adapter(second))[:2] == (200, b"ok")
        assert second.reads["header_bytes"] == 1
    uvloop.run(scenario())


@pytest.mark.parametrize("method", ["get", "_get", "resolve"])
def test_transplanted_standard_router_methods_keep_the_donor_host(method, monkeypatch):
    async def scenario():
        app = OldmanServer(f"hostless_delegate_{method}", configure_logging=False)
        app.state.native_loop = asyncio.get_running_loop()
        @app.get("/")
        async def original(request): return text("original")
        await app._startup()
        donor = OldmanServer(f"host_donor_{method}", configure_logging=False)
        donor.state.native_loop = asyncio.get_running_loop()
        @donor.get("/", host="original.example")
        async def delegated(request): return text("delegated")
        await donor._startup()
        # The function is standard, but its receiver has a different route table.
        monkeypatch.setattr(app.router, method, getattr(donor.router, method))
        source = ObservedRequest()
        assert (await NativeApplication(app, 65536)(source))[:2] == (200, b"delegated")
        assert source.reads["header_bytes"] == 1
    uvloop.run(scenario())


def test_hostless_dispatch_omits_the_routing_host_preparation_frame():
    async def scenario():
        app = OldmanServer("hostless_prepare_frame", configure_logging=False)
        app.state.native_loop = asyncio.get_running_loop()
        @app.get("/")
        async def handler(request): return text("ok")
        await app._startup()
        adapter = NativeApplication(app, 64)
        source = ObservedRequest()
        profile = cProfile.Profile()
        profile.enable()
        try:
            result = await adapter(source)
        finally:
            profile.disable()
        assert result[:2] == (200, b"ok")
        code = Request._get_routing_host.__code__
        calls = pstats.Stats(profile).stats.get((code.co_filename, code.co_firstlineno, code.co_name))
        assert calls is None
        assert source.reads["header_bytes"] == 0
    uvloop.run(scenario())


def _slot_state(request):
    names = {name for cls in type(request).__mro__ for name in getattr(cls, "__slots__", ())}
    state = {}
    for name in sorted(names):
        try:
            value = object.__getattribute__(request, name)
        except AttributeError:
            value = "<unset>"
        state[name] = repr(value) if name == "_parsed_url" else value
    return state


def test_native_fields_constructor_matches_general_constructor():
    app = OldmanServer("native_fields_constructor", configure_logging=False)
    native = ApplicationRequest("GET", b"/a/b?q=1", ((b"Host", b"x"),), b"",
                                http_version="1.1", client=("127.0.0.1", 1), server=("127.0.0.1", 2))
    general = Request(b"/a/b?q=1", None, "1.1", "GET", None, app, _native_request=native)
    fast = Request._from_native_fields(b"/a/b?q=1", "1.1", "GET", app, native)
    general_state, fast_state = _slot_state(general), _slot_state(fast)
    assert general_state.keys() == fast_state.keys()
    for name in general_state:
        if name == "_parsed_url":
            assert (general._parsed_url.path, general._parsed_url.query) == (fast._parsed_url.path, fast._parsed_url.query)
        elif name == "_match_info":
            assert general._match_info == fast._match_info == {} and general._match_info is not fast._match_info
        else:
            assert general_state[name] == fast_state[name], name
    for url, error in ((b"/" + b"x" * 65535, URITooLong), (b"http://[::1", Exception)):
        with pytest.raises(error) as general_error:
            Request(url, None, "1.1", "GET", None, app, _native_request=native)
        with pytest.raises(error) as fast_error:
            Request._from_native_fields(url, "1.1", "GET", app, native)
        assert type(general_error.value) is type(fast_error.value)


def test_native_fields_constructor_keeps_subclass_initializers():
    class Custom(Request):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.ctx.custom = True
    app = OldmanServer("native_fields_subclass", configure_logging=False)
    native = ApplicationRequest("GET", b"/", (), b"", http_version="1.1",
                                client=("127.0.0.1", 1), server=("127.0.0.1", 2))
    request = Custom._from_native_fields(b"/", "1.1", "GET", app, native)
    assert type(request) is Custom and request.ctx.custom
