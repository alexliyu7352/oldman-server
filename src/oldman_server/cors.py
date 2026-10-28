"""Cross-origin resource sharing (CORS), collected at startup.

The settings (``CORS_ORIGINS`` and the other ``CORS_*``) give an application's
default policy. A route, static route, fixed response, blueprint, blueprint
registration or blueprint group can set its own with ``cors=``: ``False`` for
none, or ``CORS(...)``, whose fields left out come from the settings. Nothing
runs per request in Python for it: when the routes are final
(``OldmanServer.finalize``) every policy is resolved into one table, each
route keeps its policy's code, and the headers are encoded once.

The native host appends a policy's headers to every response it writes,
whatever produced it: the handler, an error, a fixed response, its response
cache or its own 503. The asyncio server applies the same table in Python. A
preflight (``OPTIONS`` with ``Origin`` and ``Access-Control-Request-Method``)
from an allowed origin is answered before routing, without middleware or
handler; any other goes on as an ordinary request.
"""

from __future__ import annotations

import re

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, NamedTuple

from oldman_server.exceptions import MethodNotAllowed, NotFound


if TYPE_CHECKING:
    from oldman_server import OldmanServer
    from oldman_server.request import Request

__all__ = ("CORS",)

_UNSET: Any = type("_Unset", (), {"__repr__": lambda self: "<unset>", "__slots__": ()})()

DEFAULT_METHODS = ("GET", "HEAD", "POST", "PUT", "PATCH", "DELETE")
DEFAULT_MAX_AGE = 600

# scheme://host[:port], or scheme://*.host[:port] for any subdomain of host.
_ORIGIN = re.compile(r"(?P<scheme>[a-z][a-z0-9+.\-]*://)(?P<wild>\*\.)?(?P<host>[a-z0-9.\-\[\]:]+)")
_WILDCARD_LABEL = re.compile(r"[a-z0-9.\-]+")


class CORS:
    """A CORS policy for ``cors=``. A field left out takes the value of the
    application's setting (``origins``: ``CORS_ORIGINS``, ``credentials``:
    ``CORS_ALLOW_CREDENTIALS``, ``methods``: ``CORS_ALLOW_METHODS``,
    ``headers``: ``CORS_ALLOW_HEADERS``, ``expose``: ``CORS_EXPOSE_HEADERS``,
    ``max_age``: ``CORS_MAX_AGE``).

    Args:
        origins: ``"*"``, or allowed origins (``"https://app.example"``; a
            ``"https://*.example.com"`` entry allows any subdomain of
            example.com), as an iterable or a comma-separated string.
        credentials: Send ``Access-Control-Allow-Credentials: true``. With
            credentials no CORS header carries ``"*"`` (a browser sends the
            preflight without credentials and would take it literally):
            origins and expose must list names, methods too; headers
            ``"*"`` is echoed as with None.
        methods: A preflight's ``Access-Control-Allow-Methods``.
        headers: A preflight's ``Access-Control-Allow-Headers``; None echoes
            the request's ``Access-Control-Request-Headers``.
        expose: ``Access-Control-Expose-Headers``.
        max_age: A preflight's ``Access-Control-Max-Age`` in seconds (0: not
            sent).

    Example:
        ```python
        @app.get("/api/report", cors=CORS(credentials=True))
        async def report(request):
            ...
        ```
    """

    __slots__ = ("origins", "credentials", "methods", "headers", "expose", "max_age")

    def __init__(self, origins: Any = _UNSET, *, credentials: Any = _UNSET, methods: Any = _UNSET,
                 headers: Any = _UNSET, expose: Any = _UNSET, max_age: Any = _UNSET):
        self.origins = _UNSET if origins is _UNSET else _origins(origins)
        self.credentials = _UNSET if credentials is _UNSET else bool(credentials)
        self.methods = _UNSET if methods is _UNSET else _names(methods, upper=True)
        self.headers = _UNSET if headers is _UNSET else (None if headers is None else _names(headers))
        self.expose = _UNSET if expose is _UNSET else _names(expose)
        self.max_age = _UNSET if max_age is _UNSET else _max_age(max_age)

    def _fields(self) -> tuple:
        return tuple(getattr(self, name) for name in self.__slots__)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, CORS) and self._fields() == other._fields()

    def __hash__(self) -> int:
        return hash(self._fields())

    def __repr__(self) -> str:
        given = ", ".join(f"{name}={value!r}" for name, value in zip(self.__slots__, self._fields())
                          if value is not _UNSET)
        return f"CORS({given})"


def _names(value: str | Iterable[str], upper: bool = False) -> tuple[str, ...]:
    items = value.split(",") if isinstance(value, str) else list(value)
    names = tuple(str(item).strip() for item in items if str(item).strip())
    return tuple(name.upper() for name in names) if upper else names


def _max_age(value: Any) -> int:
    seconds = int(value)
    if seconds < 0:
        raise ValueError(f"CORS max_age is a number of seconds, 0 or more: {value!r}")
    return seconds


def _origins(value: Any) -> str | tuple[str, ...]:
    """"*", or the allowed origins checked and lowercased; () for none."""
    if value is None or value is False:
        return ()
    if isinstance(value, str) and value.strip() == "*":
        return "*"
    origins = []
    for origin in _names(value):
        origin = origin.lower()
        if origin == "*":
            return "*"
        if not _ORIGIN.fullmatch(origin):
            raise ValueError(
                f"A CORS origin is scheme://host[:port], or scheme://*.host for any "
                f"subdomain, without a path: {origin!r}")
        origins.append(origin)
    return tuple(dict.fromkeys(origins))


class Policy(NamedTuple):
    """A resolved policy, with its headers encoded."""

    any_origin: bool
    origins: frozenset[str]
    wildcards: tuple[tuple[str, str], ...]  # (scheme://, .host suffix)
    credentials: bool
    methods: tuple[str, ...]
    headers: tuple[str, ...] | None
    expose: tuple[str, ...]
    max_age: int

    def allows(self, origin: str | None) -> bool:
        if not origin:
            return False
        if self.any_origin or origin in self.origins:
            return True
        for scheme, suffix in self.wildcards:
            if (len(origin) > len(scheme) + len(suffix) and origin.startswith(scheme)
                    and origin.endswith(suffix)
                    and _WILDCARD_LABEL.fullmatch(origin[len(scheme):len(origin) - len(suffix)])):
                return True
        return False

    @property
    def tail(self) -> tuple[tuple[str, str], ...]:
        """The headers after Access-Control-Allow-Origin."""
        tail = []
        if self.credentials:
            tail.append(("Access-Control-Allow-Credentials", "true"))
        if self.expose:
            tail.append(("Access-Control-Expose-Headers", ", ".join(self.expose)))
        return tuple(tail)

    def response_headers(self, origin: str | None) -> list[tuple[str, str]]:
        """The headers of a response to a request from `origin`: as the
        native host's Service::append_cors writes them."""
        if self.any_origin:
            return [("Access-Control-Allow-Origin", "*"), *self.tail]
        headers = []
        if self.allows(origin):
            headers.append(("Access-Control-Allow-Origin", origin))
            headers.extend(self.tail)
        headers.append(("Vary", "Origin"))
        return headers

    def preflight_headers(self, requested_headers: str | None) -> list[tuple[str, str]]:
        """A preflight answer's own headers (response_headers come with it as
        with any response)."""
        headers = self._preflight
        if self.headers is None and requested_headers:
            headers.append(("Access-Control-Allow-Headers", requested_headers))
            headers.append(("Vary", "Access-Control-Request-Headers"))
        return headers

    @property
    def _preflight(self) -> list[tuple[str, str]]:
        lines = [("Access-Control-Allow-Methods", ", ".join(self.methods))]
        if self.headers is not None:
            lines.append(("Access-Control-Allow-Headers", ", ".join(self.headers)))
        if self.max_age:
            lines.append(("Access-Control-Max-Age", str(self.max_age)))
        return lines

    def native(self) -> tuple:
        """The native host's form (ServerConfig.cors_policies)."""
        def lines(pairs):
            return "".join(f"{name}: {value}\r\n" for name, value in pairs)
        return (self.any_origin, tuple(sorted(self.origins)), self.wildcards, lines(self.tail),
                not self.any_origin, lines(self._preflight), self.headers is None)


def _settings(config) -> dict[str, Any]:
    return {
        "origins": _origins(config.get("CORS_ORIGINS")),
        "credentials": bool(config.get("CORS_ALLOW_CREDENTIALS")),
        "methods": _names(config.get("CORS_ALLOW_METHODS") or DEFAULT_METHODS, upper=True),
        "headers": None if config.get("CORS_ALLOW_HEADERS") is None
        else _names(config.get("CORS_ALLOW_HEADERS")),
        "expose": _names(config.get("CORS_EXPOSE_HEADERS") or ()),
        "max_age": _max_age(config.get("CORS_MAX_AGE", DEFAULT_MAX_AGE) or 0),
    }


def _policy(fields: dict[str, Any], where: str) -> Policy | None:
    origins = fields["origins"]
    if not origins:
        return None
    if origins == "*" and fields["credentials"]:
        raise ValueError(
            f"CORS for {where}: credentials cannot be allowed for any origin (\"*\"); "
            "list the origins")
    credentials = fields["credentials"]
    if credentials:
        # A browser sends a preflight without credentials: a "*" in its answer
        # would then be taken literally by the credentialed request.
        for name in ("methods", "expose"):
            if "*" in fields[name]:
                raise ValueError(
                    f"CORS for {where}: with credentials, {name} lists names, not \"*\"")
    headers = fields["headers"]
    if headers is not None and credentials and "*" in headers:
        headers = None  # echo the requested headers instead of a literal "*"
    exact = frozenset(o for o in origins if "*" not in o) if origins != "*" else frozenset()
    wildcards = tuple(
        (match["scheme"], "." + match["host"])
        for origin in (origins if origins != "*" else ())
        if (match := _ORIGIN.fullmatch(origin)) and match["wild"])
    return Policy(origins == "*", exact, wildcards, credentials, fields["methods"], headers,
                  fields["expose"], fields["max_age"])


def _resolve(option: Any, settings: dict[str, Any], default: Policy | None, where: str) -> Policy | None:
    if option is None or option is True:
        return default
    if option is False:
        return None
    if not isinstance(option, CORS):
        raise TypeError(f"cors= takes None, False or CORS(...), not {option!r} ({where})")
    fields = {name: settings[name] if value is _UNSET else value
              for name, value in zip(CORS.__slots__, option._fields())}
    if not fields["origins"]:
        raise ValueError(f"CORS for {where} has no origins: give origins= or set CORS_ORIGINS")
    return _policy(fields, where)


class CorsTable:
    """The policies of an application, compiled by ``compile``. A response's
    code: 0 the default policy, n policy n - 1 (1: none)."""

    __slots__ = ("policies", "default", "route_policies", "fixed")

    def __init__(self, policies: list[Policy | None], default: int, route_policies: bool,
                 fixed: dict[str, int]):
        self.policies = policies
        self.default = default
        # Whether a route has a policy of its own: then responses carry their
        # route's code and only Python, which routes, answers preflights.
        self.route_policies = route_policies
        self.fixed = fixed

    def policy(self, code: int) -> Policy | None:
        return self.policies[self.default if code == 0 else code - 1]

    def response_headers(self, code: int, origin: str | None) -> list[tuple[str, str]]:
        policy = self.policy(code)
        return policy.response_headers(origin) if policy is not None else []

    def native(self) -> dict[str, Any]:
        """The native host's ServerConfig fields."""
        empty = (False, (), (), "", False, "", False)
        return {"cors_policies": tuple(empty if p is None else p.native() for p in self.policies),
                "cors_default": self.default, "cors_route_policies": self.route_policies}

    async def preflight(self, app: OldmanServer, request: Request) -> bool:
        """Answer `request` if it is a preflight from an allowed origin. With
        route policies, the route its Access-Control-Request-Method would
        reach decides the policy (none found: not answered); otherwise the
        default policy answers for any path, as the native host does. False
        (nothing sent) for any other request."""
        headers = request.headers
        origin = headers.get("origin")
        method = headers.get("access-control-request-method")
        if not origin or not method:
            return False
        route = None
        if self.route_policies:
            try:
                route, _, _ = app.router.get(request.path, method.strip().upper(),
                                             request._get_routing_host())
            except (NotFound, MethodNotAllowed):
                return False
        policy = self.policy(getattr(route.extra, "cors_code", 0) if route is not None else 0)
        if policy is None or not policy.allows(origin):
            return False
        from oldman_server.response import HTTPResponse

        response = HTTPResponse(status=204, headers=policy.preflight_headers(
            headers.get("access-control-request-headers")))
        # Its Allow-Origin and Vary come with the policy, as on any response
        # to a request routed there.
        request.route = route
        request.stream.respond(response)
        await response.send(end_stream=True)
        return True


def compile(app: OldmanServer) -> CorsTable | None:
    """The application's table, or None when no policy allows anything (CORS
    off). Each route's code is kept in ``route.extra.cors_code``."""
    settings = _settings(app.config)
    default = _policy(settings, "the application (CORS_* settings)")
    policies: list[Policy | None] = [None]
    index = {None: 0}

    def position(policy: Policy | None) -> int:
        if policy not in index:
            index[policy] = len(policies)
            policies.append(policy)
        return index[policy]

    default_index = position(default)

    def code(option: Any, where: str) -> int:
        if option is None:
            return 0
        found = position(_resolve(option, settings, default, where))
        return 0 if found == default_index else found + 1

    route_policies = False
    for route in app.router.routes:
        route.extra.cors_code = code(getattr(route.extra, "cors", None), f"the route {route.name}")
        route_policies = route_policies or route.extra.cors_code != 0
    fixed = {path: code(option, f"the fixed response {path}")
             for path, option in app._fixed_cors.items()}
    if all(policy is None for policy in policies):
        return None
    return CorsTable(policies, default_index, route_policies, fixed)
