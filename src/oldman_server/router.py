from __future__ import annotations

from collections.abc import Iterable
from functools import lru_cache
from inspect import signature
from typing import Any
from uuid import UUID

from oldman_server.routing import BaseRouter
from oldman_server.routing.exceptions import NoMethod
from oldman_server.routing.exceptions import NotFound as RoutingNotFound
from oldman_server.routing.group import RouteGroup
from oldman_server.routing.route import Route

from oldman_server.constants import HTTP_METHODS
from oldman_server.errorpages import check_error_format
from oldman_server.exceptions import MethodNotAllowed, NotFound, OldmanServerException
from oldman_server.models.handler_types import RouteHandler


ROUTER_CACHE_SIZE = 1024
ALLOWED_LABELS = ("__file_uri__",)


class Router(BaseRouter):
    """The router implementation responsible for routing a `Request` object to the appropriate handler."""  # noqa: E501

    DEFAULT_METHOD = "GET"
    ALLOWED_METHODS = HTTP_METHODS
    # Unfinalized and reset routers must read the actual Host conservatively.
    _routing_requires_host = True
    # The native host's cache of get(path, method, None) results by (raw path,
    # method) while the finalized route table stands and needs no Host; None
    # otherwise. A new one with each finalize, none after a reset.
    _native_routes = None

    def _get(
        self, path: str, method: str, host: str | None
    ) -> tuple[Route, RouteHandler, dict[str, Any]]:
        extra = {"host": host} if host else None
        try:
            return self.resolve(path=path, method=method, extra=extra)
        except RoutingNotFound as e:
            # A group whose routes require a host checks the method with the
            # host, so a method it lacks reads as no route at all.
            config = self.ctx.app.config
            automatic = (method == "HEAD" and config.HTTP_AUTO_HEAD) or (
                method == "OPTIONS" and config.HTTP_AUTO_OPTIONS
            )
            allowed = self._methods_at(path, extra) if automatic else None
            if not allowed:
                raise NotFound(f"Requested URL {e.path} not found") from None
        except NoMethod as e:
            # A dynamic group of several routes does not say which methods
            # it has.
            allowed = frozenset(map(str, e.allowed_methods or ())) or (
                self._methods_at(path, extra)
            )
        # HEAD and OPTIONS are methods of the same resource (RFC 9110 9.3.2,
        # 9.3.7), not routes of their own: only here, where the method was not
        # found, do they come in, so other requests route as before.
        return self._automatic(path, method, extra, allowed)

    def _automatic(
        self,
        path: str,
        method: str,
        extra: dict[str, str] | None,
        allowed: frozenset[str],
    ) -> tuple[Route, RouteHandler, dict[str, Any]]:
        """The GET route itself for a HEAD request (HTTP_AUTO_HEAD); otherwise
        MethodNotAllowed, its Allow listing the automatic HEAD and OPTIONS
        too, marked for OldmanServer.handle_exception to answer an OPTIONS
        request with 204 and that Allow (HTTP_AUTO_OPTIONS)."""
        config = self.ctx.app.config
        get = None
        websocket = False
        if "GET" in allowed:
            try:
                get = self.resolve(path=path, method="GET", extra=extra)
            except (RoutingNotFound, NoMethod):
                pass  # the GET route of another host
            else:
                # A WebSocket route has no HEAD, nor an automatic OPTIONS.
                websocket = hasattr(get[1], "is_websocket")
        auto_head = get is not None and not websocket and config.HTTP_AUTO_HEAD
        if auto_head and method == "HEAD":
            return get
        auto_options = config.HTTP_AUTO_OPTIONS and bool(
            allowed - {"GET"} if websocket else allowed
        )
        methods = set(allowed)
        if auto_head:
            methods.add("HEAD")
        if auto_options:
            methods.add("OPTIONS")
        error = MethodNotAllowed(
            f"Method {method} not allowed for URL {path}",
            method=method,
            allowed_methods=tuple(sorted(methods)) if methods else None,
        )
        error.answer_options = auto_options and method == "OPTIONS"
        raise error

    def _methods_at(
        self, path: str, extra: dict[str, str] | None
    ) -> frozenset[str]:
        """The methods a request for `path` (and host) resolves with: only
        on the way to an error, when routing did not say."""
        methods = []
        for candidate in self.ALLOWED_METHODS:
            try:
                self.resolve(path=path, method=candidate, extra=extra)
            except (RoutingNotFound, NoMethod):
                continue
            methods.append(str(candidate))
        return frozenset(methods)

    @lru_cache(maxsize=ROUTER_CACHE_SIZE)
    def get(  # type: ignore
        self, path: str, method: str, host: str | None
    ) -> tuple[Route, RouteHandler, dict[str, Any]]:
        """Retrieve a `Route` object containing the details about how to handle a response for a given request

        :param request: the incoming request object
        :type request: Request
        :return: details needed for handling the request and returning the
            correct response
        :rtype: Tuple[ Route, RouteHandler, Dict[str, Any]]

        Args:
            path (str): the path of the route
            method (str): the HTTP method of the route
            host (Optional[str]): the host of the route

        Raises:
            NotFound: if the route is not found
            MethodNotAllowed: if the method is not allowed for the route

        Returns:
            Tuple[Route, RouteHandler, Dict[str, Any]]: the route, handler, and match info
        """  # noqa: E501
        __tracebackhide__ = True
        return self._get(path, method, host)

    # Keep the original call path recognizable even if an application replaces
    # an instance or class method after finalization.
    _standard_get = get
    _standard_get_impl = _get
    _standard_resolve = BaseRouter.resolve

    @property
    def _native_routing_methods_standard(self) -> bool:
        overrides = self.__dict__
        return (
            type(self) is Router
            and "get" not in overrides
            and "_get" not in overrides
            and "resolve" not in overrides
            and Router.get is Router._standard_get
            and Router._get is Router._standard_get_impl
            and Router.resolve is Router._standard_resolve
        )

    @property
    def _native_routing_host_optional(self) -> bool:
        return (
            not self._routing_requires_host
            and self._native_routing_methods_standard
        )

    def add(  # type: ignore
        self,
        uri: str,
        methods: Iterable[str],
        handler: RouteHandler,
        host: str | Iterable[str] | None = None,
        strict_slashes: bool = False,
        stream: bool = False,
        ignore_body: bool = False,
        version: str | float | int | None = None,
        name: str | None = None,
        unquote: bool = False,
        static: bool = False,
        version_prefix: str = "/v",
        overwrite: bool = False,
        error_format: str | None = None,
        cors: Any = None,
    ) -> Route | list[Route]:
        """Add a handler to the router

        Args:
            uri (str): The path of the route.
            methods (Iterable[str]): The types of HTTP methods that should be attached,
                example: ["GET", "POST", "OPTIONS"].
            handler (RouteHandler): The sync or async function to be executed.
            host (Optional[str], optional): Host that the route should be on. Defaults to None.
            strict_slashes (bool, optional): Whether to apply strict slashes. Defaults to False.
            stream (bool, optional): Whether to stream the response. Defaults to False.
            ignore_body (bool, optional): Whether the incoming request body should be read.
                Defaults to False.
            version (Union[str, float, int], optional): A version modifier for the uri. Defaults to None.
            name (Optional[str], optional): An identifying name of the route. Defaults to None.

        Returns:
            Route: The route object.
        """  # noqa: E501

        if version is not None:
            version = str(version).strip("/").lstrip("v")
            uri = "/".join([f"{version_prefix}{version}", uri.lstrip("/")])

        uri = self._normalize(uri, handler)

        params = dict(
            path=uri,
            handler=handler,
            methods=frozenset(map(str, methods)) if methods else None,
            name=name,
            strict=strict_slashes,
            unquote=unquote,
            overwrite=overwrite,
        )

        if isinstance(host, str):
            hosts = [host]
        else:
            hosts = host or [None]  # type: ignore

        routes = []

        for host in hosts:
            if host:
                params.update({"requirements": {"host": host}})

            ident = name
            if len(hosts) > 1:
                ident = (
                    f"{name}_{host.replace('.', '_')}"
                    if name
                    else "__unnamed__"
                )

            route = super().add(**params)  # type: ignore
            route.extra.ident = ident
            route.extra.ignore_body = ignore_body
            route.extra.stream = stream
            route.extra.hosts = hosts
            route.extra.static = static
            route.extra.error_format = error_format
            # Its CORS policy option (oldman_server.cors resolves it at finalize).
            route.extra.cors = cors

            if error_format:
                check_error_format(route.extra.error_format)

            routes.append(route)

        if len(routes) == 1:
            return routes[0]
        return routes

    @lru_cache(maxsize=ROUTER_CACHE_SIZE)
    def find_route_by_view_name(
        self, view_name: str, name: str | None = None
    ) -> Route | None:
        """Find a route in the router based on the specified view name.

        Args:
            view_name (str): the name of the view to search for
            name (Optional[str], optional): the name of the route. Defaults to `None`.

        Returns:
            Optional[Route]: the route object
        """  # noqa: E501
        if not view_name:
            return None

        route = self.name_index.get(view_name)
        if not route:
            full_name = self.ctx.app.generate_name(view_name)
            route = self.name_index.get(full_name)

        if not route:
            return None

        return route

    @property
    def routes_all(self) -> dict[tuple[str, ...], Route]:
        """Return all routes in the router.

        Returns:
            Dict[Tuple[str, ...], Route]: a dictionary of routes
        """
        return {route.parts: route for route in self.routes}

    @property
    def routes_static(self) -> dict[tuple[str, ...], RouteGroup]:
        """Return all static routes in the router.

        _In this context "static" routes do not refer to the `app.static()`
        method. Instead, they refer to routes that do not contain
        any path parameters._

        Returns:
            Dict[Tuple[str, ...], Route]: a dictionary of routes
        """
        return self.static_routes

    @property
    def routes_dynamic(self) -> dict[tuple[str, ...], RouteGroup]:
        """Return all dynamic routes in the router.

        _Dynamic routes are routes that contain path parameters._

        Returns:
            Dict[Tuple[str, ...], Route]: a dictionary of routes
        """
        return self.dynamic_routes

    @property
    def routes_regex(self) -> dict[tuple[str, ...], RouteGroup]:
        """Return all regex routes in the router.

        _Regex routes are routes that contain path parameters with regex
        expressions, or otherwise need regex to resolve._

        Returns:
            Dict[Tuple[str, ...], Route]: a dictionary of routes
        """
        return self.regex_routes

    def finalize(self, *args, **kwargs) -> None:
        """Finalize the router.

        Raises:
            OldmanServerException: if a route contains a parameter name that starts with "__" and is not in ALLOWED_LABELS
        """  # noqa: E501
        self._routing_requires_host = True
        super().finalize(*args, **kwargs)

        for route in self.dynamic_routes.values():
            if any(
                label.startswith("__") and label not in ALLOWED_LABELS
                for label in route.labels
            ):
                raise OldmanServerException(
                    f"Invalid route: {route}. Parameter names cannot use '__'."
                )

        self._routing_requires_host = any(route.requirements for route in self.routes)
        self._native_routes = None if self._routing_requires_host else {}

    def reset(self) -> None:
        self._routing_requires_host = True
        self._native_routes = None
        super().reset()

    def _normalize(self, uri: str, handler: RouteHandler) -> str:
        if "<" not in uri:
            return uri

        sig = signature(handler)
        mapping = {
            param.name: param.annotation.__name__.lower()
            for param in sig.parameters.values()
            if param.annotation in (str, int, float, UUID)
        }

        reconstruction = []
        for part in uri.split("/"):
            if part.startswith("<") and ":" not in part:
                name = part[1:-1]
                annotation = mapping.get(name)
                if annotation:
                    part = f"<{name}:{annotation}>"
            reconstruction.append(part)
        return "/".join(reconstruction)
