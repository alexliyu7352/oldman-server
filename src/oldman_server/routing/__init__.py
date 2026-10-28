"""The router: path matching, parameter types and the generated lookup that
oldman_server.router.Router and the signal router build on.

Derived from sanic-routing 23.12.0 (MIT, LICENSE.sanic-routing) with imports
renamed to this package.
"""
from .group import RouteGroup
from .route import Route
from .router import BaseRouter


__version__ = "23.12.0"
__all__ = ("BaseRouter", "Route", "RouteGroup")
