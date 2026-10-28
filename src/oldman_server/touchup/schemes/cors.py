from ast import (
    And,
    Attribute,
    BoolOp,
    Compare,
    If,
    Name,
    NodeTransformer,
    Pass,
    Try,
    copy_location,
)
from typing import Any

from .base import BaseScheme


class OptionalCors(BaseScheme):
    """CORS code on the request path is compiled in only where it applies
    (oldman_server.cors): an ``if`` whose test starts with ``._cors_table``
    stays while some application has CORS on, one that starts with
    ``._cors_codes`` (and the ``_Answered`` handler) while this application
    has routes with policies of their own. Otherwise they are removed, so an
    application without CORS runs no CORS check per request."""

    ident = "CORS"

    def __init__(self, app) -> None:
        super().__init__(app)
        table = app._cors_table
        self._route_policies = table is not None and table.route_policies
        # Methods compiled once per class (OldmanServer.handle_request,
        # Http.http1_response_header) serve every application of the process.
        self._enabled = any(
            other._cors_table is not None
            for other in app.__class__._app_registry.values()
        ) or table is not None

    def visitors(self) -> list[NodeTransformer]:
        return [RemoveCors(self._enabled, self._route_policies)]


class RemoveCors(NodeTransformer):
    def __init__(self, enabled: bool, route_policies: bool) -> None:
        self._kept = {"_cors_table": enabled, "_cors_codes": route_policies}

    @staticmethod
    def _marker(test) -> str | None:
        if isinstance(test, BoolOp) and isinstance(test.op, And):
            test = test.values[0]
        if isinstance(test, Compare):
            test = test.left
        if isinstance(test, Attribute) and test.attr in ("_cors_table", "_cors_codes"):
            return test.attr
        return None

    def visit_If(self, node: If) -> Any:
        self.generic_visit(node)
        marker = self._marker(node.test)
        if marker is not None and not self._kept[marker]:
            # The else branch, or a pass keeping the enclosing block valid.
            return node.orelse or copy_location(Pass(), node)
        return node

    def visit_Try(self, node: Try) -> Any:
        self.generic_visit(node)
        if not self._kept["_cors_codes"]:
            node.handlers = [
                handler for handler in node.handlers
                if not (isinstance(handler.type, Name) and handler.type.id == "_Answered")
            ]
        return node
