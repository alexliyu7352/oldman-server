from __future__ import annotations

from inspect import isawaitable
from typing import TYPE_CHECKING, Any, Optional, Union

from oldman_server import OldmanServer
from oldman_server.compat import Header
from oldman_server.exceptions import OldmanServerException
from oldman_server.request import Request
from oldman_server.response import HTTPResponse

from oldman_server.ext.exceptions import ExtensionNotFound


if TYPE_CHECKING:
    from jinja2 import Environment


class TemplateResponse(HTTPResponse): ...


class LazyResponse(TemplateResponse):
    __slots__ = (
        "body",
        "status",
        "content_type",
        "headers",
        "_cookies",
        "context",
    )

    def __init__(
        self,
        context: dict[str, Any],
        status: int = 0,
        headers: Optional[Union[Header, dict[str, str]]] = None,
        content_type: Optional[str] = None,
    ):
        super().__init__(
            content_type=content_type, status=status, headers=headers
        )
        self.context = context


async def render(
    template_name: str = "",
    status: int = 200,
    headers: Optional[dict[str, str]] = None,
    content_type: str = "text/html; charset=utf-8",
    app: Optional[OldmanServer] = None,
    environment: Optional[Environment] = None,
    context: Optional[dict[str, Any]] = None,
    *,
    template_source: str = "",
) -> TemplateResponse:
    if app is None:
        try:
            app = OldmanServer.get_app()
        except OldmanServerException as e:
            raise OldmanServerException(
                "Cannot render template beause locating the application "
                "was ambiguous. Please return  render(..., app=some_app)."
            ) from e

    if template_name and template_source:
        raise OldmanServerException(
            "You must provide template_name OR template_source, not both."
        )

    if environment is None:
        try:
            environment = app.ext.environment
        except AttributeError:
            raise ExtensionNotFound(
                "The Templating extension does not appear to be enabled. "
                "Perhaps jinja2 is not installed."
            )

    kwargs = context if context else {}

    kwargs["request"] = Request.get_current()

    if template_name or template_source:
        template = (
            environment.get_template(template_name)
            if template_name
            else environment.from_string(template_source)
        )

        render = (
            template.render_async
            if app.config.TEMPLATING_ENABLE_ASYNC
            else template.render
        )
        content = render(**kwargs)
        if isawaitable(content):
            content = await content  # type: ignore

        return TemplateResponse(  # type: ignore
            content, status=status, headers=headers, content_type=content_type
        )
    else:
        return LazyResponse(
            kwargs, status=status, headers=headers, content_type=content_type
        )
