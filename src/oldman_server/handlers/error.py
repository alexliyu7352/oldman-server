from __future__ import annotations

from inspect import isawaitable

from oldman_server.errorpages import BaseRenderer, TextRenderer, exception_response
from oldman_server.exceptions import OldmanServerException, ServerError
from oldman_server.log import error_logger
from oldman_server.models.handler_types import RouteHandler
from oldman_server.request.types import Request
from oldman_server.response import text
from oldman_server.response.types import HTTPResponse


class ErrorHandler:
    """Process and handle all uncaught exceptions.

    This error handling framework is built into the core that can be extended
    by the developers to perform a wide range of tasks from recording the error
    stats to reporting them to an external service that can be used for
    realtime alerting system.

    Args:
        base (BaseRenderer): The renderer to use for the error pages.
    """  # noqa: E501

    def __init__(
        self,
        base: type[BaseRenderer] = TextRenderer,
    ):
        self.cached_handlers: dict[
            tuple[type[BaseException], str | None], RouteHandler | None
        ] = {}
        self.debug = False
        self.base = base

    def _full_lookup(self, exception, route_name: str | None = None):
        return self.lookup(exception, route_name)

    def _add(
        self,
        key: tuple[type[BaseException], str | None],
        handler: RouteHandler,
    ) -> None:
        if key in self.cached_handlers:
            exc, name = key
            if name is None:
                name = "__ALL_ROUTES__"

            message = (
                f"Duplicate exception handler definition on: route={name} "
                f"and exception={exc}"
            )
            raise ServerError(message)
        self.cached_handlers[key] = handler

    def add(self, exception, handler, route_names: list[str] | None = None):
        """Add a new exception handler to an already existing handler object.

        Args:
            exception (sanic.exceptions.OldmanServerException or Exception): Type
                of exception that needs to be handled.
            handler (function): Reference to the function that will
                handle the exception.

        Returns:
            None

        """  # noqa: E501
        if route_names:
            for route in route_names:
                self._add((exception, route), handler)
        else:
            self._add((exception, None), handler)

    def lookup(self, exception, route_name: str | None = None):
        """Lookup the existing instance of `ErrorHandler` and fetch the registered handler for a specific type of exception.

        This method leverages a dict lookup to speedup the retrieval process.

        Args:
            exception (sanic.exceptions.OldmanServerException or Exception): Type
                of exception.

        Returns:
            Registered function if found, ``None`` otherwise.

        """  # noqa: E501
        exception_class = type(exception)

        for name in (route_name, None):
            exception_key = (exception_class, name)
            handler = self.cached_handlers.get(exception_key)
            if handler:
                return handler

        for name in (route_name, None):
            for ancestor in type.mro(exception_class):
                exception_key = (ancestor, name)
                if exception_key in self.cached_handlers:
                    handler = self.cached_handlers[exception_key]
                    self.cached_handlers[(exception_class, route_name)] = (
                        handler
                    )
                    return handler

                if ancestor is BaseException:
                    break
        self.cached_handlers[(exception_class, route_name)] = None
        handler = None
        return handler

    _lookup = _full_lookup

    def response(self, request, exception):
        """Fetch and executes an exception handler and returns a response object.

        One contract, whether the handler and `default` are plain functions or
        coroutines: the handler's response; on None, the response the handler
        sent itself (`request.respond()`) or else `default(request,
        exception)`; when the handler raises a framework exception,
        `default(request, that exception)`; any other failure, of the handler
        or of `default`, is logged and answered with a 500. When the handler
        or `default` returns an awaitable, this returns a coroutine that
        applies the same rules once awaited.

        Args:
            request (sanic.request.Request): Instance of the request.
            exception (sanic.exceptions.OldmanServerException or Exception): Exception to handle.

        Returns:
            Wrap the return value obtained from the `default` function or the registered handler for that type of exception.

        """  # noqa: E501
        route_name = request.name if request else None
        handler = self._lookup(exception, route_name)
        if handler is None:
            return self._default(request, exception)
        before = _sent(request)
        try:
            response = handler(request, exception)
        except OldmanServerException as error:
            return self._default(request, error)
        except Exception:
            return self._failed(request, _name(handler))
        if isawaitable(response):
            return self._settle(request, exception, handler, response, before)
        if response is None and _sent(request) is before:
            return self._default(request, exception)
        return response

    async def _settle(self, request, exception, handler, pending, before):
        """response()'s rules for a coroutine handler, once it has finished."""
        try:
            response = await pending
        except OldmanServerException as error:
            response = self._default(request, error)
        except Exception:
            return self._failed(request, _name(handler))
        else:
            if response is None and _sent(request) is before:
                response = self._default(request, exception)
        if isawaitable(response):
            # A coroutine default(), or one the handler returned unawaited.
            response = await response
        return response

    def _default(self, request, exception):
        """default(request, exception); its failure answered with a 500."""
        try:
            response = self.default(request, exception)
        except Exception:
            return self._failed(request, "default")
        if isawaitable(response):
            return self._settle_default(request, response)
        return response

    async def _settle_default(self, request, pending):
        try:
            return await pending
        except Exception:
            return self._failed(request, "default")

    def _failed(self, request, name):
        """The 500 for a handler (or default) that failed: logged, with the
        reason only in debug mode."""
        try:
            url = repr(request.url)
        except AttributeError:  # no cov
            url = "unknown"
        response_message = 'Exception raised in exception handler "%s" for uri: %s'
        error_logger.exception(response_message, name, url)
        if self.debug:
            return text(response_message % (name, url), 500)
        return text("An error occurred while handling an error", 500)

    def default(self, request: Request, exception: Exception) -> HTTPResponse:
        """Provide a default behavior for the objects of ErrorHandler.

        If a developer chooses to extend the ErrorHandler, they can
        provide a custom implementation for this method to behave in a way
        they see fit.

        Args:
            request (sanic.request.Request): Incoming request.
            exception (sanic.exceptions.OldmanServerException or Exception): Exception object.

        Returns:
            HTTPResponse: The response object.

        Examples:
            ```python
            class CustomErrorHandler(ErrorHandler):
                def default(self, request: Request, exception: Exception) -> HTTPResponse:
                    # Custom logic for handling the exception and creating a response
                    custom_response = my_custom_logic(request, exception)
                    return custom_response

            app = OldmanServer("MyApp", error_handler=CustomErrorHandler())
            ```
        """  # noqa: E501
        self.log(request, exception)
        fallback = request.app.config.FALLBACK_ERROR_FORMAT
        return exception_response(
            request,
            exception,
            debug=self.debug,
            base=self.base,
            fallback=fallback,
        )

    @staticmethod
    def log(request: Request, exception: Exception) -> None:
        """Logs information about an incoming request and the associated exception.

        Args:
            request (Request): The incoming request to be logged.
            exception (Exception): The exception that occurred during the handling of the request.

        Returns:
            None
        """  # noqa: E501
        quiet = getattr(exception, "quiet", False)
        noisy = getattr(request.app.config, "NOISY_EXCEPTIONS", False)
        if quiet is False or noisy is True:
            try:
                url = repr(request.url)
            except AttributeError:  # no cov
                url = "unknown"

            error_logger.exception(
                "Exception occurred while handling uri: %s", url
            )


def _name(handler) -> str:
    """The handler's name for the log (a partial or callable object has none)."""
    return getattr(handler, "__name__", None) or repr(handler)


def _sent(request):
    """The response connected to the request's stream: a handler that
    answers with request.respond() changes it (and then returns None)."""
    return getattr(getattr(request, "stream", None), "response", None)
