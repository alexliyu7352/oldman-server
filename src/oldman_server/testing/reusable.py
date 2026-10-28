import asyncio
import typing
from functools import partial
from typing import Any, Dict, Optional, Tuple

import httpx
from oldman_server import OldmanServer
from oldman_server.application.state import ApplicationServerInfo
from oldman_server.request import Request

from .testing import (
    HOST,
    PORT,
    Result,
    _endpoint_result,
    bind_test_socket,
    endpoint_url,
    send_request,
)
from .websocket import WebsocketProxy


class ReusableClient:
    """Requests to one server that stays up between them (with ... as client).

    The methods, their arguments and return values are those of
    app.test_client. port=None binds a port the system picks.
    """

    def __init__(
        self,
        app: OldmanServer,
        host=HOST,
        port=PORT,
        loop=None,
        server_kwargs=None,
        client_kwargs=None,
    ):
        if not loop:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        server_kwargs = server_kwargs or {}
        client_kwargs = client_kwargs or {}

        OldmanServer.test_mode = True
        self.app = app
        self.host = host
        sock, self._url_host, self.port = bind_test_socket(host, port)
        self._loop = loop
        self.debug = False
        self._server = None
        # The address this server serves, for as long as it does: stop()
        # removes it, so a later run of the app does not take it for one of
        # its own.
        self._server_info = ApplicationServerInfo(
            settings={
                "unix": None,
                "sock": None,
                "loop": None,
                "host": self.host,
                "port": self.port,
            }
        )
        self.app.state.server_info.append(self._server_info)

        self._session = httpx.AsyncClient(verify=False, **client_kwargs)
        self._server_co = self.app.create_server(
            sock=sock,
            debug=self.debug,
            return_asyncio_server=True,
            **server_kwargs,
        )

    def __enter__(self):
        self.run()
        return self

    def __exit__(self, *_):
        self.stop()

    def run(self):
        self._loop._stopping = False
        self.app.router.reset()
        self.app.signal_router.reset()
        self._run(self.app._startup())
        self._run(self.app._server_event("init", "before", loop=self._loop))
        self._server = self._run(self._server_co)
        self._run(self.app._server_event("init", "after", loop=self._loop))

    def stop(self):
        self._run(
            self.app._server_event("shutdown", "before", loop=self._loop)
        )
        if self._session:
            self._run(self._session.aclose())
            self._session = None

        if self._server:
            self._server.close()
            self._run(self._server.wait_closed())
            self._server = None

        self._run(self.app._server_event("shutdown", "after", loop=self._loop))
        if self._server_info in self.app.state.server_info:
            self.app.state.server_info.remove(self._server_info)

    def _sanic_endpoint_test(
        self,
        method: str = "get",
        uri: str = "/",
        gather_request: bool = True,
        debug: bool = False,
        server_kwargs: Optional[Dict[str, Any]] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        *request_args,
        **request_kwargs,
    ) -> Tuple[Optional[Request], Any]:
        results: list = [None, None]
        if host or port:
            url_host = host or self.host
            url = endpoint_url(uri, method, url_host, port or self.port)
        else:
            url = endpoint_url(uri, method, self._url_host, self.port)

        if gather_request:
            _collect_request = partial(self._collect_request, results)
            self.app.request_middleware.appendleft(_collect_request)  # type: ignore  # noqa
            for route in self.app.router.routes:
                if _collect_request not in route.extra.request_middleware:
                    route.extra.request_middleware.appendleft(_collect_request)
        try:
            # Exceptions on the way propagate as raised: the request runs here.
            results[-1] = self._run(
                self._local_request(method, url, *request_args, **request_kwargs)
            )
        finally:
            if gather_request:
                for middleware in (
                    self.app.request_middleware,
                    *(route.extra.request_middleware for route in self.app.router.routes),
                ):
                    try:
                        middleware.remove(_collect_request)  # type: ignore
                    except ValueError:
                        pass
        return _endpoint_result(results, gather_request)

    async def _local_request(self, method, url, *args, **kwargs):
        return await send_request(self._session, method, url, *args, **kwargs)

    def _run(self, coro):
        if not self._loop:
            raise RuntimeError("Test client has no loop")
        return self._loop.run_until_complete(coro)

    @staticmethod
    def _collect_request(results, request):
        if results[0] is None:
            results[0] = request

    def request(self, *args, **kwargs) -> Result:
        return self._sanic_endpoint_test("request", *args, **kwargs)

    def get(self, *args, **kwargs) -> Result:
        return self._sanic_endpoint_test("get", *args, **kwargs)

    def post(self, *args, **kwargs) -> Result:
        return self._sanic_endpoint_test("post", *args, **kwargs)

    def put(self, *args, **kwargs) -> Result:
        return self._sanic_endpoint_test("put", *args, **kwargs)

    def delete(self, *args, **kwargs) -> Result:
        return self._sanic_endpoint_test("delete", *args, **kwargs)

    def patch(self, *args, **kwargs) -> Result:
        return self._sanic_endpoint_test("patch", *args, **kwargs)

    def options(self, *args, **kwargs) -> Result:
        return self._sanic_endpoint_test("options", *args, **kwargs)

    def head(self, *args, **kwargs) -> Result:
        return self._sanic_endpoint_test("head", *args, **kwargs)

    def websocket(
        self,
        *args,
        mimic: typing.Optional[
            typing.Callable[..., typing.Coroutine[None, None, typing.Any]]
        ] = None,
        **kwargs,
    ) -> Tuple[Optional[Request], WebsocketProxy]:
        kwargs["mimic"] = mimic
        return self._sanic_endpoint_test("websocket", *args, **kwargs)
