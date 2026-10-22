import typing
from functools import partial
from ipaddress import IPv6Address, ip_address
from json import JSONDecodeError
from socket import AF_INET6, SOCK_STREAM, socket
from string import ascii_lowercase

import httpx
from oldman_server import OldmanServer  # type: ignore
from oldman_server.exceptions import MethodNotSupported, ServerError  # type: ignore
from oldman_server.log import logger  # type: ignore
from oldman_server.request import Request  # type: ignore
from oldman_server.response import text  # type: ignore

from oldman_server.testing.websocket import websocket_proxy

HOST = "127.0.0.1"
PORT = None


httpx_version = tuple(
    map(int, httpx.__version__.strip(ascii_lowercase).split("."))
)


class TestingResponse(httpx.Response):
    @property
    def status(self):
        return self.status_code

    @property
    def body(self):
        return self.content

    @property
    def content_type(self):
        return self.headers.get("content-type")

    @property
    def json(self):
        if getattr(self, "_json", None):
            return self._json
        try:
            self._json = super().json()
        except (JSONDecodeError, UnicodeDecodeError):
            self._json = None

        return self._json


def _blank(*_, **__):
    ...


class TestClient:
    # Not a test class, although its name says so (pytest collects Test*).
    __test__ = False

    def __init__(
        self, app: OldmanServer, port: typing.Optional[int] = PORT, host: str = HOST
    ) -> None:
        """Use port=None to bind to a random port"""
        OldmanServer.test_mode = True
        self.app = app
        self.port = port
        self.host = host
        self._do_request = _blank
        app.after_server_start(self._run_request)

    def _run_request(self, *args, **kwargs):
        return self._do_request(*args, **kwargs)

    @classmethod
    def _start_test_mode(cls, sanic, *args, **kwargs):
        OldmanServer.test_mode = True

    @classmethod
    def _end_test_mode(cls, sanic, *args, **kwargs):
        OldmanServer.test_mode = False

    def get_new_session(self, **kwargs) -> httpx.AsyncClient:
        return httpx.AsyncClient(verify=False, **kwargs)

    async def _local_request(self, method: str, url: str, *args, **kwargs):
        logger.info(url)
        raw_cookies = kwargs.pop("raw_cookies", None)
        session_kwargs = kwargs.pop("session_kwargs", {})
        if httpx_version >= (0, 20) and method != "websocket":
            kwargs["follow_redirects"] = True
            allow_redirects = kwargs.pop("allow_redirects", None)
            if allow_redirects is not None:
                kwargs["follow_redirects"] = allow_redirects

        if method == "websocket":
            return await websocket_proxy(url, *args, **kwargs)
        else:
            async with self.get_new_session(**session_kwargs) as session:
                try:
                    if method == "request":
                        args = tuple([url] + list(args))
                        url = kwargs.pop("http_method", "GET").upper()
                    response = await getattr(session, method.lower())(
                        url, *args, **kwargs
                    )
                except httpx.HTTPError as e:
                    if hasattr(e, "response"):
                        response = getattr(e, "response")
                    else:
                        logger.error(
                            f"{method.upper()} {url} received no response!",
                            exc_info=True,
                        )
                        return None

                response.__class__ = TestingResponse

                if raw_cookies:
                    response.raw_cookies = {}

                    for cookie in response.cookies.jar:
                        response.raw_cookies[cookie.name] = cookie

            return response

    @classmethod
    def _collect_request(cls, results, request):
        if results[0] is None:
            results[0] = request

    async def _collect_response(
        self,
        method,
        url,
        exceptions,
        results,
        sanic,
        loop,
        **request_kwargs,
    ):
        try:
            response = await self._local_request(method, url, **request_kwargs)
            results[-1] = response
            if method == "websocket":
                await response.ws.close()
        except Exception as e:
            logger.exception("Exception")
            exceptions.append(e)
        finally:
            self.app.stop()

    async def _error_handler(self, request, exception):
        if request.method in ["HEAD", "PATCH", "PUT", "DELETE"]:
            return text("", exception.status_code, headers=exception.headers)
        else:
            return self.app.error_handler.default(request, exception)

    def _sanic_endpoint_test(
        self,
        method: str = "get",
        uri: str = "/",
        gather_request: bool = True,
        debug: bool = False,
        server_kwargs: typing.Optional[typing.Dict[str, typing.Any]] = None,
        host: typing.Optional[str] = None,
        allow_none: bool = False,
        *request_args,
        **request_kwargs,
    ) -> typing.Tuple[
        typing.Optional[Request], typing.Optional[TestingResponse]
    ]:
        results = [None, None]
        exceptions: typing.List[Exception] = []

        server_kwargs = server_kwargs or {"auto_reload": False}
        _collect_request = partial(self._collect_request, results)

        self.app.router.reset()
        self.app.signal_router.reset()

        if gather_request:
            self.app.request_middleware.appendleft(_collect_request)  # type: ignore  # noqa

        try:
            self.app.exception(MethodNotSupported)(self._error_handler)
        except ServerError:
            ...

        if self.port:
            server_kwargs = dict(
                host=host or self.host,
                port=self.port,
                **server_kwargs,
            )
            host, port = host or self.host, self.port
        else:
            bind = host or self.host
            ip = ip_address(bind)
            if isinstance(ip, IPv6Address):
                sock = socket(AF_INET6, SOCK_STREAM)
                port = 0
            else:
                sock = socket()
                port = 0
            sock.bind((bind, port))
            server_kwargs = dict(sock=sock, **server_kwargs)

            if isinstance(ip, IPv6Address):
                host, port, _, _ = sock.getsockname()
                host = f"[{host}]"
            else:
                host, port = sock.getsockname()
            self.port = port

        if uri.startswith(
            ("http:", "https:", "ftp:", "ftps://", "//", "ws:", "wss:")
        ):
            url = uri
        else:
            uri = uri if uri.startswith("/") else f"/{uri}"
            scheme = "ws" if method == "websocket" else "http"
            url = f"{scheme}://{host}:{port}{uri}"
        # Tests construct URLs using PORT = None, which means random port not
        # known until this function is called, so fix that here
        url = url.replace(":None/", f":{port}/")

        self._do_request = partial(
            self._collect_response,
            method,
            url,
            exceptions,
            results,
            **request_kwargs,
        )

        self.app.run(  # type: ignore
            debug=debug,
            single_process=True,
            **server_kwargs,
        )

        if exceptions:
            raise ValueError(f"Exception during request: {exceptions}")

        if gather_request:
            try:
                self.app.request_middleware.remove(_collect_request)  # type: ignore  # noqa
            except BaseException:  # noqa
                pass

            try:
                request, response = results
                if response is None:
                    if not allow_none:
                        raise ValueError(
                            "No response returned to the test client."
                        )
                return request, response
            except BaseException:  # noqa
                if not allow_none:
                    raise ValueError(
                        "Request and response object expected, "
                        f"got ({results})"
                    )
                return None, None
        else:
            try:
                if results[-1] is None:
                    if not allow_none:
                        raise ValueError(
                            "No response returned to the test client."
                        )
                return None, results[-1]
            except BaseException:  # noqa
                if not allow_none:
                    raise ValueError(
                        f"Request object expected, got ({results})"
                    )
                return None, None

    def request(self, *args, **kwargs):
        return self._sanic_endpoint_test("request", *args, **kwargs)

    def get(self, *args, **kwargs):
        return self._sanic_endpoint_test("get", *args, **kwargs)

    def post(self, *args, **kwargs):
        return self._sanic_endpoint_test("post", *args, **kwargs)

    def put(self, *args, **kwargs):
        return self._sanic_endpoint_test("put", *args, **kwargs)

    def delete(self, *args, **kwargs):
        return self._sanic_endpoint_test("delete", *args, **kwargs)

    def patch(self, *args, **kwargs):
        return self._sanic_endpoint_test("patch", *args, **kwargs)

    def options(self, *args, **kwargs):
        return self._sanic_endpoint_test("options", *args, **kwargs)

    def head(self, *args, **kwargs):
        return self._sanic_endpoint_test("head", *args, **kwargs)

    def websocket(
        self,
        *args,
        mimic: typing.Optional[
            typing.Callable[..., typing.Coroutine[None, None, typing.Any]]
        ] = None,
        **kwargs,
    ):
        kwargs["mimic"] = mimic
        return self._sanic_endpoint_test("websocket", *args, **kwargs)
