import typing
from functools import partial
from ipaddress import IPv6Address, ip_address
from json import JSONDecodeError
from socket import AF_INET, AF_INET6, SO_REUSEADDR, SOCK_STREAM, SOL_SOCKET, socket

import httpx
from oldman_server import OldmanServer  # type: ignore
from oldman_server.log import logger  # type: ignore
from oldman_server.mixins.startup import RUN_CONFIG_KEYS  # type: ignore
from oldman_server.request import Request  # type: ignore

from oldman_server.testing.websocket import WebsocketProxy, websocket_proxy

HOST = "127.0.0.1"
PORT = None


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


def bind_test_socket(host: str, port: typing.Optional[int] = None):
    """A socket bound to `host` and `port` (None: one the system picks), with
    SO_REUSEADDR as the servers' own bind_socket: (socket, host for URLs,
    port bound)."""
    ip = ip_address(host)
    sock = socket(AF_INET6 if isinstance(ip, IPv6Address) else AF_INET, SOCK_STREAM)
    sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    sock.bind((host, port or 0))
    bound = sock.getsockname()[1]
    return sock, (f"[{host}]" if isinstance(ip, IPv6Address) else host), bound


def endpoint_url(uri: str, method: str, host: str, port: int) -> str:
    """The URL a test client requests for `uri` (a path, or a full URL)."""
    if uri.startswith(("http:", "https:", "ftp:", "ftps://", "//", "ws:", "wss:")):
        url = uri
    else:
        uri = uri if uri.startswith("/") else f"/{uri}"
        scheme = "ws" if method == "websocket" else "http"
        url = f"{scheme}://{host}:{port}{uri}"
    # Tests build URLs with PORT = None, the random port not known until the
    # server binds it.
    return url.replace(":None/", f":{port}/")


def follow_redirects(kwargs: typing.Dict[str, typing.Any]) -> None:
    """Turn a call's redirect options into httpx's follow_redirects (in
    `kwargs`): redirects are followed unless follow_redirects=False or its
    alias allow_redirects=False says otherwise; the two disagreeing is a
    TypeError. Repeating it changes nothing."""
    allow = kwargs.pop("allow_redirects", None)
    follow = kwargs.pop("follow_redirects", None)
    if allow is not None and follow is not None and bool(allow) != bool(follow):
        raise TypeError("allow_redirects and follow_redirects disagree")
    kwargs["follow_redirects"] = next((v for v in (follow, allow) if v is not None), True)


async def send_request(session: httpx.AsyncClient, method: str, url: str, *args, **kwargs):
    """One request the way every test client makes it: a TestingResponse,
    never None; when no response comes (the connection fails, say), httpx's
    error is raised. Redirects are followed unless follow_redirects=False
    (httpx's name) or allow_redirects=False (its alias) says otherwise;
    method "request" takes the HTTP method as http_method=, and "websocket"
    opens a WebSocket (see websocket_proxy)."""
    if method == "websocket":
        return await websocket_proxy(url, *args, **kwargs)
    raw_cookies = kwargs.pop("raw_cookies", None)
    follow_redirects(kwargs)
    if method == "request":
        args = (url, *args)
        url = kwargs.pop("http_method", "GET").upper()
    try:
        response = await getattr(session, method.lower())(url, *args, **kwargs)
    except httpx.HTTPError as e:
        response = getattr(e, "response", None)
        if response is None:
            logger.error(f"{method.upper()} {url} received no response!")
            raise

    response.__class__ = TestingResponse

    if raw_cookies:
        response.raw_cookies = {}
        for cookie in response.cookies.jar:
            response.raw_cookies[cookie.name] = cookie

    return response


# What a test client call returns: the request the server received first
# (None with gather_request=False, or when the server received none) and the
# response, which is never None: a call that gets no response raises.
Result = typing.Tuple[typing.Optional[Request], TestingResponse]


def _endpoint_result(results, gather_request) -> typing.Tuple[typing.Optional[Request], typing.Any]:
    """(request, response) of a call; a call that got no response raises."""
    request, response = results
    if response is None:
        raise ValueError("No response returned to the test client.")
    return (request if gather_request else None), response


class TestClient:
    # Not a test class, although its name says so (pytest collects Test*).
    __test__ = False

    def __init__(
        self, app: OldmanServer, port: typing.Optional[int] = PORT, host: str = HOST
    ) -> None:
        """port=None binds a port the system picks, a fresh one on every call;
        `port` reports the one the last call used."""
        OldmanServer.test_mode = True
        self.app = app
        # The configured port (None: the system's choice on every call), kept
        # apart from the port the last call used, which `port` reports.
        self._port = port
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
        session_kwargs = kwargs.pop("session_kwargs", {})
        if method == "websocket":
            return await send_request(None, method, url, *args, **kwargs)
        async with self.get_new_session(**session_kwargs) as session:
            return await send_request(session, method, url, *args, **kwargs)

    @classmethod
    def _collect_request(cls, results, request):
        if results[0] is None:
            results[0] = request

    async def _collect_response(self, method, url, exceptions, results, app, **request_kwargs):
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

    def _sanic_endpoint_test(
        self,
        method: str = "get",
        uri: str = "/",
        gather_request: bool = True,
        debug: bool = False,
        server_kwargs: typing.Optional[typing.Dict[str, typing.Any]] = None,
        host: typing.Optional[str] = None,
        *request_args,
        **request_kwargs,
    ) -> typing.Tuple[typing.Optional[Request], typing.Any]:
        if method != "websocket":
            # A mistake in the options raises here, not inside the server.
            follow_redirects(request_kwargs)
        results = [None, None]
        exceptions: typing.List[Exception] = []

        server_kwargs = server_kwargs or {"auto_reload": False}
        _collect_request = partial(self._collect_request, results)

        self.app.router.reset()
        self.app.signal_router.reset()

        if gather_request:
            self.app.request_middleware.appendleft(_collect_request)  # type: ignore  # noqa

        host = host or self.host
        if self._port:
            server_kwargs = dict(host=host, port=self._port, **server_kwargs)
            url_host = f"[{host}]" if isinstance(ip_address(host), IPv6Address) else host
            port = self._port
        else:
            # A port the system picks, every call: the last call's port may
            # still be in TIME_WAIT from a connection the server closed.
            sock, url_host, port = bind_test_socket(host)
            server_kwargs = dict(sock=sock, **server_kwargs)
        self.port = port

        url = endpoint_url(uri, method, url_host, port)
        self._do_request = partial(
            self._collect_response,
            method,
            url,
            exceptions,
            results,
            **request_kwargs,
        )

        # A call runs the app once; the settings prepare() writes from run()'s
        # arguments (access_log on in debug mode, say) are this call's only.
        settings = {key: self.app.config.get(key) for key in RUN_CONFIG_KEYS}
        try:
            self.app.run(  # type: ignore
                debug=debug,
                single_process=True,
                **server_kwargs,
            )
        finally:
            self._do_request = _blank
            for key, value in settings.items():
                setattr(self.app.config, key, value)
            if gather_request:
                try:
                    self.app.request_middleware.remove(_collect_request)  # type: ignore  # noqa
                except ValueError:
                    pass

        if exceptions:
            raise ValueError(f"Exception during request: {exceptions}")
        return _endpoint_result(results, gather_request)

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
    ) -> typing.Tuple[typing.Optional[Request], WebsocketProxy]:
        kwargs["mimic"] = mimic
        return self._sanic_endpoint_test("websocket", *args, **kwargs)
