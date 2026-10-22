from itertools import count

from oldman_server import Request, OldmanServer
from oldman_server.compat import Header
from oldman_server.models.protocol_types import TransportProtocol


class CountedRequest(Request):
    __slots__ = ()

    _counter = count()
    count = next(_counter)

    def __init__(
        self,
        url_bytes: bytes,
        headers: Header,
        version: str,
        method: str,
        transport: TransportProtocol,
        app: OldmanServer,
        head: bytes = b"",
    ):
        super().__init__(
            url_bytes,
            headers,
            version,
            method,
            transport,
            app,
            head,
        )
        self.__class__._increment()
        if hasattr(self.app, "multiplexer"):
            self.app.multiplexer.state["request_count"] = self.__class__.count

    @classmethod
    def _increment(cls):
        cls.count = next(cls._counter)

    @classmethod
    def reset_count(cls):
        cls._counter = count()
        cls.count = next(cls._counter)
