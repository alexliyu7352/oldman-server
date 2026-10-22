from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from oldman_server.models.protocol_types import TransportProtocol


class Signal:
    stopped = False


def _socket_address(address):
    """A native (host, port) as the socket module reports it: IPv6 as a 4-tuple.
    Unix socket addresses (str) are kept."""
    if isinstance(address, tuple) and ":" in address[0]:
        return (address[0], address[1], 0, 0)
    return address


class _ConnectionInfo:
    """Owned socket metadata for a native backend, not a socket transport."""

    __slots__ = ("client", "server", "protocol")

    def __init__(self, client, server, protocol=None):
        self.client, self.server = _socket_address(client), _socket_address(server)
        self.protocol = protocol

    def get_protocol(self):
        return self.protocol

    def get_extra_info(self, name, default=None):
        if name == "peername":
            return self.client
        if name == "sockname":
            return self.server
        if name == "network_paths":
            return []
        return default


class ConnInfo:
    """
    Local and remote addresses of a connection.
    """

    __slots__ = (
        "client_port",
        "client",
        "client_ip",
        "ctx",
        "lost",
        "peername",
        "server_port",
        "server",
        "sockname",
        "network_paths",
    )

    @classmethod
    def _from_addresses(cls, client, server):
        return cls(None, _addresses=(_socket_address(client), _socket_address(server)))

    def __init__(self, transport: TransportProtocol, unix=None, *, _addresses=None):
        self.ctx = SimpleNamespace()
        self.lost = False
        self.peername: tuple[str, int] | None = None
        self.server = self.client = ""
        self.server_port = self.client_port = 0
        self.client_ip = ""
        self.sockname = addr = (
            transport.get_extra_info("sockname")
            if _addresses is None else _addresses[1]
        )
        self.network_paths: list[Any] = []
        if isinstance(addr, str):  # UNIX socket
            self.server = unix or addr
            return
        # IPv4 (ip, port) or IPv6 (ip, port, flowinfo, scopeid)
        if isinstance(addr, tuple):
            self.server = addr[0] if len(addr) == 2 else f"[{addr[0]}]"
            self.server_port = addr[1]
            # self.server gets non-standard port appended
            if addr[1] != 80:
                self.server = f"{self.server}:{addr[1]}"
        self.peername = addr = (
            transport.get_extra_info("peername")
            if _addresses is None else _addresses[0]
        )
        if _addresses is None:
            self.network_paths = transport.get_extra_info("network_paths")  # type: ignore

        if isinstance(addr, tuple):
            self.client = addr[0] if len(addr) == 2 else f"[{addr[0]}]"
            self.client_ip = addr[0]
            self.client_port = addr[1]


class _NativeConnection:
    """What HttpProtocol keeps per connection, for requests of the native host:
    one transport and ConnInfo (with its ctx) and the protocol state."""

    __slots__ = ("closed", "conn_info", "state", "transport")

    def __init__(self, client, server, unix=False):
        if unix:
            # As asyncio reports a unix socket connection: an empty peer name
            # and the socket path, which ConnInfo shows as the server.
            client, server = "", server[0]
        self.transport = _ConnectionInfo(client, server, self)
        self.conn_info = ConnInfo._from_addresses(client, server)
        self.state = {"requests_count": 0}
        self.closed = False

    @property
    def version(self):
        from oldman_server.http.constants import HTTP  # sanic.http imports this module

        return HTTP.VERSION_1

    def connection_lost(self):
        self.closed = True
        self.conn_info.lost = True

    async def websocket_handshake(self, request, subprotocols=None):
        """WebSocketProtocol.websocket_handshake: the request's native stream
        switches the connection to a WebSocket."""
        return await request.stream.websocket_handshake(request, subprotocols)


# Open native connections by id, None until a request asks for their state. The
# native host registers each connection and removes it when it closes.
native_connections: dict[int, _NativeConnection | None] = {}


def native_connection(native) -> _NativeConnection:
    """The connection of a native request. One that already closed (or that the
    host does not track) gets state of its own; closed state is marked lost."""
    key = native.connection
    if key and key in native_connections:
        connection = native_connections[key]
        if connection is None:
            connection = native_connections[key] = _NativeConnection(
                native.client, native.server, native.unix_socket)
    else:
        connection = _NativeConnection(native.client, native.server, native.unix_socket)
        if key:
            connection.connection_lost()
    if native.sequence > connection.state["requests_count"]:
        connection.state["requests_count"] = native.sequence
    return connection
