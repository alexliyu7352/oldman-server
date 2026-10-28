import typing

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosedOK


class WebsocketProxy:
    def __init__(self, ws):
        self.ws = ws
        self.opened = True
        self.client_received: typing.List[str] = []
        self.client_sent: typing.List[str] = []

    @property
    def server_received(self):
        return self.client_sent

    @property
    def server_sent(self):
        return self.client_received


async def websocket_proxy(url, *args, **kwargs) -> WebsocketProxy:
    """Connect to `url` and run mimic(ws), the client side of the test, with
    websockets' asyncio client: other keyword arguments go to its connect()
    (additional_headers=, subprotocols=, ...). After mimic returns, an empty
    message is sent, then the connection closes."""
    mimic = kwargs.pop("mimic", None)
    # The local test server, never through a proxy from the environment.
    kwargs.setdefault("proxy", None)
    async with connect(url, *args, **kwargs) as websocket:
        ws_proxy = WebsocketProxy(websocket)

        if mimic:
            do_send = websocket.send
            do_recv = websocket.recv

            async def send(data):
                ws_proxy.client_sent.append(data)
                await do_send(data)

            async def recv():
                message = await do_recv()
                ws_proxy.client_received.append(message)
                return message

            websocket.send = send  # type: ignore
            websocket.recv = recv  # type: ignore

            try:
                await mimic(websocket)
            except ConnectionClosedOK:
                pass
            else:
                await websocket.send("")
    return ws_proxy
