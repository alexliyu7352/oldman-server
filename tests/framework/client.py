import asyncio

from textwrap import dedent
from typing import AnyStr


class RawClient:
    CRLF = b"\r\n"

    def __init__(self, host: str, port: int):
        self.reader = None
        self.writer = None
        self.host = host
        self.port = port

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(
            self.host, self.port
        )

    async def close(self):
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except ConnectionResetError:
            pass  # the server already reset it (see recv)

    async def send(self, message: AnyStr):
        if isinstance(message, str):
            msg = self._clean(message).encode("utf-8")
        else:
            msg = message
        await self._send(msg)

    async def _send(self, message: bytes):
        if not self.writer:
            raise Exception("No open write stream")
        self.writer.write(message)

    async def recv(self, nbytes: int = -1) -> bytes:
        if not self.reader:
            raise Exception("No open read stream")
        if nbytes != -1:
            return await self.reader.read(nbytes)
        # To the end: a server that answers and closes while this side still
        # sends resets the connection, and StreamReader then raises before
        # handing over what arrived ahead of the reset: the answer.
        data = b""
        while True:
            try:
                block = await self.reader.read(65536)
            except ConnectionResetError:
                return data + bytes(self.reader._buffer)
            if not block:
                return data
            data += block

    def _clean(self, message: str) -> str:
        return (
            dedent(message)
            .lstrip("\n")
            .replace("\n", self.CRLF.decode("utf-8"))
        )
