import asyncio
import codecs

from collections import deque
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from websockets.frames import Frame, Opcode
from websockets.typing import Data

from oldman_server.exceptions import ServerError


if TYPE_CHECKING:
    from .impl import WebsocketImplProtocol

UTF8Decoder = codecs.getincrementaldecoder("utf-8")

DATA_OPCODES = (Opcode.TEXT, Opcode.BINARY, Opcode.CONT)


class WebsocketFrameAssembler:
    """
    Assemble messages from the frames received, in order, and end the stream
    after them.

    put() queues each data frame as the protocol receives it; get() returns
    the next complete message; close() ends the stream after the queued
    frames, so get() raises EOFError once the queued messages are taken.
    get() can be cancelled (a timeout, say) at any point: a message leaves the
    queue only when get() returns it. Reading from the peer pauses while more
    than ``high`` frames wait and resumes once ``low`` or fewer do.

    After the websockets project's own asyncio assembler:
    https://github.com/python-websockets/websockets/blob/main/src/websockets/asyncio/messages.py
    """

    __slots__ = (
        "protocol",
        "frames",
        "complete",
        "waiter",
        "get_in_progress",
        "closed",
        "high",
        "low",
        "paused",
    )
    if TYPE_CHECKING:
        protocol: "WebsocketImplProtocol"
        frames: deque[Frame]
        complete: int
        waiter: asyncio.Future | None
        get_in_progress: bool
        closed: bool
        high: int
        low: int
        paused: bool

    def __init__(self, protocol, high: int = 16, low: int | None = None) -> None:
        self.protocol = protocol
        # Data frames received and not yet taken, and how many of the messages
        # they hold are complete (their last frame is queued).
        self.frames = deque()
        self.complete = 0
        # The reader waiting for a frame or the end, if any.
        self.waiter = None
        # This flag prevents concurrent calls to get() and get_iter().
        self.get_in_progress = False
        # The end of the stream: no frame comes after the queued ones.
        self.closed = False
        self.high = high
        self.low = high // 4 if low is None else low
        self.paused = False

    def __len__(self) -> int:
        return len(self.frames)

    def reaches_reader(self) -> bool:
        """Whether the end of the stream reaches the reader through get(): it
        is waiting for a message, or messages it has not taken are queued."""
        return self.get_in_progress or bool(self.frames)

    async def get(self, timeout: float | None = None) -> Data | None:
        """
        Read the next message.
        :meth:`get` returns a single :class:`str` or :class:`bytes`.
        If the message was fragmented, :meth:`get` waits until the last frame
        is received, then it reassembles the message.
        If ``timeout`` is set and elapses before a complete message is
        received, :meth:`get` returns ``None``; ``0`` only takes a message
        already received.
        Raises :exc:`EOFError` when the stream has ended and no complete
        message is left.
        """
        if self.get_in_progress:
            raise ServerError(
                "Called get() on Websocket frame assembler "
                "while asynchronous get is already in progress."
            )
        self.get_in_progress = True
        try:
            if not self.complete:
                if timeout is None:
                    await self._wait_complete()
                elif timeout <= 0:
                    if self.closed:
                        raise EOFError("stream of frames ended")
                    return None
                else:
                    try:
                        await asyncio.wait_for(self._wait_complete(), timeout)
                    except asyncio.TimeoutError:
                        return None
            frames = []
            while True:
                frame = self._take()
                frames.append(frame)
                if frame.fin:
                    break
        finally:
            self.get_in_progress = False
        data = b"".join(frame.data for frame in frames)
        if frames[0].opcode is Opcode.TEXT:
            return data.decode()  # UnicodeDecodeError for invalid UTF-8
        return data

    async def get_iter(self) -> AsyncIterator[Data]:
        """
        Stream the next message.
        Iterating the return value of :meth:`get_iter` yields a :class:`str`
        or :class:`bytes` for each frame in the message, as each arrives. The
        iterator must be consumed to the end; a cancelled one leaves the rest
        of its message unread. Raises :exc:`EOFError` when the stream has
        ended before the message's first frame.
        """
        if self.get_in_progress:
            raise ServerError(
                "Called get_iter on Websocket frame assembler "
                "while asynchronous get is already in progress."
            )
        self.get_in_progress = True
        try:
            frame = await self._next()
            decoder = (
                UTF8Decoder(errors="strict")
                if frame.opcode is Opcode.TEXT
                else None
            )
            while True:
                if decoder is None:
                    yield bytes(frame.data)
                else:
                    yield decoder.decode(frame.data, frame.fin)
                if frame.fin:
                    break
                frame = await self._next()
        finally:
            self.get_in_progress = False

    def put(self, frame: Frame) -> None:
        """
        Add ``frame`` to the next message. Control frames are ignored, as is
        anything after the end of the stream.
        :meth:`put` assumes that the stream of frames respects the protocol.
        If it doesn't, the behavior is undefined.
        """
        if frame.opcode not in DATA_OPCODES or self.closed:
            return
        self.frames.append(frame)
        if frame.fin:
            self.complete += 1
        self._wake()
        if len(self.frames) > self.high and not self.paused:
            self.paused = True
            self.protocol.pause_frames()

    def close(self) -> None:
        """End the stream after the queued frames."""
        if not self.closed:
            self.closed = True
            self._wake()

    def _wake(self) -> None:
        if self.waiter is not None and not self.waiter.done():
            self.waiter.set_result(None)

    async def _wait(self) -> None:
        """Wait for the next frame or the end of the stream."""
        self.waiter = asyncio.get_running_loop().create_future()
        try:
            await self.waiter
        finally:
            self.waiter = None

    async def _wait_complete(self) -> None:
        """Wait until a complete message is queued; EOFError at the end of
        the stream (a message left incomplete is never returned)."""
        while not self.complete:
            if self.closed:
                raise EOFError("stream of frames ended")
            await self._wait()

    async def _next(self) -> Frame:
        """The next frame, waiting for it; EOFError at the end of the stream."""
        while not self.frames:
            if self.closed:
                raise EOFError("stream of frames ended")
            await self._wait()
        return self._take()

    def _take(self) -> Frame:
        frame = self.frames.popleft()
        if frame.fin:
            self.complete -= 1
        if self.paused and len(self.frames) <= self.low:
            self.paused = False
            self.protocol.resume_frames()
        return frame
