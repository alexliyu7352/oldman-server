import asyncio
import re

from unittest.mock import Mock

import pytest

from websockets.frames import CTRL_OPCODES, Frame, Opcode

from oldman_server.exceptions import ServerError
from oldman_server.server.websockets.frame import WebsocketFrameAssembler


def text(data, fin=True):
    return Frame(Opcode.TEXT, data.encode(), fin=fin)


def binary(data, fin=True):
    return Frame(Opcode.BINARY, data, fin=fin)


def cont(data, fin=True):
    return Frame(Opcode.CONT, data, fin=fin)


async def test_ws_frame_messages_come_out_in_order():
    assembler = WebsocketFrameAssembler(Mock())
    assembler.put(text("one"))
    assembler.put(binary(b"two"))
    assembler.put(text("thr", fin=False))
    assembler.put(cont(b"ee"))

    assert await assembler.get() == "one"
    assert await assembler.get() == b"two"
    assert await assembler.get() == "three"


async def test_ws_frame_get_waits_for_the_last_frame():
    assembler = WebsocketFrameAssembler(Mock())
    get = asyncio.ensure_future(assembler.get())
    assembler.put(binary(b"fo", fin=False))
    await asyncio.sleep(0)
    assert not get.done()
    assembler.put(cont(b"o"))
    assert await get == b"foo"


async def test_ws_frame_get_timeout_0_takes_only_a_complete_message():
    assembler = WebsocketFrameAssembler(Mock())
    assert await assembler.get(0) is None
    assembler.put(text("in", fin=False))
    assert await assembler.get(0) is None
    assembler.put(cont(b"complete"))
    assert await assembler.get(0) == "incomplete"


async def test_ws_frame_get_timeout_then_a_message_still_arrives():
    # A get() that times out (or is cancelled) loses nothing: the next one
    # gets the message, including the frames that came before the timeout.
    assembler = WebsocketFrameAssembler(Mock())
    assembler.put(text("la", fin=False))
    assert await assembler.get(0.05) is None
    assert assembler.get_in_progress is False
    assembler.put(cont(b"te"))
    assert await assembler.get(0.05) == "late"


async def test_ws_frame_a_cancelled_get_leaves_the_assembler_usable():
    assembler = WebsocketFrameAssembler(Mock())
    get = asyncio.ensure_future(assembler.get())
    await asyncio.sleep(0)
    get.cancel()
    with pytest.raises(asyncio.CancelledError):
        await get
    assembler.put(text("next"))
    assert await assembler.get() == "next"


async def test_ws_frame_get_in_progress():
    assembler = WebsocketFrameAssembler(Mock())
    first = asyncio.ensure_future(assembler.get())
    await asyncio.sleep(0)

    message = re.escape(
        "Called get() on Websocket frame assembler "
        "while asynchronous get is already in progress."
    )
    with pytest.raises(ServerError, match=message):
        await assembler.get()
    first.cancel()


async def test_ws_frame_close_ends_the_stream_after_the_queued_messages():
    assembler = WebsocketFrameAssembler(Mock())
    assembler.put(text("one"))
    assembler.put(text("two"))
    assembler.put(text("partial", fin=False))
    assembler.close()

    assert await assembler.get() == "one"
    assert await assembler.get() == "two"
    with pytest.raises(EOFError):
        await assembler.get()  # a message left incomplete is never returned
    with pytest.raises(EOFError):
        await assembler.get(0)


async def test_ws_frame_close_wakes_a_waiting_get():
    assembler = WebsocketFrameAssembler(Mock())
    get = asyncio.ensure_future(assembler.get())
    await asyncio.sleep(0)
    assembler.close()
    with pytest.raises(EOFError):
        await get
    assembler.put(text("after the end"))  # ignored
    assert len(assembler) == 0


@pytest.mark.parametrize("opcode", CTRL_OPCODES)
async def test_ws_frame_put_skips_control_frames(opcode):
    assembler = WebsocketFrameAssembler(Mock())
    assembler.put(Frame(opcode, b""))
    assert len(assembler) == 0


async def test_ws_frame_invalid_utf8_raises():
    assembler = WebsocketFrameAssembler(Mock())
    assembler.put(Frame(Opcode.TEXT, b"\xff"))
    with pytest.raises(UnicodeDecodeError):
        await assembler.get()


async def test_ws_frame_reading_pauses_above_high_and_resumes_at_low():
    protocol = Mock()
    assembler = WebsocketFrameAssembler(protocol, high=4)
    for index in range(5):
        assembler.put(text(str(index)))
    protocol.pause_frames.assert_called_once()
    for _ in range(3):
        await assembler.get()
    protocol.resume_frames.assert_not_called()  # 2 left, low is 1
    await assembler.get()
    protocol.resume_frames.assert_called_once()


async def test_ws_frame_get_iter_streams_the_frames_of_a_message():
    assembler = WebsocketFrameAssembler(Mock())
    assembler.put(text("fra", fin=False))
    chunks = []

    async def read():
        async for chunk in assembler.get_iter():
            chunks.append(chunk)

    reader = asyncio.ensure_future(read())
    await asyncio.sleep(0)
    assert chunks == ["fra"]
    assembler.put(cont(b"gme", fin=False))
    assembler.put(cont(b"nts"))
    await reader
    assert chunks == ["fra", "gme", "nts"]
    assert assembler.get_in_progress is False


async def test_ws_frame_get_iter_in_progress():
    assembler = WebsocketFrameAssembler(Mock())
    assembler.get_in_progress = True

    message = re.escape(
        "Called get_iter on Websocket frame assembler "
        "while asynchronous get is already in progress."
    )

    with pytest.raises(ServerError, match=message):
        [x async for x in assembler.get_iter()]


async def test_ws_frame_get_iter_at_the_end_of_the_stream():
    assembler = WebsocketFrameAssembler(Mock())
    assembler.close()
    with pytest.raises(EOFError):
        [x async for x in assembler.get_iter()]


async def test_ws_frame_reaches_reader():
    assembler = WebsocketFrameAssembler(Mock())
    assert not assembler.reaches_reader()
    assembler.put(text("unread"))
    assert assembler.reaches_reader()
    await assembler.get()
    assert not assembler.reaches_reader()
    get = asyncio.ensure_future(assembler.get())
    await asyncio.sleep(0)
    assert assembler.reaches_reader()
    get.cancel()


async def test_ws_replies_wait_for_a_send_in_progress():
    # The pong a ping asks for is written after a send that holds conn_mutex
    # (the native tunnel may write one frame in several pieces): frames on
    # the wire never interleave.
    from websockets.protocol import State
    from websockets.server import ServerProtocol

    from oldman_server.server.websockets.impl import WebsocketImplProtocol

    websocket = WebsocketImplProtocol(ServerProtocol(state=State.OPEN), ping_interval=None)
    written = []
    websocket.io_proto = Mock()
    websocket.io_proto.transport.write = written.append
    ping = Frame(Opcode.PING, b"x").serialize(mask=True)  # as a client sends it
    async with websocket.conn_mutex:
        websocket.data_received(ping)
        await asyncio.sleep(0)
        assert written == []
        written.append(b"<the frame being sent>")
    for _ in range(3):
        await asyncio.sleep(0)
    assert written == [b"<the frame being sent>", Frame(Opcode.PONG, b"x").serialize(mask=False)]

    websocket.data_received(ping)  # nothing in progress: written at once
    assert written[-1] == Frame(Opcode.PONG, b"x").serialize(mask=False)
