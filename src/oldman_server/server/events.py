from __future__ import annotations

from collections.abc import Iterable
from inspect import isawaitable
from typing import TYPE_CHECKING, Any, Callable

from oldman_server.helpers import accepts


if TYPE_CHECKING:
    from oldman_server import OldmanServer


def trigger_events(
    events: Iterable[Callable[..., Any]] | None,
    loop,
    app: OldmanServer | None = None,
    **kwargs,
):
    """Trigger event callbacks (functions or async)

    Args:
        events (Optional[Iterable[Callable[..., Any]]]): [description]
        loop ([type]): [description]
        app (Optional[OldmanServer], optional): [description]. Defaults to None.
    """
    if events:
        for event in events:
            # By the signature, not by calling it: a TypeError the event
            # raises is its own.
            first = () if not app else (app,)
            if accepts(event, *first, **kwargs) or not accepts(event, *first, loop, **kwargs):
                result = event(*first, **kwargs)
            else:
                result = event(*first, loop, **kwargs)
            if isawaitable(result):
                loop.run_until_complete(result)
