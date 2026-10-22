from __future__ import annotations

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from oldman_server import OldmanServer


def setup_ext(app: OldmanServer, **kwargs):
    """Set up the extensions (oldman_server.ext) for an application that asks
    for them: app.extend() or app.ext. Unlike Sanic with sanic-ext installed,
    server startup does not extend an application that did not ask.
    AUTO_EXTEND = False turns the extensions off.

    Args:
        app (OldmanServer): The application.
        **kwargs: Keyword arguments to pass to `Extend`.

    Returns:
        Extend: The extensions, when set up here.
    """
    if not app.config.AUTO_EXTEND or getattr(app, "_ext", None):
        return None
    from oldman_server.ext import Extend

    app._ext = Extend(app, **kwargs)
    return app.ext
