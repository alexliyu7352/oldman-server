import logging
import os

from oldman_server.helpers import Default, _default
from oldman_server.log import (
    access_logger,
    error_logger,
    logger,
    server_logger,
    websockets_logger,
)
from oldman_server.logging.formatter import (
    AutoAccessFormatter,
    AutoFormatter,
    DebugAccessFormatter,
    DebugFormatter,
    ProdAccessFormatter,
    ProdFormatter,
)


def setup_logging(
    debug: bool,
    no_color: bool = False,
    log_extra: bool | Default = _default,
) -> None:
    if AutoFormatter.SETUP:
        return

    if isinstance(log_extra, Default):
        log_extra = debug
        os.environ["OLDMAN_SERVER_LOG_EXTRA"] = str(log_extra)
    AutoFormatter.LOG_EXTRA = log_extra

    if no_color:
        os.environ["OLDMAN_SERVER_NO_COLOR"] = str(no_color)
        AutoFormatter.NO_COLOR = no_color

    AutoFormatter.SETUP = True

    for lggr in (logger, server_logger, error_logger, websockets_logger):
        _auto_format(
            lggr,
            AutoFormatter,
            DebugFormatter if debug else ProdFormatter,
        )
    _auto_format(
        access_logger,
        AutoAccessFormatter,
        DebugAccessFormatter if debug else ProdAccessFormatter,
    )


def _auto_format(
    logger: logging.Logger,
    auto_class: type[AutoFormatter],
    formatter_class: type[AutoFormatter],
) -> None:
    for handler in logger.handlers:
        if type(handler.formatter) is auto_class:
            handler.setFormatter(formatter_class())
