import asyncio

from os import getenv

from oldman_server.log import error_logger
from oldman_server.utils import str_to_bool


def try_use_uvloop() -> None:
    """Use uvloop instead of the default asyncio loop."""
    try:
        import uvloop  # type: ignore
    except ImportError:
        error_logger.warning(
            "You are trying to use uvloop, but uvloop is not "
            "installed in your system. In order to use uvloop "
            "you must first install it. Otherwise, you can disable "
            "uvloop completely by setting the 'USE_UVLOOP' "
            "configuration value to false. oldman-server will now continue "
            "to run with the default event loop."
        )
        return

    uvloop_install_removed = str_to_bool(getenv("OLDMAN_SERVER_NO_UVLOOP", "no"))
    if uvloop_install_removed:
        error_logger.info(
            "The 'OLDMAN_SERVER_NO_UVLOOP' environment variable is set, but "
            "oldman-server always installs uvloop and uses it. To keep the "
            "default event loop policy, set the 'USE_UVLOOP' configuration "
            "value to false."
        )

    if not isinstance(asyncio.get_event_loop_policy(), uvloop.EventLoopPolicy):
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
