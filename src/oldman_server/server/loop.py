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
            "You are requesting to run oldman-server using uvloop, but the "
            "install-time 'OLDMAN_SERVER_NO_UVLOOP' environment variable (used to "
            "opt-out of installing uvloop with oldman-server) is set to true. If "
            "you want to prevent oldman-server from overriding the event loop policy "
            "during runtime, set the 'USE_UVLOOP' configuration value to "
            "false."
        )

    if not isinstance(asyncio.get_event_loop_policy(), uvloop.EventLoopPolicy):
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
