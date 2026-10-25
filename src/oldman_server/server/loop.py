import asyncio

from os import getenv

from oldman_server.compat import OS_IS_WINDOWS
from oldman_server.log import error_logger
from oldman_server.utils import str_to_bool


def try_use_uvloop() -> None:
    """Use uvloop instead of the default asyncio loop."""
    if OS_IS_WINDOWS:
        error_logger.warning(
            "You are trying to use uvloop, but uvloop is not compatible "
            "with your system. You can disable uvloop completely by setting "
            "the 'USE_UVLOOP' configuration value to false, or simply not "
            "defining it and letting oldman-server handle it for you. "
            "oldman-server will now continue to run using the default event loop."
        )
        return

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


def try_windows_loop():
    """Try to use the WindowsSelectorEventLoopPolicy instead of the default"""
    if not OS_IS_WINDOWS:
        error_logger.warning(
            "You are trying to use an event loop policy that is not "
            "compatible with your system. You can simply let oldman-server "
            "handle selecting the best loop for you. oldman-server will now "
            "continue to run using the default event loop."
        )
        return

    if not isinstance(
        asyncio.get_event_loop_policy(), asyncio.WindowsSelectorEventLoopPolicy
    ):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
