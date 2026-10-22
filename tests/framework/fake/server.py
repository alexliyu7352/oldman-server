import json

from oldman_server import OldmanServer, text
from oldman_server.application.constants import Mode
from oldman_server.config import Config
from oldman_server.log import LOGGING_CONFIG_DEFAULTS, logger


LOGGING_CONFIG = {**LOGGING_CONFIG_DEFAULTS}
LOGGING_CONFIG["formatters"]["generic"]["format"] = "%(message)s"
LOGGING_CONFIG["loggers"]["oldman_server.root"]["level"] = "DEBUG"

app = OldmanServer("FakeServer", log_config=LOGGING_CONFIG)


@app.get("/")
async def handler(request):
    return text(request.ip)


@app.main_process_start
async def app_info_dump(app: OldmanServer, _):
    app_data = {
        "access_log": app.config.ACCESS_LOG,
        "auto_reload": app.auto_reload,
        "debug": app.debug,
        "noisy_exceptions": app.config.NOISY_EXCEPTIONS,
    }
    logger.info(json.dumps(app_data))


@app.main_process_stop
async def app_cleanup(app: OldmanServer, _):
    app.state.auto_reload = False
    app.state.mode = Mode.PRODUCTION
    app.config = Config()


@app.after_server_start
async def shutdown(app: OldmanServer, _):
    app.stop()


def create_app():
    return app


def create_app_with_args(args):
    try:
        logger.info(f"foo={args.foo}")
    except AttributeError:
        logger.info(f"target={args.target}")

    return app


@app.command
async def foo(one, two: str, three: str = "..."):
    logger.info(f"FOO {one=} {two=} {three=}")


@app.command
def bar():
    logger.info("BAR")


@app.command(name="qqq")
async def baz():
    logger.info("BAZ")
