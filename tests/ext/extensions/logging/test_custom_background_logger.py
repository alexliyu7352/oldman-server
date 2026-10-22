from oldman_server import OldmanServer

from oldman_server.ext.extensions.logging.logger import Logger


def test_defult_config(app: OldmanServer):
    assert hasattr(app.config, "LOGGERS")


def test_custom_background_logger(app: OldmanServer):
    assert Logger.LOGGERS == []
    Logger.prepare(app)
    assert hasattr(app.shared_ctx, "logger_queue")
    assert Logger.LOGGERS == app.config.LOGGERS
    assert "oldman_server.root" in Logger.LOGGERS

    logger = Logger()
    assert (
        len(logger.loggers) == len(Logger.LOGGERS) == len(app.config.LOGGERS)
    )
