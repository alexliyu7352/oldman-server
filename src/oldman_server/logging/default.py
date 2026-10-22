import sys

from typing import Any


LOGGING_CONFIG_DEFAULTS: dict[str, Any] = dict(  # no cov
    version=1,
    disable_existing_loggers=False,
    loggers={
        "oldman_server.root": {"level": "INFO", "handlers": ["console"]},
        "oldman_server.error": {
            "level": "INFO",
            "handlers": ["error_console"],
            "propagate": True,
            "qualname": "oldman_server.error",
        },
        "oldman_server.access": {
            "level": "INFO",
            "handlers": ["access_console"],
            "propagate": True,
            "qualname": "oldman_server.access",
        },
        "oldman_server.server": {
            "level": "INFO",
            "handlers": ["console"],
            "propagate": True,
            "qualname": "oldman_server.server",
        },
        "oldman_server.websockets": {
            "level": "INFO",
            "handlers": ["console"],
            "propagate": True,
            "qualname": "oldman_server.websockets",
        },
    },
    handlers={
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "generic",
            "stream": sys.stdout,
        },
        "error_console": {
            "class": "logging.StreamHandler",
            "formatter": "generic",
            "stream": sys.stderr,
        },
        "access_console": {
            "class": "logging.StreamHandler",
            "formatter": "access",
            "stream": sys.stdout,
        },
    },
    formatters={
        "generic": {"class": "oldman_server.logging.formatter.AutoFormatter"},
        "access": {"class": "oldman_server.logging.formatter.AutoAccessFormatter"},
    },
)
"""
Default logging configuration
"""
