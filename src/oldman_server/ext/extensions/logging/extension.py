from ..base import Extension
from .logger import Logger


class LoggingExtension(Extension):
    name = "logging"

    def startup(self, bootstrap) -> None:
        if self.included():
            Logger.setup(self.app)

    def included(self):
        return self.config.LOGGING
