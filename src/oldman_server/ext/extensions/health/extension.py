from oldman_server.ext.extensions.health.endpoint import setup_health_endpoint

from ..base import Extension
from .monitor import HealthMonitor


class HealthExtension(Extension):
    name = "health"

    def startup(self, bootstrap) -> None:
        if self.config.HEALTH:
            if self.config.HEALTH_MONITOR:
                HealthMonitor.setup(self.app)

            if self.config.HEALTH_ENDPOINT:
                setup_health_endpoint(self.app)

    def included(self):
        return self.config.HEALTH
