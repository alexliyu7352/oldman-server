from oldman_server import OldmanServer  # type: ignore

from oldman_server.testing.testing import TestClient


class TestManager:
    __test__ = False

    def __init__(self, app: OldmanServer) -> None:
        self.test_client = TestClient(app)
        app._test_manager = self  # type: ignore
