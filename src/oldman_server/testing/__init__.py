"""The test client for applications: TestClient (requests through a
running server), as `app.test_client`. It needs httpx.

Derived from sanic-testing 24.6.0 (MIT, LICENSE.sanic-testing) with imports
renamed to this package.
"""
from oldman_server.testing.manager import TestManager

# The sanic-testing release this subpackage derives from.
__version__ = "24.6.0"
__all__ = ("TestManager",)
