"""The test clients for applications: TestClient (requests through a
running server), as `app.test_client`, and AsyncTestClient, to await inside
a running event loop, as `app.async_test_client`. They need httpx.

Derived from sanic-testing 24.6.0 (MIT, LICENSE.sanic-testing) with imports
renamed to this package.
"""
from oldman_server.testing.manager import TestManager
from oldman_server.testing.testing import AsyncTestClient, TestClient

# The sanic-testing release this subpackage derives from.
__version__ = "24.6.0"
__all__ = ("AsyncTestClient", "TestClient", "TestManager")
