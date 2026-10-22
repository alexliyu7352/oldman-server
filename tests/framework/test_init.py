from importlib import import_module

import pytest


@pytest.mark.parametrize(
    "item",
    (
        "__version__",
        "OldmanServer",
        "Blueprint",
        "HTTPMethod",
        "HTTPResponse",
        "Request",
        "Websocket",
        "empty",
        "file",
        "html",
        "json",
        "redirect",
        "text",
    ),
)
def test_imports(item):
    assert hasattr(import_module("oldman_server"), item)
