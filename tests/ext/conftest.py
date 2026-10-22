import re

import pytest

from oldman_server import OldmanServer

from oldman_server.ext import Extend, Extension
from oldman_server.ext.extensions.http.extension import HTTPExtension
from oldman_server.ext.extensions.injection.extension import InjectionExtension
from oldman_server.ext.extensions.openapi.builders import (
    OperationStore,
    SpecificationBuilder,
)
from oldman_server.ext.extensions.openapi.extension import OpenAPIExtension


slugify = re.compile(r"[^a-zA-Z0-9_\-]")


@pytest.fixture(autouse=True)
def reset_globals():
    yield
    SpecificationBuilder.reset()
    OperationStore.reset()
    Extend.reset()
    Extension.reset()
    OldmanServer._app_registry.clear()


@pytest.fixture(autouse=True)
def reset_extensions():
    yield
    for ext in (HTTPExtension, InjectionExtension, OpenAPIExtension):
        ext._singleton = None


@pytest.fixture
def bare_app(request):
    app = OldmanServer(slugify.sub("-", request.node.name))

    yield app


@pytest.fixture
def app(bare_app):
    Extend(bare_app)

    yield bare_app


@pytest.fixture
def get_docs(app):
    def fetch():
        nonlocal app

        _, response = app.test_client.get("/docs/openapi.json")
        return response.json

    return fetch
