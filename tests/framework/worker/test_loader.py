import sys

from os import getcwd
from types import SimpleNamespace

import pytest

from oldman_server.app import OldmanServer
from oldman_server.worker.loader import AppLoader


@pytest.mark.parametrize(
    "module_input", ("tests.framework.fake.server:app", "tests.framework.fake.server.app")
)
def test_load_app_instance(module_input):
    loader = AppLoader(module_input)
    app = loader.load()
    assert isinstance(app, OldmanServer)


@pytest.mark.parametrize(
    "module_input",
    ("tests.framework.fake.server:create_app", "tests.framework.fake.server:create_app()"),
)
def test_load_app_factory(module_input):
    loader = AppLoader(module_input, as_factory=True)
    app = loader.load()
    assert isinstance(app, OldmanServer)


def test_create_with_factory():
    loader = AppLoader(factory=lambda: OldmanServer("Test"))
    app = loader.load()
    assert isinstance(app, OldmanServer)


def test_cwd_in_path():
    AppLoader("tests.framework.fake.server:app").load()
    assert getcwd() in sys.path


def test_input_is_factory():
    ns = SimpleNamespace(target="foo")
    loader = AppLoader("tests.framework.fake.server:create_app", args=ns)
    app = loader.load()
    assert isinstance(app, OldmanServer)


