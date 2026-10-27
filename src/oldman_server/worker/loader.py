from __future__ import annotations

import os
import sys

from importlib import import_module
from inspect import isfunction
from typing import TYPE_CHECKING, Any, Callable



if TYPE_CHECKING:
    from oldman_server import OldmanServer as AppType

DEFAULT_APP_NAME = "app"


class AppLoader:
    """A helper to load application instances.

    Generally used by the worker to load the application instance.

    See [Dynamic Applications](/en/guide/deployment/app-loader) for information on when you may need to use this.

    Args:
        module_input (str): The module to load the application from.
        as_factory (bool): Whether the application is a factory.
        args (Any): Arguments to pass to the application factory.
        factory (Callable[[], SanicApp]): A callable that returns a Sanic application instance.
    """  # noqa: E501

    def __init__(
        self,
        module_input: str = "",
        as_factory: bool = False,
        args: Any = None,
        factory: Callable[[], AppType] | None = None,
    ) -> None:
        self.module_input = module_input
        self.module_name = ""
        self.app_name = ""
        self.as_factory = as_factory
        self.args = args
        self.factory = factory
        self.cwd = os.getcwd()

        if module_input:
            delimiter = ":" if ":" in module_input else "."
            if (
                delimiter in module_input
                and "\\" not in module_input
                and "/" not in module_input
            ):
                module_name, app_name = module_input.rsplit(delimiter, 1)
                self.module_name = module_name
                self.app_name = app_name
                if self.app_name.endswith("()"):
                    self.as_factory = True
                    self.app_name = self.app_name[:-2]

    def load(self) -> AppType:
        module_path = os.path.abspath(self.cwd)
        if module_path not in sys.path:
            sys.path.append(module_path)

        if self.factory:
            return self.factory()

        implied_app_name = False
        if not self.module_name and not self.app_name:
            self.module_name = self.module_input
            self.app_name = DEFAULT_APP_NAME
            implied_app_name = True
        module = import_module(self.module_name)
        app = getattr(module, self.app_name, None)
        if not app and implied_app_name:
            raise ValueError(
                "Looks like you only supplied a module name. oldman-server "
                "tried to locate an application instance named "
                f"{self.module_name}:app, but was unable to locate "
                "an application instance. Please provide a path "
                "to a global instance of OldmanServer(), or a callable that "
                "will return an OldmanServer() application instance."
            )
        if self.as_factory or isfunction(app):
            if not callable(app):
                type_name = type(app).__name__
                raise ValueError(
                    f"Expected a callable, but got {type_name}."
                )
            try:
                app = app(self.args)
            except TypeError:
                app = app()
        return app
