from __future__ import annotations

import os

from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any, Callable, Optional, Union

from oldman_server import OldmanServer
from oldman_server.__version__ import SANIC_VERSION as __version__
from oldman_server.config import DEFAULT_CONFIG
from oldman_server.exceptions import OldmanServerException
from oldman_server.helpers import Default, _default
from oldman_server.log import logger

from oldman_server.ext.config import Config, add_fallback_config
from oldman_server.ext.extensions.base import Extension
from oldman_server.ext.extensions.health.extension import HealthExtension
from oldman_server.ext.extensions.http.extension import HTTPExtension
from oldman_server.ext.extensions.injection.extension import InjectionExtension
from oldman_server.ext.extensions.injection.registry import (
    ConstantRegistry,
    InjectionRegistry,
)
from oldman_server.ext.extensions.logging.extension import LoggingExtension
from oldman_server.ext.extensions.openapi.builders import SpecificationBuilder
from oldman_server.ext.extensions.openapi.extension import OpenAPIExtension
from oldman_server.ext.utils.string import camel_to_snake


try:
    from jinja2 import Environment

    from oldman_server.ext.extensions.templating.engine import Templating
    from oldman_server.ext.extensions.templating.extension import TemplatingExtension

    TEMPLATING_ENABLED = True
except (ImportError, ModuleNotFoundError):
    TEMPLATING_ENABLED = False

try:
    from oldman_server.ext.extensions.mcp.extension import MCPExtension

    MCP_ENABLED = True
except (ImportError, ModuleNotFoundError):
    MCP_ENABLED = False

class Extend:
    _pre_registry: list[Union[type[Extension], Extension]] = []

    if TEMPLATING_ENABLED:
        environment: Environment
        templating: Templating

    def __init__(
        self,
        app: OldmanServer,
        *,
        extensions: Optional[list[Union[type[Extension], Extension]]] = None,
        built_in_extensions: bool = True,
        config: Optional[Union[Config, dict[str, Any]]] = None,
        **kwargs,
    ) -> None:
        """
        Ingress for instantiating sanic-ext

        :param app: Sanic application
        :type app: OldmanServer
        """
        if not isinstance(app, OldmanServer):
            raise OldmanServerException(
                f"Cannot apply the extensions to {app.__class__.__name__}"
            )

        self._injection_registry: Optional[InjectionRegistry] = None
        self._constant_registry: Optional[ConstantRegistry] = None
        self._openapi: Optional[SpecificationBuilder] = None
        self.app = app
        self.extensions: list[Extension] = []
        # The Sanic interface this package implements (extensions may read it).
        self.sanic_version = tuple(map(int, __version__.split(".")))
        app._ext = self
        app.ctx._dependencies = SimpleNamespace()

        if not isinstance(config, Config):
            config = Config.from_dict(config or {})
        self.config = add_fallback_config(app, config, **kwargs)

        extensions = extensions or []
        if built_in_extensions:
            extensions.extend(
                [
                    InjectionExtension,
                    OpenAPIExtension,
                    HTTPExtension,
                    HealthExtension,
                    LoggingExtension,
                ]
            )

            if TEMPLATING_ENABLED:
                extensions.append(TemplatingExtension)
            if MCP_ENABLED:
                extensions.append(MCPExtension)
        extensions.extend(Extend._pre_registry)

        started = set()
        for ext in extensions:
            if ext in started:
                continue
            extension = Extension.create(ext, app, self.config)
            extension._startup(self)
            self.extensions.append(extension)
            started.add(ext)

    def _display(self):
        if "OLDMAN_SERVER_WORKER_IDENTIFIER" in os.environ:
            return
        init_logs = ["Extensions:"]
        for extension in self.extensions:
            label = extension.render_label()
            if extension.included():
                init_logs.append(f"  > {extension.name} {label}")

        list(map(logger.info, init_logs))

    def add_dependency(
        self,
        type: type,
        constructor: Optional[Callable[..., Any]] = None,
        request_arg: Optional[str] = None,
    ) -> None:
        """
        Add a dependency for injection

        :param type: The type of the dependency
        :type type: Type
        :param constructor: A callable that will return an instance to be
            injected, when ``False`` it will call the type, defaults to None
        :type constructor: Optional[Callable[..., Any]], optional
        :param request_arg: Explicitly state which argument in the
            constructor (if any) should be a ``Request`` object, when set to
            ``None`` the constructor will be introspected to check the
            type annotations looking for a request object. You should really
            only use this if you **MUST** use a lambda explression, it is
            otherwise better to use a properly type annotated constructor,
            defaults to None
        :type request_arg: Optional[str], optional
        :raises OldmanServerException: _description_
        """
        if not self._injection_registry:
            raise OldmanServerException("Injection extension not enabled")
        self._injection_registry.register(
            type, constructor, request_arg=request_arg
        )

    def add_constant(self, name: str, value: Any, overwrite: bool = False):
        if not self._constant_registry:
            raise ValueError("Cannot add constant. No registry created.")
        self._constant_registry.register(name, value, overwrite)

    def load_constants(
        self,
        constants: Optional[Mapping[str, Any]] = None,
        overwrite: Union[Default, bool] = _default,
    ):
        if not constants:
            constants = {
                k: v
                for k, v in self.app.config.items()
                if k.isupper()
                and k not in DEFAULT_CONFIG
                and k not in self.config
                and not k.startswith("_")
            }
            if isinstance(overwrite, Default):
                overwrite = True
        if isinstance(overwrite, Default):
            overwrite = False
        for name, value in constants.items():
            self.add_constant(name, value, overwrite)

    def dependency(self, obj: Any, name: Optional[str] = None) -> None:
        if not name:
            name = camel_to_snake(obj.__class__.__name__)
        setattr(self.app.ctx._dependencies, name, obj)

        def getter(*_):
            return obj

        self.add_dependency(obj.__class__, getter)

    @property
    def openapi(self) -> SpecificationBuilder:
        if not self._openapi:
            self._openapi = SpecificationBuilder()

        return self._openapi

    def template(self, template_name: str, **kwargs):
        return self.templating.template(template_name, **kwargs)

    @classmethod
    def register(cls, extension: Union[type[Extension], Extension]) -> None:
        cls._pre_registry.append(extension)

    @classmethod
    def reset(cls) -> None:
        cls._pre_registry.clear()
