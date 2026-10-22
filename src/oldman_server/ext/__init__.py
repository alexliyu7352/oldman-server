from oldman_server.ext.bootstrap import Extend
from oldman_server.ext.config import Config
from oldman_server.ext.extensions.base import Extension
from oldman_server.ext.extensions.health.extension import HealthExtension
from oldman_server.ext.extensions.http.extension import HTTPExtension
from oldman_server.ext.extensions.injection.extension import InjectionExtension
from oldman_server.ext.extensions.openapi.extension import OpenAPIExtension
from oldman_server.ext.extensions.http.cors import cors
from oldman_server.ext.extensions.openapi import openapi
from oldman_server.ext.extensions.templating.render import render
from oldman_server.ext.extras.request import CountedRequest
from oldman_server.ext.extras.serializer.decorator import serializer
from oldman_server.ext.extras.validation.decorator import validate


# The Sanic Extensions release this subpackage derives from (MIT,
# LICENSE.sanic-ext).
__version__ = "25.12.0"

__all__ = [
    "Config",
    "CountedRequest",
    "Extend",
    "Extension",
    "HealthExtension",
    "HTTPExtension",
    "InjectionExtension",
    "OpenAPIExtension",
    "TemplatingExtension",
    "cors",
    "openapi",
    "render",
    "serializer",
    "validate",
]


def __getattr__(name):
    # TemplatingExtension needs Jinja2, an optional dependency: imported on use.
    if name == "TemplatingExtension":
        from oldman_server.ext.extensions.templating.extension import (
            TemplatingExtension,
        )

        return TemplatingExtension
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
