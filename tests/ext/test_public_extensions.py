"""The built-in extension classes are part of oldman_server.ext's interface
(applications pass them to Extend(extensions=[...]))."""
from oldman_server import ext
from oldman_server.ext.extensions.health.extension import HealthExtension
from oldman_server.ext.extensions.http.extension import HTTPExtension
from oldman_server.ext.extensions.injection.extension import InjectionExtension
from oldman_server.ext.extensions.openapi.extension import OpenAPIExtension
from oldman_server.ext.extensions.templating.extension import TemplatingExtension


def test_built_in_extensions_are_exported():
    assert (ext.HealthExtension, ext.HTTPExtension, ext.InjectionExtension,
            ext.OpenAPIExtension, ext.TemplatingExtension) == (
        HealthExtension, HTTPExtension, InjectionExtension, OpenAPIExtension,
        TemplatingExtension)
