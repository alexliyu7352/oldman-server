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


def test_the_health_endpoint_beside_an_application_health_blueprint():
    # The extension's blueprint has a name of its own: "Health" is a common
    # name for an application's, and blueprint names must be unique.
    from oldman_server import Blueprint, OldmanServer

    app = OldmanServer("HealthNames")
    app.blueprint(Blueprint("Health", url_prefix="/app-health"))
    app.extend(config={"health": True, "health_endpoint": True})
    assert "OldmanServerHealth" in app.blueprints and "Health" in app.blueprints
