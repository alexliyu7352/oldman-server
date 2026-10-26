"""ext_app.py stock|candidate PORT WORKERS: the site's three routes with oldman's
five extensions, applied as oldman's web runtime does (its scaffolds' config),
on stock Sanic + sanic-ext or on oldman-server + its ext (native host).
Run by drive.py: stock in the sanic-ext venv, candidate in this project's."""
import asyncio
import sys

MODE = sys.argv[1]
if MODE == "stock":
    from sanic import Sanic as App
    from sanic.response import text
    from sanic_ext import Config, Extend
    from sanic_ext.extensions.health.extension import HealthExtension
    from sanic_ext.extensions.http.extension import HTTPExtension
    from sanic_ext.extensions.injection.extension import InjectionExtension
    from sanic_ext.extensions.openapi.extension import OpenAPIExtension
    from sanic_ext.extensions.templating.extension import TemplatingExtension
else:
    from oldman_server import OldmanServer as App
    from oldman_server.ext import Config, Extend
    from oldman_server.ext.extensions.health.extension import HealthExtension
    from oldman_server.ext.extensions.http.extension import HTTPExtension
    from oldman_server.ext.extensions.injection.extension import InjectionExtension
    from oldman_server.ext.extensions.openapi.extension import OpenAPIExtension
    from oldman_server.ext.extensions.templating.extension import TemplatingExtension
    from oldman_server.response import text

# At module level: the spawned workers import this module (with these arguments).
app = App("benchmark", configure_logging=False, strict_slashes=True)
app.config.ACCESS_LOG = False
if MODE == "candidate":
    app.config.NATIVE_SERVER = True


@app.route("/")
async def index(request):
    return text("")


@app.route("/user/<id:int>", methods=["GET"])
async def user_info(request, id):
    return text(str(id))


@app.route("/user", methods=["POST"])
async def user(request):
    return text("")


@app.route("/loop")
async def loop(request):
    return text(type(asyncio.get_running_loop()).__module__)


Extend(app, config=Config(oas=False, oas_autodoc=False, templating_enable_async=True, logging=False),
       extensions=[InjectionExtension, OpenAPIExtension, HTTPExtension, HealthExtension, TemplatingExtension],
       built_in_extensions=False)

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(sys.argv[2]), workers=int(sys.argv[3]), access_log=False, motd=False)
