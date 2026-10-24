"""The web-frameworks-benchmark routes (the-benchmarker), built with stock Sanic
(its source importable as `sanic`) or with oldman_server; /work spends 25 us of
the handler's own CPU first, /loop names the event loop."""
import asyncio
import time


def make_app(stock=False):
    if stock:
        from sanic import Sanic as App
        from sanic.response import text
    else:
        from oldman_server import OldmanServer as App
        from oldman_server.response import text

    app = App("benchmark", configure_logging=False)
    app.config.ACCESS_LOG = False

    @app.route("/")
    async def index(request):
        return text("")

    @app.route("/user/<id:int>", methods=["GET"])
    async def user_info(request, id):
        return text(str(id))

    @app.route("/user", methods=["POST"])
    async def user(request):
        return text("")

    @app.route("/work")
    async def work(request):
        end = time.perf_counter() + 25e-6
        while time.perf_counter() < end:
            pass
        return text("")

    @app.route("/loop")
    async def loop(request):
        return text(type(asyncio.get_running_loop()).__module__)

    return app
