import pytest

from oldman_server import OldmanServer, text


pytestmark = pytest.mark.xdist_group(name="process_spawning")


@pytest.fixture
def late_app(app: OldmanServer):
    app.config.TOUCHUP = False
    app.get("/")(lambda _: text(""))
    return app


def test_late_route(late_app: OldmanServer):
    @late_app.before_server_start
    async def late(app: OldmanServer):
        @app.get("/late")
        def handler(_):
            return text("late")

    _, response = late_app.test_client.get("/late")
    assert response.status_code == 200
    assert response.text == "late"


def test_late_middleware(late_app: OldmanServer):
    @late_app.get("/late")
    def handler(request):
        return text(request.ctx.late)

    @late_app.before_server_start
    async def late(app: OldmanServer):
        @app.on_request
        def handler(request):
            request.ctx.late = "late"

    _, response = late_app.test_client.get("/late")
    assert response.status_code == 200
    assert response.text == "late"


def test_late_signal(late_app: OldmanServer):
    @late_app.get("/late")
    def handler(request):
        return text(request.ctx.late)

    @late_app.before_server_start
    async def late(app: OldmanServer):
        @app.signal("http.lifecycle.request")
        def handler(request):
            request.ctx.late = "late"

    _, response = late_app.test_client.get("/late")
    assert response.status_code == 200
    assert response.text == "late"
