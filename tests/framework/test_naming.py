from oldman_server import Blueprint, OldmanServer, text


def factory(sanic_cls: type[OldmanServer], blueprint_cls: type[Blueprint]):
    app = sanic_cls("Foo")
    bp = blueprint_cls("Bar", url_prefix="/bar")

    @app.get("/")
    async def handler(request):
        return text(request.name)

    @bp.get("/")
    async def handler(request):  # noqa: F811  // Intentionally reused handler name
        return text(request.name)

    app.blueprint(bp)

    return app


def test_vanilla_sanic():
    app = factory(OldmanServer, Blueprint)
    _, foo_response = app.test_client.get("/")
    _, bar_response = app.test_client.get("/bar/")

    assert foo_response.text == "Foo.handler"
    assert bar_response.text == "Foo.Bar.handler"


def test_custom_app():
    class Custom(OldmanServer):
        def generate_name(self, *objects):
            existing = self._generate_name(*objects)
            return existing.replace("Foo", "CHANGED_APP")

    app = factory(Custom, Blueprint)
    _, foo_response = app.test_client.get("/")
    _, bar_response = app.test_client.get("/bar/")

    assert foo_response.text == "CHANGED_APP.handler"
    assert bar_response.text == "CHANGED_APP.Bar.handler"


def test_custom_blueprint():
    class Custom(Blueprint):
        def generate_name(self, *objects):
            existing = self._generate_name(*objects)
            return existing.replace("Bar", "CHANGED_BP")

    app = factory(OldmanServer, Custom)
    _, foo_response = app.test_client.get("/")
    _, bar_response = app.test_client.get("/bar/")

    assert foo_response.text == "Foo.handler"
    assert bar_response.text == "Foo.CHANGED_BP.handler"
