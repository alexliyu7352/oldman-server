"""HTTPMethodView.before_dispatch: runs before the method handler with the
route arguments; a response it returns is the response, None goes on."""
from oldman_server.response import text
from oldman_server.views import HTTPMethodView


def test_a_response_from_before_dispatch_short_circuits(app):
    calls = []

    class Guarded(HTTPMethodView):
        async def before_dispatch(self, request, *args, **kwargs):
            calls.append(("before", request.method, kwargs))
            if request.headers.get("authorization") != "yes":
                return text("denied", status=403)
            return None

        async def get(self, request, item):
            calls.append(("get", item))
            return text(f"item {item}")

    app.add_route(Guarded.as_view(), "/items/<item>")

    _, response = app.test_client.get("/items/7")
    assert response.status == 403
    assert response.text == "denied"
    assert calls == [("before", "GET", {"item": "7"})]

    calls.clear()
    _, response = app.test_client.get("/items/7", headers={"authorization": "yes"})
    assert response.status == 200
    assert response.text == "item 7"
    assert calls == [("before", "GET", {"item": "7"}), ("get", "7")]


def test_a_synchronous_before_dispatch_and_handler(app):
    class Sync(HTTPMethodView):
        def before_dispatch(self, request, *args, **kwargs):
            request.ctx.checked = True

        def get(self, request):
            return text(str(request.ctx.checked))

    app.add_route(Sync.as_view(), "/")
    _, response = app.test_client.get("/")
    assert response.text == "True"


def test_an_exception_from_before_dispatch_goes_to_the_error_handler(app):
    from oldman_server.exceptions import Forbidden

    class View(HTTPMethodView):
        def before_dispatch(self, request, *args, **kwargs):
            raise Forbidden("no")

        async def get(self, request):
            return text("unreachable")

    app.add_route(View.as_view(), "/")
    _, response = app.test_client.get("/")
    assert response.status == 403

