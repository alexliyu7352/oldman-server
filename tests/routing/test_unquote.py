from unittest.mock import Mock

from oldman_server.routing import BaseRouter


class Router(BaseRouter):
    def get(self, path, method, extra=None):
        return self.resolve(path=path, method=method, extra=extra)


def test_no_unquote():
    handler = Mock(return_value=123)

    router = Router()
    router.add("/<foo>/<bar>", methods=["GET"], handler=handler, unquote=False)
    router.finalize()

    _, handler, params = router.get("/%F0%9F%98%8E/sunglasses", "GET")
    assert params == {"bar": "sunglasses", "foo": "%F0%9F%98%8E"}

    _, handler, params = router.get("/😎/sunglasses", "GET")
    assert params == {"bar": "sunglasses", "foo": "😎"}


def test_unquote():
    handler = Mock(return_value=123)

    router = Router()
    router.add("/<foo>/<bar>", methods=["GET"], handler=handler, unquote=True)
    router.finalize()

    _, handler, params = router.get("/%F0%9F%98%8E/sunglasses", "GET")
    assert params == {"bar": "sunglasses", "foo": "😎"}

    _, handler, params = router.get("/😎/sunglasses", "GET")
    assert params == {"bar": "sunglasses", "foo": "😎"}


def test_unquote_non_string():
    handler = Mock(return_value=123)

    router = Router()
    router.add(
        "/<foo>/<bar:int>", methods=["GET"], handler=handler, unquote=True
    )
    router.finalize()

    _, handler, params = router.get("/%F0%9F%98%8E/123", "GET")
    assert params == {"bar": 123, "foo": "😎"}

    _, handler, params = router.get("/😎/123", "GET")
    assert params == {"bar": 123, "foo": "😎"}


def test_unquote_applies_only_to_the_route_that_asks_for_it():
    # The option used to be kept on the shared tree and spread to every
    # dynamic route built after a route that set it.
    for order in ((True, False), (False, True)):
        router = Router()
        for unquote, path in zip(order, ("/files/<name>", "/tags/<tag>")):
            router.add(path, methods=["GET"], handler=Mock(), unquote=unquote)
        router.finalize()

        _, _, files = router.get("/files/Python%203", "GET")
        _, _, tags = router.get("/tags/Python%203", "GET")
        assert files == {"name": "Python 3" if order[0] else "Python%203"}
        assert tags == {"tag": "Python 3" if order[1] else "Python%203"}


def test_unquote_on_routes_sharing_a_node():
    router = Router()
    router.add("/files/<name>", methods=["GET"], handler=Mock(), unquote=True)
    router.add("/files/<name>/raw", methods=["GET"], handler=Mock(), unquote=False)
    router.finalize()

    assert router.get("/files/a%20b", "GET")[2] == {"name": "a b"}
    assert router.get("/files/a%20b/raw", "GET")[2] == {"name": "a%20b"}


def test_ext_next_to_an_unquote_route():
    router = Router()
    router.add("/files/<name>", methods=["GET"], handler=Mock(), unquote=True)
    router.add("/images/<file:ext>", methods=["GET"], handler=Mock())
    router.add("/docs/<file:ext>", methods=["GET"], handler=Mock(), unquote=True)
    router.finalize()

    assert router.get("/images/cat.png", "GET")[2] == {"file": "cat", "ext": "png"}
    assert router.get("/images/my%20cat.png", "GET")[2] == {"file": "my%20cat", "ext": "png"}
    assert router.get("/docs/my%20doc.txt", "GET")[2] == {"file": "my doc", "ext": "txt"}


def test_unquote_on_a_regex_route():
    router = Router()
    router.add(r"/code/<code:[a-z%0-9]+>", methods=["GET"], handler=Mock(), unquote=True)
    router.add(r"/raw/<code:[a-z%0-9]+>", methods=["GET"], handler=Mock())
    router.finalize()

    assert router.get("/code/a%20b", "GET")[2] == {"code": "a b"}
    assert router.get("/raw/a%20b", "GET")[2] == {"code": "a%20b"}
