from oldman_server import Request, OldmanServer, text

from oldman_server.ext.extensions.openapi import openapi

from .utils import get_spec


def test_tag(app: OldmanServer):
    TAG_NAME = "test-tag-1"

    @app.route("/test0")
    async def handler0(request: Request, val1: int):
        """
        openapi:
        ---
        tags:
          - test-tag-1
        """
        return text("ok")

    @app.route("/test1")
    @openapi.tag(TAG_NAME)
    async def handler1(request: Request, val1: int):
        return text("ok")

    @app.route("/test2")
    @openapi.definition(tag=TAG_NAME)
    async def handler2(request: Request, val1: int):
        return text("ok")

    spec = get_spec(app)
    for i in range(3):
        assert f"/test{i}" in spec["paths"]
        tag = spec["paths"][f"/test{i}"]["get"]["tags"][0]
        assert tag == TAG_NAME
