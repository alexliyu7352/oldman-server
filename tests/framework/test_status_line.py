"""Status lines take their reason phrase from one table,
oldman_server.helpers.STATUS_CODES (RFC 9110), on both servers: the native
host is given it at startup."""
from oldman_server import OldmanServer
from oldman_server.exceptions import OldmanServerException
from oldman_server.helpers import STATUS_CODES
from oldman_server.response import empty
from oldman_server.testing.reusable import ReusableClient


def test_rfc_9110_phrases():
    assert STATUS_CODES[413] == b"Content Too Large"
    assert STATUS_CODES[414] == b"URI Too Long"
    assert STATUS_CODES[416] == b"Range Not Satisfiable"
    assert STATUS_CODES[421] == b"Misdirected Request"
    assert STATUS_CODES[422] == b"Unprocessable Content"
    assert STATUS_CODES[425] == b"Too Early"


def test_every_status_line_uses_the_table():
    app = OldmanServer("StatusLines")

    @app.get("/<status:int>")
    async def answer(request, status: int):
        return empty(status=status)

    # Every final status either table may know, and two that none does.
    statuses = [status for status in STATUS_CODES if status >= 200] + [299, 599]
    with ReusableClient(app) as client:
        for status in statuses:
            _, response = client.get(f"/{status}", allow_redirects=False)
            assert response.status == status
            assert response.reason_phrase == STATUS_CODES.get(status, b"").decode(), status


def test_an_error_page_describes_the_status_with_its_phrase():
    app = OldmanServer("StatusPhrase")
    app.config.FALLBACK_ERROR_FORMAT = "json"

    @app.get("/")
    async def unprocessable(request):
        raise OldmanServerException("no", status_code=422)

    _, response = app.test_client.get("/")
    assert response.status == 422
    assert response.reason_phrase == "Unprocessable Content"
    assert response.json["description"] == "Unprocessable Content"
