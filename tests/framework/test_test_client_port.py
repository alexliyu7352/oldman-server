from oldman_server.testing.testing import PORT, TestClient

from oldman_server.response import json, text


# ------------------------------------------------------------ #
#  UTF-8
# ------------------------------------------------------------ #


def test_test_client_port_none(app):
    @app.get("/get")
    def handler(request):
        return text("OK")

    test_client = TestClient(app, port=None)

    request, response = test_client.get("/get")
    assert response.text == "OK"

    request, response = test_client.post("/get")
    assert response.status == 405


def test_test_client_port_default(app):
    @app.get("/get")
    def handler(request):
        return json(request.transport.get_extra_info("sockname")[1])

    test_client = TestClient(app)
    assert test_client.port == PORT  # Can be None before request

    request, response = test_client.get("/get")
    assert test_client.port > 0
    assert response.json == test_client.port


def test_the_test_client_is_not_collected_as_a_test_class(tmp_path):
    # Imported into a test module, a class named Test* is collected by
    # pytest: a warning, and an error where collection warnings are errors.
    import subprocess
    import sys

    (tmp_path / "test_uses_client.py").write_text(
        "from oldman_server.testing.testing import TestClient\n\n"
        "def test_nothing():\n    pass\n"
    )
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
         "-W", "error::pytest.PytestCollectionWarning", str(tmp_path)],
        capture_output=True, text=True, timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
