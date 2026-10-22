from pathlib import Path

from oldman_server import OldmanServer
from oldman_server.exceptions import OldmanServerException


def create_simple_server(directory: Path):
    if not directory.is_dir():
        raise OldmanServerException(
            "Cannot set up the simple server without a path to a directory"
        )

    app = OldmanServer("SimpleServer")
    app.static(
        "/", directory, name="main", directory_view=True, index="index.html"
    )

    return app
