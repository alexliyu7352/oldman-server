import os
import tempfile

from pathlib import Path

import pytest

from oldman_server import OldmanServer
from oldman_server.handlers.directory import DirectoryHandler


pytestmark = pytest.mark.xdist_group(name="static_files")


def get_file_path(static_file_directory, file_name):
    return os.path.join(static_file_directory, file_name)


def get_file_content(static_file_directory, file_name):
    """The content of the static file to check"""
    with open(get_file_path(static_file_directory, file_name), "rb") as file:
        return file.read()


def test_static_index_single(app: OldmanServer, static_file_directory: str):
    app.static("/static", static_file_directory, index="test.html")

    _, response = app.test_client.get("/static/")
    assert response.status == 200
    assert response.body == get_file_content(
        static_file_directory, "test.html"
    )
    assert response.headers["Content-Type"] == "text/html; charset=utf-8"


def test_static_index_single_not_found(app: OldmanServer, static_file_directory: str):
    app.static("/static", static_file_directory, index="index.html")

    _, response = app.test_client.get("/static/")
    assert response.status == 404


def test_static_index_multiple(app: OldmanServer, static_file_directory: str):
    app.static(
        "/static",
        static_file_directory,
        index=["index.html", "test.html"],
    )

    _, response = app.test_client.get("/static/")
    assert response.status == 200
    assert response.body == get_file_content(
        static_file_directory, "test.html"
    )
    assert response.headers["Content-Type"] == "text/html; charset=utf-8"


def test_static_index_in_a_nested_directory(app: OldmanServer, static_file_directory: str):
    app.static("/static", static_file_directory, index="foo.txt")

    _, response = app.test_client.get("/static/nested/")
    assert response.status == 404

    _, response = app.test_client.get("/static/nested/dir/")
    assert response.status == 200
    assert response.body == get_file_content(
        f"{static_file_directory}/nested/dir", "foo.txt"
    )
    assert response.content_type == "text/plain; charset=utf-8"


def test_static_directory_handler(app: OldmanServer, static_file_directory: str):
    dh = DirectoryHandler("/static", Path(static_file_directory), index="foo.txt")
    app.static("/static", static_file_directory, directory_handler=dh)

    _, response = app.test_client.get("/static/nested/")
    assert response.status == 404

    _, response = app.test_client.get("/static/nested/dir/")
    assert response.status == 200
    assert response.body == get_file_content(
        f"{static_file_directory}/nested/dir", "foo.txt"
    )
    assert response.content_type == "text/plain; charset=utf-8"


def test_static_directory_handler_fails(app: OldmanServer):
    dh = DirectoryHandler("/static", Path(""), index="foo.txt")
    message = (
        "When explicitly setting directory_handler, you cannot set index. "
        "Instead, pass it to your DirectoryHandler instance."
    )
    with pytest.raises(ValueError, match=message):
        app.static("/static", "", directory_handler=dh, index="index.html")


@pytest.fixture
def symlink_test_directory(tmp_path):
    static_root = tmp_path / "static"
    static_root.mkdir()

    (static_root / "normal_file.txt").write_text("normal content")

    subdir = static_root / "subdir"
    subdir.mkdir()
    (subdir / "sub_file.txt").write_text("sub content")

    outside_dir = tmp_path / "outside"
    outside_dir.mkdir()
    secret_file = outside_dir / "secret.txt"
    secret_file.write_text("secret content")

    symlink_to_outside_file = static_root / "link_to_secret"
    symlink_to_outside_file.symlink_to(secret_file)

    symlink_to_outside_dir = static_root / "link_to_outside_dir"
    symlink_to_outside_dir.symlink_to(outside_dir)

    symlink_to_inside = static_root / "link_to_subdir"
    symlink_to_inside.symlink_to(subdir)

    broken_symlink = static_root / "broken_link"
    broken_symlink.symlink_to(tmp_path / "nonexistent")

    return static_root


def test_symlink_to_outside_file_returns_404(
    app: OldmanServer, symlink_test_directory: Path
):
    app.static("/static", symlink_test_directory)

    _, response = app.test_client.get("/static/link_to_secret")
    assert response.status == 404


def test_symlink_to_outside_dir_returns_404(
    app: OldmanServer, symlink_test_directory: Path
):
    app.static("/static", symlink_test_directory)

    _, response = app.test_client.get("/static/link_to_outside_dir/secret.txt")
    assert response.status == 404


@pytest.mark.parametrize(
    "follow_files,follow_dirs,path,expected_status",
    [
        # Normal file - always accessible
        (False, False, "/static/normal_file.txt", 200),
        (True, False, "/static/normal_file.txt", 200),
        (False, True, "/static/normal_file.txt", 200),
        (True, True, "/static/normal_file.txt", 200),
        # Symlink to file inside root - always accessible
        (False, False, "/static/link_to_subdir/sub_file.txt", 200),
        (True, False, "/static/link_to_subdir/sub_file.txt", 200),
        (False, True, "/static/link_to_subdir/sub_file.txt", 200),
        (True, True, "/static/link_to_subdir/sub_file.txt", 200),
        # Symlink to file outside root - only with follow_files=True
        (False, False, "/static/link_to_secret", 404),
        (True, False, "/static/link_to_secret", 200),
        (False, True, "/static/link_to_secret", 404),
        (True, True, "/static/link_to_secret", 200),
        # File in symlinked dir outside root - only with follow_dirs=True
        (False, False, "/static/link_to_outside_dir/secret.txt", 404),
        (True, False, "/static/link_to_outside_dir/secret.txt", 404),
        (False, True, "/static/link_to_outside_dir/secret.txt", 200),
        (True, True, "/static/link_to_outside_dir/secret.txt", 200),
        # Broken symlink - always 404
        (False, False, "/static/broken_link", 404),
        (True, False, "/static/broken_link", 404),
        (False, True, "/static/broken_link", 404),
        (True, True, "/static/broken_link", 404),
    ],
)
def test_symlink_serving_permutations(
    app: OldmanServer,
    symlink_test_directory: Path,
    follow_files: bool,
    follow_dirs: bool,
    path: str,
    expected_status: int,
):
    app.static(
        "/static",
        symlink_test_directory,
        follow_external_symlink_files=follow_files,
        follow_external_symlink_dirs=follow_dirs,
    )

    _, response = app.test_client.get(path)
    assert response.status == expected_status


@pytest.mark.parametrize(
    "dir_name",
    [
        "你好",  # Chinese
        "こんにちは",  # Japanese
        "안녕하세요",  # Korean
        "hello 世界",  # Mixed ASCII and CJK
    ],
)
def test_static_index_with_cjk_directory_name(app: OldmanServer, dir_name: str):
    """Test static file serving with CJK characters in directory names."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create directory with CJK name containing an index file
        cjk_dir = Path(tmpdir) / dir_name
        cjk_dir.mkdir()
        index_file = cjk_dir / "index.html"
        index_content = b"<html>Hello from CJK directory</html>"
        index_file.write_bytes(index_content)

        app.static("/static", tmpdir, index="index.html")

        # Access the CJK directory - should serve index.html
        _, response = app.test_client.get(f"/static/{dir_name}/")
        assert response.status == 200
        assert response.body == index_content
