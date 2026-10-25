"""What a build of this tree publishes.

The working tree also holds private material that git excludes only through
.git/info/exclude, which hatchling does not read: a source distribution built
here must still contain only the public sources, an allowlist.
A wheel is only built with the native extension; one built without it (from
the source distribution, say) would install a package without its native
host, so that is refused unless asked for.
"""
from fnmatch import fnmatch
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile

import pytest

ROOT = Path(__file__).resolve().parents[1]
PUBLIC = ("src/", "native/", "upstream/", "scripts/", "LICENSES/",
          "CMakeLists.txt", "dependency-lock.json", "hatch_build.py", "LICENSE",
          "README.md", "pyproject.toml", "PKG-INFO", ".gitignore")

pytestmark = pytest.mark.skipif(shutil.which("uv") is None, reason="needs uv")


def build(*arguments, cwd=ROOT, env=None):
    return subprocess.run(["uv", "build", "--python", sys.executable, *arguments],
                          cwd=cwd, capture_output=True,
                          text=True, timeout=300, env={**os.environ, **(env or {})})


@pytest.fixture(scope="module")
def sdist(tmp_path_factory):
    out = tmp_path_factory.mktemp("sdist")
    result = build("--sdist", "--out-dir", str(out))
    assert result.returncode == 0, result.stderr
    (archive,) = out.glob("*.tar.gz")
    return archive


def test_sdist_holds_only_public_sources(sdist):
    with tarfile.open(sdist) as archive:
        names = [name.split("/", 1)[1] for name in archive.getnames() if "/" in name]
    files = [name for name in names if name and not name.endswith("/")]
    assert files
    assert [name for name in files if not name.startswith(PUBLIC)] == []
    # Nor anything this clone excludes privately (where it has such entries).
    exclude = ROOT / ".git/info/exclude"
    private = [line.strip().strip("/") for line in
               (exclude.read_text().splitlines() if exclude.is_file() else [])
               if line.strip() and not line.startswith("#")]
    assert [name for name in files
            if any(name == path or name.startswith(path + "/") or fnmatch(name, path)
                   for path in private)] == []
    assert "src/oldman_server/app.py" in files and "native/server.cc" in files


def test_wheel_without_the_extension_is_refused(sdist, tmp_path):
    with tarfile.open(sdist) as archive:
        archive.extractall(tmp_path, filter="data")
    (tree,) = [path for path in tmp_path.iterdir() if path.is_dir()]
    result = build("--wheel", "--out-dir", str(tmp_path / "dist"), cwd=tree)
    assert result.returncode != 0
    assert "native extension" in result.stdout + result.stderr
    result = build("--wheel", "--out-dir", str(tmp_path / "dist"), cwd=tree,
                   env={"OLDMAN_SERVER_PURE_WHEEL": "1"})
    assert result.returncode == 0, result.stderr
    assert [path.name for path in (tmp_path / "dist").glob("*-py3-none-any.whl")]


def test_runtime_requirements_are_not_pinned():
    # Exact versions the tests rely on belong in the lock and its constraints
    # (uv.lock, [tool.uv]), not in what users install the package with.
    import tomllib
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    assert [requirement for requirement in project["dependencies"] if "==" in requirement] == []
