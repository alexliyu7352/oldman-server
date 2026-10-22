"""scripts/fetch_sources.py reuses an unpacked archive only while both the
archive and its patches are the ones it was unpacked with."""
import hashlib
import importlib.util
import io
from pathlib import Path
import tarfile

ROOT = Path(__file__).resolve().parents[1]


def load():
    spec = importlib.util.spec_from_file_location("fetch_sources", ROOT / "scripts/fetch_sources.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_changed_patch_is_applied_to_a_fresh_unpack(tmp_path):
    fetch_sources = load()
    fetch_sources.ROOT = tmp_path  # patch files are relative to the root
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w:gz") as archive:
        content = b"a\nb\n"
        info = tarfile.TarInfo("top/f.txt")
        info.size = len(content)
        archive.addfile(info, io.BytesIO(content))
    archive = tmp_path / "x.tar.gz"
    archive.write_bytes(data.getvalue())

    def patch(line):
        text = f"--- a/f.txt\n+++ b/f.txt\n@@ -1,2 +1,2 @@\n a\n-b\n+{line}\n".encode()
        (tmp_path / f"{line}.patch").write_bytes(text)
        return {"File": f"{line}.patch", "SHA256": hashlib.sha256(text).hexdigest()}

    entry = {"Name": "x", "SHA256": hashlib.sha256(archive.read_bytes()).hexdigest(),
             "StripComponents": 1, "Patches": [patch("first")]}
    fetch_sources.unpack(entry, archive, tmp_path / "src")
    assert (tmp_path / "src/x/f.txt").read_text().split() == ["a", "first"]
    entry["Patches"] = [patch("second")]
    fetch_sources.unpack(entry, archive, tmp_path / "src")
    assert (tmp_path / "src/x/f.txt").read_text().split() == ["a", "second"]
    fetch_sources.unpack(entry, archive, tmp_path / "src")  # unchanged: reused
    assert (tmp_path / "src/x/f.txt").read_text().split() == ["a", "second"]
