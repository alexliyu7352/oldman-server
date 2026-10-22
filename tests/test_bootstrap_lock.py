"""The installer must work from immutable archives without live indexes."""
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import zipfile

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/local_deps.py"


def locked_fixture(tmp_path):
    package = tmp_path / "package"
    (package / "DEBIAN").mkdir(parents=True)
    (package / "usr/bin").mkdir(parents=True)
    # libc6 is already installed on the test host. Never execute its package
    # scripts or install it globally: this tiny archive only owns a probe file.
    (package / "DEBIAN/control").write_text(
        "Package: libc6\nVersion: 0.0-probe\nArchitecture: amd64\n"
        "Maintainer: Probe <probe@local.invalid>\nDescription: extraction fixture\n")
    (package / "usr/bin/locked-compiler-probe").write_text("locked payload\n")
    deps = tmp_path / "deps"
    cache = deps / "debs"
    cache.mkdir(parents=True)
    archive = cache / "fixture.deb"
    subprocess.run(["dpkg-deb", "--build", "--root-owner-group", str(package), str(archive)],
                   check=True, capture_output=True)
    lock = {"schema": 1, "python": "3.12.14", "host_requirements": {}, "packages": [
        {"Package": "libc6", "Version": "0.0-probe", "Filename": "pool/fixture.deb",
         "SHA256": hashlib.sha256(archive.read_bytes()).hexdigest()}]}
    lock_path = tmp_path / "lock.json"
    lock_path.write_text(json.dumps(lock))
    return deps, lock, lock_path


def run_install(deps, lock_path):
    return subprocess.run([sys.executable, str(SCRIPT), str(deps), "--lock", str(lock_path)],
                          capture_output=True, text=True, timeout=15)


def test_locked_package_extracted_even_when_host_package_is_installed(tmp_path):
    deps, _, lock_path = locked_fixture(tmp_path)
    run = run_install(deps, lock_path)
    assert run.returncode == 0, run.stderr
    assert (deps / "sysroot/usr/bin/locked-compiler-probe").read_text() == "locked payload\n"


def test_locked_checksum_mismatch_rejected_before_extraction(tmp_path):
    deps, lock, lock_path = locked_fixture(tmp_path)
    lock["packages"][0]["SHA256"] = "0" * 64
    lock_path.write_text(json.dumps(lock))
    run = run_install(deps, lock_path)
    assert run.returncode != 0 and "checksum mismatch" in run.stderr
    assert not (deps / "sysroot/usr/bin/locked-compiler-probe").exists()


def test_incompatible_host_requirement_is_explicit(tmp_path):
    deps, lock, lock_path = locked_fixture(tmp_path)
    lock["host_requirements"] = {"libc6": "999.0-incompatible"}
    lock_path.write_text(json.dumps(lock))
    run = run_install(deps, lock_path)
    assert run.returncode != 0 and "host version mismatch" in run.stderr


def test_host_preflight_checks_without_creating_dependency_tree(tmp_path):
    deps = tmp_path / "not-created"
    lock_path = tmp_path / "lock.json"
    lock_path.write_text(json.dumps({"schema": 1, "host_requirements": {}, "packages": []}))
    run = subprocess.run(
        [sys.executable, str(SCRIPT), str(deps), "--lock", str(lock_path), "--check"],
        capture_output=True, text=True, timeout=15)
    assert run.returncode == 0, run.stderr
    assert not deps.exists()


def test_host_preflight_rejects_incompatible_host_without_downloads(tmp_path):
    deps = tmp_path / "not-created"
    lock_path = tmp_path / "lock.json"
    lock_path.write_text(json.dumps({"schema": 1,
        "host_requirements": {"libc6": "999.0-incompatible"}, "packages": []}))
    run = subprocess.run(
        [sys.executable, str(SCRIPT), str(deps), "--lock", str(lock_path), "--check"],
        capture_output=True, text=True, timeout=15)
    assert run.returncode != 0 and "host version mismatch" in run.stderr
    assert not deps.exists()


def test_package_origin_and_destination_are_locked(tmp_path):
    deps, lock, lock_path = locked_fixture(tmp_path)
    archive = deps / "debs/fixture.deb"
    origin = tmp_path / "origin.deb"
    archive.rename(origin)
    lock["packages"][0].update(URL=origin.as_uri(), Destination="toolchain")
    lock_path.write_text(json.dumps(lock))
    run = run_install(deps, lock_path)
    assert run.returncode == 0, run.stderr
    assert (deps / "toolchain/usr/bin/locked-compiler-probe").read_text() == "locked payload\n"
    assert not (deps / "sysroot").exists()


def archive_fixture(tmp_path, conda=False):
    payload = b"locked source or runtime\n"
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as tar:
        member = tarfile.TarInfo("lib/runtime.so" if conda else "release/source.cc")
        member.size = len(payload)
        tar.addfile(member, io.BytesIO(payload))
    origin = tmp_path / ("origin.conda" if conda else "origin.tar")
    if conda:
        compressed = subprocess.run(["zstd", "-q", "-c"], input=buffer.getvalue(),
                                    capture_output=True, check=True).stdout
        with zipfile.ZipFile(origin, "w") as archive:
            archive.writestr("metadata.json", '{"conda_pkg_format_version": 2}')
            archive.writestr("pkg-fixture.tar.zst", compressed)
    else:
        origin.write_bytes(buffer.getvalue())
    entry = {"Name": "fixture", "Version": "1", "Filename": origin.name,
             "URL": origin.as_uri(), "SHA256": hashlib.sha256(origin.read_bytes()).hexdigest(),
             "Format": "conda" if conda else "tar", "Destination": "runtime" if conda else "sources/fixture",
             "StripComponents": 0 if conda else 1}
    lock = {"schema": 1, "host_requirements": {}, "packages": [], "archives": [entry]}
    lock_path = tmp_path / "lock.json"
    lock_path.write_text(json.dumps(lock))
    return tmp_path / "deps", lock, lock_path


def test_source_archive_extracts_into_locked_destination(tmp_path):
    deps, _, lock_path = archive_fixture(tmp_path)
    run = run_install(deps, lock_path)
    assert run.returncode == 0, run.stderr
    assert (deps / "sources/fixture/source.cc").read_bytes() == b"locked source or runtime\n"


def test_conda_runtime_extracts_without_installing_an_environment(tmp_path):
    deps, _, lock_path = archive_fixture(tmp_path, conda=True)
    run = run_install(deps, lock_path)
    assert run.returncode == 0, run.stderr
    assert (deps / "runtime/lib/runtime.so").read_bytes() == b"locked source or runtime\n"
    assert not (deps / "runtime/metadata.json").exists()


def test_source_checksum_mismatch_rejected_before_extraction(tmp_path):
    deps, lock, lock_path = archive_fixture(tmp_path)
    lock["archives"][0]["SHA256"] = "0" * 64
    lock_path.write_text(json.dumps(lock))
    run = run_install(deps, lock_path)
    assert run.returncode != 0 and "checksum mismatch" in run.stderr
    assert not (deps / "sources/fixture").exists()


def patched_fixture(tmp_path):
    deps, lock, lock_path = archive_fixture(tmp_path)
    patch = tmp_path / "change.patch"
    patch.write_text("--- a/source.cc\n+++ b/source.cc\n@@ -1 +1 @@\n"
                     "-locked source or runtime\n+patched source\n")
    lock["archives"][0]["Patches"] = [{"File": "change.patch",
        "SHA256": hashlib.sha256(patch.read_bytes()).hexdigest()}]
    lock_path.write_text(json.dumps(lock))
    return deps, lock, lock_path


def test_locked_source_patch_applies_after_fresh_and_repeated_extraction(tmp_path):
    deps, _, lock_path = patched_fixture(tmp_path)
    for _ in range(2):
        run = run_install(deps, lock_path)
        assert run.returncode == 0, run.stderr
        assert (deps / "sources/fixture/source.cc").read_text() == "patched source\n"


def test_modified_patch_is_rejected_before_extracting_source(tmp_path):
    deps, _, lock_path = patched_fixture(tmp_path)
    (tmp_path / "change.patch").write_text("changed after lock")
    run = run_install(deps, lock_path)
    assert run.returncode != 0 and "patch checksum mismatch" in run.stderr
    assert not (deps / "sources/fixture").exists()


def test_inapplicable_patch_cannot_report_a_successful_install(tmp_path):
    deps, lock, lock_path = patched_fixture(tmp_path)
    patch = tmp_path / "change.patch"
    patch.write_text(patch.read_text().replace('-locked source or runtime', '-different source'))
    lock["archives"][0]["Patches"][0]["SHA256"] = hashlib.sha256(patch.read_bytes()).hexdigest()
    lock_path.write_text(json.dumps(lock))
    run = run_install(deps, lock_path)
    assert run.returncode != 0
    assert not (deps / "install-observed.json").exists()
