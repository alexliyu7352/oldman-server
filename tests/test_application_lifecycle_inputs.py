"""Frozen inputs and live process identities are mandatory benchmark evidence."""
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import sysconfig

import pytest

from probes.application_lifecycle import inputs


def test_selected_library_overrides_inherited_candidate_and_checks_actual_map(tmp_path, monkeypatch):
    # Real loader behavior, without initializing Seastar: two tiny DSOs with
    # the same filename let the child distinguish A from B independently.
    records = []
    for name, marker in (("baseline", 11), ("candidate", 22)):
        folder = tmp_path / name
        folder.mkdir()
        source = folder / "marker.c"
        source.write_text(f"int marker(void) {{ return {marker}; }}\n")
        library = folder / "libseastar.so"
        subprocess.run(["cc", "-shared", "-fPIC", str(source), "-o", str(library)], check=True)
        records.append({"native_libraries": {str(library): inputs.fingerprint(library)["sha256"]}})
    monkeypatch.setenv("LD_LIBRARY_PATH", str(tmp_path / "candidate"))
    code = "import ctypes,sys; print(ctypes.CDLL('libseastar.so').marker(),flush=True); sys.stdin.readline()"
    for number, record in enumerate(records):
        with subprocess.Popen([sys.executable, "-c", code],
                env=os.environ | inputs.library_environment(record),
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True) as child:
            try:
                assert child.stdout.readline().strip() == str((11, 22)[number])
                assert inputs.verify_seastar_libraries(child.pid, record) == record["native_libraries"]
                with pytest.raises(ValueError, match="Seastar"):
                    inputs.verify_seastar_libraries(child.pid, records[1-number])
            finally:
                child.communicate("quit\n", timeout=10)


@pytest.mark.parametrize("libraries", [{}, {"/a/libseastar.so": "a", "/b/libseastar.so": "b"}])
def test_unknown_or_ambiguous_seastar_library_is_rejected(libraries):
    with pytest.raises(ValueError, match="Seastar"):
        inputs.library_environment({"native_libraries": libraries})


@pytest.fixture
def snapshot(tmp_path):
    root = tmp_path / "snapshot"
    root.mkdir()
    extension = "build/_seastar_probe" + sysconfig.get_config_var("EXT_SUFFIX")
    source = ["python/probe_engine.py", "python/seastar_http.py", ".python-version"]
    binaries = [extension, "build/http_load", "build/framework_native"]
    hashes = {}
    for name in source + binaries:
        path = root / name
        path.parent.mkdir(exist_ok=True)
        path.write_bytes(name.encode())
        hashes[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    record = {"schema": 1, "source_commit": "a" * 40, "source_dirty": False,
              "python": sys.version, "snapshot": str(root),
              "source_sha256": {name: hashes[name] for name in source},
              "binary_sha256": {name: hashes[name] for name in binaries},
              "native_libraries": {}}
    (root / "SNAPSHOT.json").write_text(json.dumps(record))
    return root, record


@pytest.mark.parametrize("relative", ["python/probe_engine.py", "build/http_load"])
def test_modified_snapshot_source_or_binary_is_rejected(snapshot, relative):
    root, _ = snapshot
    inputs.verify_snapshot(root)
    (root / relative).write_text("modified after freeze")
    with pytest.raises(ValueError, match="hash"):
        inputs.verify_snapshot(root)


def test_ambiguous_extension_is_rejected(snapshot):
    root, _ = snapshot
    (root / "build/_seastar_probe.other.so").write_text("different ABI")
    with pytest.raises(ValueError, match="extension"):
        inputs.verify_snapshot(root)


def test_shared_dependency_change_is_rejected(snapshot, tmp_path):
    root, record = snapshot
    library = tmp_path / "libseastar.so"
    library.write_bytes(b"frozen dependency")
    record["native_libraries"][str(library)] = hashlib.sha256(library.read_bytes()).hexdigest()
    (root / "SNAPSHOT.json").write_text(json.dumps(record))
    inputs.verify_snapshot(root)
    library.write_bytes(b"new dependency")
    with pytest.raises(ValueError, match="hash"):
        inputs.verify_snapshot(root)


def test_original_build_paths_map_to_frozen_copies(snapshot, tmp_path):
    root, record = snapshot
    original = tmp_path / "workspace/build/http_load"
    record["native_libraries"][str(original)] = record["binary_sha256"]["build/http_load"]
    (root / "SNAPSHOT.json").write_text(json.dumps(record))
    # The original build may be gone; the copied frozen client is the input.
    inputs.verify_snapshot(root)


@pytest.mark.parametrize("module", ["probe_engine", "_seastar_probe"])
def test_identical_bytes_loaded_from_wrong_tree_are_rejected(snapshot, module):
    root, record = snapshot
    loaded = inputs.expected_modules(root, record)
    inputs.verify_loaded(loaded, root, record)
    wrong = copy.deepcopy(loaded)
    wrong[module]["path"] = str(root.parent / "wrong-tree" / Path(wrong[module]["path"]).name)
    with pytest.raises(ValueError, match="module"):
        inputs.verify_loaded(wrong, root, record)


def test_missing_or_mismatched_loaded_module_is_rejected(snapshot):
    root, record = snapshot
    loaded = inputs.expected_modules(root, record)
    loaded["_seastar_probe"]["sha256"] = "wrong"
    with pytest.raises(ValueError, match="module"):
        inputs.verify_loaded(loaded, root, record)
    del loaded["_seastar_probe"]
    with pytest.raises(ValueError, match="module"):
        inputs.verify_loaded(loaded, root, record)


def test_harness_dirty_diff_and_untracked_source_changes_are_detected(tmp_path):
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    source = tmp_path / "scripts/example.py"
    source.parent.mkdir()
    source.write_text("original")
    subprocess.run(["git", "-C", str(tmp_path), "add", "."], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "-c", "user.name=Fixture", "-c", "user.email=fixture@example.test",
                    "commit", "-qm", "fixture"], check=True)
    before = inputs.harness_fingerprint(tmp_path)
    source.write_text("tracked change")
    changed = inputs.harness_fingerprint(tmp_path)
    assert changed["dirty_diff_sha256"] != before["dirty_diff_sha256"]
    untracked = source.with_name("new_module.py")
    untracked.write_text("untracked source")
    assert inputs.harness_fingerprint(tmp_path) != changed


def test_identity_rejects_wrong_parent_and_reused_pid_evidence():
    identity = inputs.process_identity(os.getpid(), os.getppid(), sys.executable)
    inputs.check_identity(identity)
    with pytest.raises(ValueError, match="identity"):
        inputs.process_identity(os.getpid(), os.getppid() + 1, sys.executable)
    changed = identity | {"start_ticks": identity["start_ticks"] + 1}
    with pytest.raises(ValueError, match="identity"):
        inputs.check_identity(changed)


def test_runtime_gate_rejects_disabled_gil(monkeypatch):
    monkeypatch.setattr(sys, "_is_gil_enabled", lambda: False)
    with pytest.raises(ValueError, match="GIL"):
        inputs.runtime_state()
