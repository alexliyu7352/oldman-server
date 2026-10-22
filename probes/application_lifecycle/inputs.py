"""Input and identity gates for matched lifecycle experiments."""
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import sysconfig


def fingerprint(path):
    path = Path(path).resolve()
    try:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as error:
        raise ValueError(f"cannot fingerprint input {path}") from error
    return {"path": str(path), "sha256": digest}


def _check_hash(path, expected):
    if fingerprint(path)["sha256"] != expected:
        raise ValueError(f"input hash mismatch: {path}")


def verify_snapshot(root):
    root = Path(root).resolve()
    try:
        record = json.loads((root / "SNAPSHOT.json").read_text())
    except (OSError, ValueError) as error:
        raise ValueError(f"missing or invalid snapshot manifest: {root}") from error
    if (record.get("schema") != 1 or record.get("source_dirty") is not False or
            not re.fullmatch(r"[0-9a-f]{40}", record.get("source_commit", "")) or
            record.get("python") != sys.version or Path(record["snapshot"]).resolve() != root):
        raise ValueError("snapshot provenance/interpreter mismatch")
    extension = "build/_seastar_probe" + sysconfig.get_config_var("EXT_SUFFIX")
    found = sorted((root / "build").glob("_seastar_probe*.so"))
    if found != [root / extension]:
        raise ValueError("missing or ambiguous snapshot extension")
    source = record["source_sha256"]
    binaries = record["binary_sha256"]
    if not {"python/probe_engine.py", "python/seastar_http.py", ".python-version"} <= source.keys():
        raise ValueError("missing required snapshot source hashes")
    if not {extension, "build/http_load", "build/framework_native"} <= binaries.keys():
        raise ValueError("missing required snapshot binary hashes")
    for relative, digest in (source | binaries).items():
        path = (root / relative).resolve()
        if not path.is_relative_to(root):
            raise ValueError(f"snapshot input escapes its root: {relative}")
        _check_hash(path, digest)
    # The original manifest records ldd inputs from the build workspace. Its
    # copied executables/extensions are frozen here; dependencies stay shared.
    by_name = {Path(relative).name: root / relative for relative in binaries}
    for filename, digest in record["native_libraries"].items():
        path = Path(filename)
        _check_hash(by_name.get(path.name, path), digest)
    return record


def expected_modules(root, record):
    root = Path(root).resolve()
    extension = "build/_seastar_probe" + sysconfig.get_config_var("EXT_SUFFIX")
    return {name: {"path": str(root / relative), "sha256": hashes[relative]}
            for name, relative, hashes in (
                ("_seastar_probe", extension, record["binary_sha256"]),
                ("probe_engine", "python/probe_engine.py", record["source_sha256"]),
                ("seastar_http", "python/seastar_http.py", record["source_sha256"]))}


def verify_loaded(loaded, root, record):
    for name, expected in expected_modules(root, record).items():
        if loaded.get(name) != expected:
            raise ValueError(f"unexpected loaded module {name}: {loaded.get(name)}")


def seastar_libraries(record):
    libraries = {path: digest for path, digest in record["native_libraries"].items()
                 if Path(path).name.startswith("libseastar.so")}
    if len(libraries) != 1:
        raise ValueError("one unambiguous Seastar library required")
    return libraries


def library_environment(record):
    directory = str(Path(next(iter(seastar_libraries(record)))).parent)
    inherited = os.environ.get("LD_LIBRARY_PATH")
    return {"LD_LIBRARY_PATH": directory + (os.pathsep + inherited if inherited else "")}


def verify_seastar_libraries(pid, record):
    maps = Path(f"/proc/{pid}/maps").read_text().splitlines()
    paths = {line.split(maxsplit=5)[5] for line in maps if "libseastar.so" in line}
    observed = {path: fingerprint(path)["sha256"] for path in paths}
    if observed != seastar_libraries(record):
        raise ValueError("unexpected loaded Seastar library")
    return observed


def harness_fingerprint(root):
    root = Path(root).resolve()
    paths = []
    for directory in ("scripts", "probes/framework_json", "probes/final", "probes/application_lifecycle",
                      "probes/blocking_attribution"):
        paths.extend((root / directory).glob("*.py"))
    paths.extend((root / "build").glob("_seastar_probe*.so"))
    profiler = root / "probes/io_business/diagnostic_profile.py"
    if profiler.exists():
        paths.append(profiler)
    paths.extend(root / name for name in ("pyproject.toml", "uv.lock") if (root / name).exists())
    diff = subprocess.check_output(["git", "diff", "--no-ext-diff", "--binary", "HEAD", "--"], cwd=root)
    return {"git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
            "dirty_diff_sha256": hashlib.sha256(diff).hexdigest(),
            "sha256": {str(path.relative_to(root)): fingerprint(path)["sha256"] for path in sorted(paths)}}


def runtime_state():
    if not sys._is_gil_enabled() or sysconfig.get_config_var("Py_GIL_DISABLED"):
        raise ValueError("standard GIL-enabled CPython required")
    jit = getattr(sys, "_jit", None)
    from uvloop.loop import libuv_get_version
    return {"python": sys.version, "executable": str(Path(sys.executable).resolve()),
            "libuv_version": libuv_get_version(),
            "soabi": sysconfig.get_config_var("SOABI"), "gil_enabled": True,
            "jit_available": bool(jit and jit.is_available()), "jit_enabled": bool(jit and jit.is_enabled()),
            "versions": {name: importlib.metadata.version(name) for name in ("uvloop", "aiohttp")}}


def runtime_fingerprint():
    files = {}
    for name in ("uvloop", "aiohttp"):
        distribution = importlib.metadata.distribution(name)
        for relative in distribution.files:
            if relative.suffix in (".py", ".so"):
                path = Path(distribution.locate_file(relative)).resolve()
                files[str(path)] = fingerprint(path)["sha256"]
    return {"state": runtime_state(), "python_binary": fingerprint(sys.executable), "files": files}


def process_identity(pid, parent, executable):
    # This experiment runs directly on the host. Refuse a mismatched procfs/PID
    # namespace rather than silently collecting another process's CPU counters.
    folder = Path("/proc") / str(pid)
    try:
        status = dict(line.split(":", 1) for line in (folder / "status").read_text().splitlines()
                      if ":" in line)
        namespace = os.readlink(folder / "ns/pid")
        actual_executable = (folder / "exe").resolve()
        fields = (folder / "stat").read_text().rsplit(")", 1)[1].split()
        if (int(status["NSpid"].split()[-1]) != pid or int(status["PPid"]) != parent or
                namespace != os.readlink("/proc/self/ns/pid") or
                actual_executable != Path(executable).resolve()):
            raise ValueError(f"process identity mismatch for PID {pid}")
        return {"pid": pid, "procfs_pid": pid, "parent": parent, "pid_namespace": namespace,
                "executable": str(actual_executable), "start_ticks": int(fields[19])}
    except (OSError, KeyError, IndexError) as error:
        raise ValueError(f"unresolved process identity for PID {pid}") from error


def check_identity(identity):
    if process_identity(identity["pid"], identity["parent"], identity["executable"]) != identity:
        raise ValueError("process identity changed during measurement")
