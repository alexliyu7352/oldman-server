"""Runtime and process identity checks for the comparison drivers."""
import hashlib
import importlib.metadata
import os
from pathlib import Path
import sys
import sysconfig


def fingerprint(path):
    path = Path(path).resolve()
    try:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as error:
        raise ValueError(f"cannot fingerprint input {path}") from error
    return {"path": str(path), "sha256": digest}


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
