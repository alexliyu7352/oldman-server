"""Process helpers the comparison drivers share: JSON results, process
samples, stopping a child, and the native libraries a run loaded."""
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]


def atomic_json(path, value):
    temp = path.with_suffix(path.suffix+".tmp")
    temp.write_text(json.dumps(value, indent=2)+"\n")
    temp.replace(path)


def process_sample(pids):
    rows = []
    for pid in pids:
        directory = Path(f"/proc/{pid}")
        stat = (directory/"stat").read_text().rsplit(")", 1)[1].split()
        mem = {}
        for line in (directory/"smaps_rollup").read_text().splitlines()[1:]:
            parts = line.split()
            if len(parts) == 3 and parts[2] == "kB": mem[parts[0].rstrip(":")] = int(parts[1])*1024
        rows.append({"pid": pid, "cpu_ticks": int(stat[11])+int(stat[12]), "memory": mem})
    return {"time": time.monotonic(), "processes": rows,
            "pss_bytes": sum(r["memory"]["Pss"] for r in rows),
            "rss_bytes": sum(r["memory"]["Rss"] for r in rows)}


def stop_process(p):
    if p is None:
        return
    try:
        # Every caller starts a new session. The owned process group can
        # outlive its leader, including workers that ignore SIGTERM.
        try:
            os.killpg(p.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            p.wait(3)
        except subprocess.TimeoutExpired:
            pass
        finally:
            try:
                os.killpg(p.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        p.wait()
    finally:
        for pipe in (p.stdout, p.stderr, p.stdin):
            if pipe is not None:
                pipe.close()


def native_libraries():
    """Resolve ldd in the actual LD_LIBRARY_PATH, and hash loaded library bytes."""
    import aiohttp._http_parser
    binaries = [ROOT/"build/http_load", *list((ROOT/"src/oldman_server/native").glob("_native*.so")),
                Path(sys.executable).resolve(), Path(aiohttp._http_parser.__file__)]
    paths = set(binaries)
    for binary in binaries:
        output = subprocess.check_output(["ldd", str(binary)], text=True)
        for filename in re.findall(r"(/[^\s]+)\s+\(", output): paths.add(Path(filename).resolve())
    return {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)}
