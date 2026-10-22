#!/usr/bin/env python3
"""Build/run focused native header allocation and wire tests; source env.sh first."""
import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess

root = Path(__file__).resolve().parents[1]
deps = root.parent / "seastar_deps"
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--seastar-build", type=Path,
                    default=Path(os.environ.get("PROBE_SEASTAR_BUILD", deps / "build/seastar-header-lines")))
build = parser.parse_args().seastar_build.resolve()
entry = next(item for item in json.loads((build / "compile_commands.json").read_text())
             if item["file"].endswith("/src/http/reply.cc"))
command = shlex.split(entry["command"])
command = command[:command.index("-o")]
binary = root / "build/test_reply_headers"
command += [str(root / "tests/native/reply_headers.cc"),
            "-o", str(binary), str(build / "libseastar.so"),
            str(deps / "native-prefix/lib/libboost_program_options.so"),
            str(deps / "native-prefix/lib/libfmt.so"), "-pthread"]
subprocess.run(command, check=True)
environment = os.environ | {"LD_LIBRARY_PATH": str(build) + ":" + os.environ.get("LD_LIBRARY_PATH", "")}
subprocess.run([str(binary), "--smp", "1", "--cpuset", str(min(os.sched_getaffinity(0))),
                "--reactor-backend", "epoll", "--overprovisioned",
                "--default-log-level", "warn"], check=True, env=environment)
