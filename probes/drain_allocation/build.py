"""Build only the diagnostic interposer with the existing native ABI flags."""
import json
from pathlib import Path
import shlex
import subprocess
import sys

root = Path(__file__).resolve().parents[2]
commands = subprocess.check_output(["ninja", "-C", str(root / "build"), "-t", "commands", "probe_runtime"], text=True)
command = shlex.split(next(line for line in commands.splitlines() if " -c " in line and "/native/server.cc" in line))
for flag, value in {"-c": str(Path(__file__).with_name("trace.cc")), "-o": str(root / "build/drain_trace.o"),
                    "-MF": str(root / "build/drain_trace.d")}.items():
    command[command.index(flag) + 1] = value
command.extend(["-I" + str(root / "native"), "-Wall", "-Wextra", "-Werror"])
link = [command[0], "-shared", str(root / "build/drain_trace.o"), "-ldl", "-pthread", "-o", str(root / "build/drain_trace.so")]
with Path(sys.argv[1]).open("x") as stream:
    json.dump([command, link], stream, indent=2)
for cmd in (command, link):
    subprocess.run(cmd, cwd=root / "build", check=True)
