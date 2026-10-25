#!/usr/bin/env bash
# Source this file; all dependency locations are local to this experiment.
PROBE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ ! -x "$PROBE_ROOT/.venv/bin/python" ]]; then
  echo "Project Python environment missing; run uv sync --locked first." >&2
  return 1 2>/dev/null || exit 1
fi
if ! "$PROBE_ROOT/.venv/bin/python" - "$PROBE_ROOT/.python-version" <<'PY'
from pathlib import Path
import sys
import sysconfig

expected = Path(sys.argv[1]).read_text().strip()
actual = ".".join(map(str, sys.version_info[:3]))
if sys.implementation.name != "cpython" or actual != expected:
    raise SystemExit(f"CPython {expected} required; selected {sys.implementation.name} {actual}")
if sysconfig.get_config_var("Py_GIL_DISABLED") or not getattr(sys, "_is_gil_enabled", lambda: True)():
    raise SystemExit("A standard GIL-enabled CPython build is required")
PY
then
  return 1 2>/dev/null || exit 1
fi
# The locked GCC 14 toolchain is a shared tool (scripts/local_deps.py ROOT
# --only toolchain); Asio and liburing are this project's (scripts/bootstrap.sh).
PROBE_TOOLCHAIN="${PROBE_TOOLCHAIN:-/opt/tools/gcc-14}"
PROBE_DEPS="${PROBE_DEPS:-$PROBE_ROOT/build/deps}"
PROBE_PREFIX="$PROBE_DEPS/native-prefix"
PROBE_CC="$PROBE_TOOLCHAIN/usr/bin/gcc-14"
PROBE_CXX="$PROBE_TOOLCHAIN/usr/bin/g++-14"
export PATH="$PROBE_ROOT/.venv/bin:$PROBE_TOOLCHAIN/usr/bin:$PATH"
export LD_LIBRARY_PATH="$PROBE_TOOLCHAIN/lib:$PROBE_PREFIX/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export LIBRARY_PATH="$PROBE_TOOLCHAIN/lib${LIBRARY_PATH:+:$LIBRARY_PATH}"
# Seastar (with io_uring, liburing in the prefix): not the native host's; only
# the comparison's load generator and native reference server use it. Built
# where its locked sources were extracted into PROBE_DEPS.
if [[ -d "$PROBE_DEPS/sources/seastar-header-lines" ]]; then
  export PROBE_SEASTAR_SOURCE="$PROBE_DEPS/sources/seastar-header-lines"
  export PROBE_SEASTAR_BUILD="$PROBE_DEPS/build/seastar-uring"
  export PATH="$PROBE_DEPS/seastar-tools/usr/bin:$PATH"
  export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$PROBE_SEASTAR_BUILD"
fi
export PKG_CONFIG_PATH="$PROBE_PREFIX/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
# Use host headers/glibc; a dependency prefix is not an alternate OS sysroot.
unset PKG_CONFIG_SYSROOT_DIR PKG_CONFIG_LIBDIR
