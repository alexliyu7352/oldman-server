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
PROBE_DEPS="$(cd "$PROBE_ROOT/../seastar_deps" && pwd)"
PROBE_PREFIX="$PROBE_DEPS/native-prefix"
export PROBE_SEASTAR_SOURCE="$PROBE_DEPS/sources/seastar-header-lines"
# Seastar with io_uring (liburing in the prefix); build/seastar-header-lines is
# the earlier epoll/linux-aio-only build.
export PROBE_SEASTAR_BUILD="$PROBE_DEPS/build/seastar-uring"
PROBE_CC="$PROBE_DEPS/toolchain/usr/bin/gcc-14"
PROBE_CXX="$PROBE_DEPS/toolchain/usr/bin/g++-14"
export PATH="$PROBE_ROOT/.venv/bin:$PROBE_DEPS/toolchain/usr/bin:$PATH"
export LD_LIBRARY_PATH="$PROBE_DEPS/toolchain/lib:$PROBE_PREFIX/lib:$PROBE_SEASTAR_BUILD${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export LIBRARY_PATH="$PROBE_DEPS/toolchain/lib${LIBRARY_PATH:+:$LIBRARY_PATH}"
export PKG_CONFIG_PATH="$PROBE_PREFIX/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
# Use host headers/glibc; a dependency prefix is not an alternate OS sysroot.
unset PKG_CONFIG_SYSROOT_DIR PKG_CONFIG_LIBDIR
