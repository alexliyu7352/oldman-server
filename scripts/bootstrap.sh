#!/usr/bin/env bash
# Project bootstrap. Native lock compatibility is checked before any downloads.
set -euo pipefail
PROBE_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
python3 "$PROBE_ROOT/scripts/local_deps.py" "$PROBE_ROOT/../seastar_deps" --check
mkdir -p "$PROBE_ROOT/../seastar_deps"
cd "$PROBE_ROOT"
PROBE_DEPS="$(cd ../seastar_deps && pwd)"
export UV_PYTHON_INSTALL_DIR="$PROBE_DEPS/python"
"${PROBE_UV:-uv}" sync --locked --all-groups
"$PROBE_ROOT/.venv/bin/python" scripts/local_deps.py "$PROBE_DEPS"
# The compiler's Debian dev package expects a multiarch runtime location.
# Select the locked GCC 14 runtime, never the host's older libstdc++.
ln -sfn ../../../../../lib/libstdc++.so.6 \
  "$PROBE_DEPS/toolchain/usr/lib/gcc/x86_64-linux-gnu/14/libstdc++.so"
source scripts/env.sh
bash scripts/build_dependencies.sh
bash scripts/build.sh
