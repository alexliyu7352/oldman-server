#!/usr/bin/env bash
# Project bootstrap. Native lock compatibility is checked before any downloads.
# Python is uv's (its shared installs, .python-version's release); the locked
# GCC 14 toolchain goes to PROBE_TOOLCHAIN (/opt/tools/gcc-14), Asio and
# liburing to PROBE_DEPS (build/deps).
set -euo pipefail
PROBE_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
toolchain="${PROBE_TOOLCHAIN:-/opt/tools/gcc-14}"
deps="${PROBE_DEPS:-$PROBE_ROOT/build/deps}"
python3 "$PROBE_ROOT/scripts/local_deps.py" "$toolchain" --check
cd "$PROBE_ROOT"
"${PROBE_UV:-uv}" sync --locked --all-groups
if [[ ! -x "$toolchain/usr/bin/g++-14" ]]; then
  mkdir -p "$toolchain"
  "$PROBE_ROOT/.venv/bin/python" scripts/local_deps.py "$toolchain" --only toolchain
  # The compiler's Debian dev package expects a multiarch runtime location.
  # Select the locked GCC 14 runtime, never the host's older libstdc++.
  ln -sfn ../../../../../lib/libstdc++.so.6 \
    "$toolchain/usr/lib/gcc/x86_64-linux-gnu/14/libstdc++.so"
fi
"$PROBE_ROOT/.venv/bin/python" scripts/fetch_sources.py "$deps/sources" liburing asio
export PROBE_TOOLCHAIN="$toolchain" PROBE_DEPS="$deps"
source scripts/env.sh
bash scripts/build_dependencies.sh
bash scripts/build.sh
