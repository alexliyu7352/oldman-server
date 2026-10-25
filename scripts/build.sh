#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
# Another interpreter's extension (e.g. PROBE_PYTHON=build/venv312/bin/python
# PROBE_PROJECT_BUILD=build/cmake312 scripts/build.sh _native): its own build
# directory; arguments name the targets (default: all).
python="${PROBE_PYTHON:-$PROBE_ROOT/.venv/bin/python}"
project_build="${PROBE_PROJECT_BUILD:-$PROBE_ROOT/build}"
cmake -S "$PROBE_ROOT" -B "$project_build" -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_COMPILER="$PROBE_CXX" \
  -DCMAKE_PREFIX_PATH="$PROBE_PREFIX;$("$python" -m pybind11 --cmakedir)" \
  -DASIO_INCLUDE_DIR="$PROBE_DEPS/sources/asio/include" \
  -DPython_EXECUTABLE="$python"
cmake --build "$project_build" -j "${PROBE_BUILD_JOBS:-2}" ${1:+--target "$@"}
