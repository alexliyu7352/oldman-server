#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
seastar=()
if [[ -n "${PROBE_SEASTAR_SOURCE:-}" ]]; then
  # The comparison's measurement tools (env.sh): Seastar first.
  cmake -S "$PROBE_SEASTAR_SOURCE" -B "$PROBE_SEASTAR_BUILD" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_STANDARD=23 \
    -DCMAKE_CXX_COMPILER="$PROBE_CXX" \
    -DCMAKE_CXX_FLAGS="-DSEASTAR_DEFAULT_ALLOCATOR -isystem $PROBE_DEPS/seastar-tools/usr/include" \
    -DCMAKE_PREFIX_PATH="$PROBE_PREFIX" -DBOOST_ROOT="$PROBE_PREFIX" \
    -DOPENSSL_ROOT_DIR="$PROBE_PREFIX" \
    -Dc-ares_LIBRARY="$PROBE_PREFIX/lib/libcares.so" -Dc-ares_INCLUDE_DIR="$PROBE_PREFIX/include" \
    -DBUILD_SHARED_LIBS=ON -DSeastar_API_LEVEL=9 -DSeastar_IO_URING=ON \
    -DSeastar_LTTNG=OFF -DSeastar_DPDK=OFF -DSeastar_GNUTLS=OFF -DSeastar_DOCS=OFF \
    -DSeastar_EXCLUDE_TESTS_FROM_ALL=ON -DSeastar_EXCLUDE_DEMOS_FROM_ALL=ON \
    -DSeastar_EXCLUDE_APPS_FROM_ALL=ON -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
  cmake --build "$PROBE_SEASTAR_BUILD" --target seastar -j "${PROBE_BUILD_JOBS:-2}"
  seastar=(-DSeastar_DIR="$PROBE_SEASTAR_BUILD" -DCMAKE_MODULE_PATH="$PROBE_SEASTAR_SOURCE/cmake"
           -DOPENSSL_ROOT_DIR="$PROBE_PREFIX"
           -Dc-ares_LIBRARY="$PROBE_PREFIX/lib/libcares.so" -Dc-ares_INCLUDE_DIR="$PROBE_PREFIX/include")
fi
# Another interpreter's extension (e.g. PROBE_PYTHON=build/venv312/bin/python
# PROBE_PROJECT_BUILD=build/cmake312 scripts/build.sh _native): its own build
# directory; arguments name the targets (default: all).
python="${PROBE_PYTHON:-$PROBE_ROOT/.venv/bin/python}"
project_build="${PROBE_PROJECT_BUILD:-$PROBE_ROOT/build}"
cmake -S "$PROBE_ROOT" -B "$project_build" -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_COMPILER="$PROBE_CXX" \
  -DCMAKE_PREFIX_PATH="${PROBE_SEASTAR_BUILD:+$PROBE_SEASTAR_BUILD;}$PROBE_PREFIX;$("$python" -m pybind11 --cmakedir)" \
  -DASIO_INCLUDE_DIR="$PROBE_DEPS/sources/asio/include" \
  "${seastar[@]}" \
  -DPython_EXECUTABLE="$python"
cmake --build "$project_build" -j "${PROBE_BUILD_JOBS:-2}" ${1:+--target "$@"}
