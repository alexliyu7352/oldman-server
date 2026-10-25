#!/usr/bin/env bash
# Build the native host's library, using the host ABI and a local prefix.
set -euo pipefail
source "$(dirname "$0")/env.sh"
jobs="${PROBE_BUILD_JOBS:-2}"
mkdir -p "$PROBE_DEPS/build" "$PROBE_PREFIX"

# liburing (io_uring reactor backend); its own configure script, not CMake.
cd "$PROBE_DEPS/sources/liburing"
./configure --prefix="$PROBE_PREFIX" --includedir="$PROBE_PREFIX/include" \
  --libdir="$PROBE_PREFIX/lib" --libdevdir="$PROBE_PREFIX/lib" \
  --mandir="$PROBE_DEPS/build/liburing-man" --cc="$PROBE_CC" --cxx="$PROBE_CXX"
make -C src -j "$jobs"
make install
