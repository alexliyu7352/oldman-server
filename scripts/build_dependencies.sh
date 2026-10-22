#!/usr/bin/env bash
# Build only missing/newer libraries, using the host ABI and a local prefix.
set -euo pipefail
source "$(dirname "$0")/env.sh"
jobs="${PROBE_BUILD_JOBS:-2}"
mkdir -p "$PROBE_DEPS/build" "$PROBE_PREFIX"

cd "$PROBE_DEPS/sources/boost"
./bootstrap.sh --with-libraries=filesystem,program_options,thread,test --prefix="$PROBE_PREFIX"
printf 'using gcc : 14 : "%s" ;\n' "$PROBE_CXX" > "$PROBE_DEPS/build/boost-user-config.jam"
./b2 --user-config="$PROBE_DEPS/build/boost-user-config.jam" \
  --build-dir="$PROBE_DEPS/build/boost" --prefix="$PROBE_PREFIX" \
  --with-filesystem --with-program_options --with-thread --with-test \
  toolset=gcc-14 cxxstd=23 variant=release link=shared threading=multi \
  --layout=system -j "$jobs" install

mkdir -p "$PROBE_DEPS/build/openssl"
cd "$PROBE_DEPS/build/openssl"
CC="$PROBE_CC" "$PROBE_DEPS/sources/openssl/Configure" linux-x86_64 \
  --prefix="$PROBE_PREFIX" --openssldir="$PROBE_PREFIX/ssl" --libdir=lib shared no-tests
make -j "$jobs"
make install_sw

for library in fmt yaml-cpp c-ares; do
  case "$library" in
    fmt) options=(-DFMT_TEST=OFF -DFMT_DOC=OFF) ;;
    yaml-cpp) options=(-DYAML_CPP_BUILD_TESTS=OFF -DYAML_CPP_BUILD_TOOLS=OFF -DYAML_BUILD_SHARED_LIBS=ON) ;;
    c-ares) options=(-DCARES_SHARED=ON -DCARES_STATIC=OFF -DCARES_BUILD_TESTS=OFF -DCARES_BUILD_TOOLS=OFF) ;;
  esac
  CC="$PROBE_CC" CXX="$PROBE_CXX" cmake -S "$PROBE_DEPS/sources/$library" -B "$PROBE_DEPS/build/$library" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="$PROBE_PREFIX" -DCMAKE_INSTALL_LIBDIR=lib \
    -DCMAKE_POLICY_VERSION_MINIMUM=3.5 -DBUILD_SHARED_LIBS=ON \
    "${options[@]}"
  cmake --build "$PROBE_DEPS/build/$library" -j "$jobs"
  cmake --install "$PROBE_DEPS/build/$library"
done

# liburing (io_uring reactor backend); its own configure script, not CMake.
cd "$PROBE_DEPS/sources/liburing"
./configure --prefix="$PROBE_PREFIX" --includedir="$PROBE_PREFIX/include" \
  --libdir="$PROBE_PREFIX/lib" --libdevdir="$PROBE_PREFIX/lib" \
  --mandir="$PROBE_DEPS/build/liburing-man" --cc="$PROBE_CC" --cxx="$PROBE_CXX"
make -C src -j "$jobs"
make install
