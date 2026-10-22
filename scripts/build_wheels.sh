#!/usr/bin/env bash
# Release wheels, built inside a manylinux_2_28 container (the GitHub workflows
# run it there, on x86_64 and on aarch64):
#   1. the native dependencies from the source archives dependency-lock.json
#      pins (scripts/fetch_sources.py checks their SHA-256), into a prefix;
#   2. Seastar, as scripts/build.sh configures it, installed into the prefix;
#   3. for CPython 3.12, 3.13 and 3.14: the extension, the wheel
#      (hatch_build.py adds the extension and its tag), and auditwheel repair,
#      which copies the dependencies into the wheel and tags it manylinux_2_28.
#
#   scripts/build_wheels.sh [OUT]        repaired wheels go to OUT (default dist)
#   scripts/build_wheels.sh --prepare    steps 1 and 2 only (CI caches the prefix
#                                        before building the wheels)
#
# WHEEL_WORK: scratch directory (default /tmp/oldman-server-wheels); its
# prefix/ holds the built dependencies and Seastar and is worth caching (it is
# rebuilt when the lock or this script changes: prefix/.stamp).
# WHEEL_JOBS: parallel jobs (default: the CPU count).
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
prepare_only=false
if [[ "${1:-}" == "--prepare" ]]; then
  prepare_only=true
  shift
fi
out="$(realpath -m "${1:-$root/dist}")"
work="${WHEEL_WORK:-/tmp/oldman-server-wheels}"
prefix="$work/prefix"
jobs="${WHEEL_JOBS:-$(nproc)}"
arch="$(uname -m)"
pythons=(cp312-cp312 cp313-cp313 cp314-cp314)
tools=/opt/python/cp312-cp312/bin

if [[ ! -d /opt/python || ! -f /etc/almalinux-release ]]; then
  echo "run this inside a manylinux_2_28 container (quay.io/pypa/manylinux_2_28_$arch)" >&2
  exit 1
fi
# gcc-toolset links what the system libstdc++ lacks into the binaries, which
# keeps them within manylinux_2_28; Seastar needs C++23 (GCC 13 or newer).
if (( $(gcc -dumpversion | cut -d. -f1) < 13 )); then
  echo "GCC 13 or newer is required, found $(gcc -dumpversion)" >&2
  exit 1
fi

# Headers and small libraries the image's distribution provides; auditwheel
# copies those the build links (lz4) into the wheels.
dnf install -y --setopt=install_weak_deps=False \
  bzip2 zlib-devel lz4-devel lksctp-tools-devel valgrind-devel \
  perl-IPC-Cmd perl-Time-Piece perl-Data-Dumper
# Seastar's file support includes the XFS headers (xfs/xfs.h), which AlmaLinux 8
# ships in its powertools repository.
dnf install -y --setopt=install_weak_deps=False --enablerepo=powertools xfsprogs-devel
"$tools/python" -m pip install --quiet "cmake==4.4.3" "ninja==1.13.2" "pybind11==3.1.0" "uv==0.9.18"
export PATH="$tools:$PATH"

# What the prefix is built from: the lock, patches and scripts, and the image's
# compiler and the distribution packages above (versions not in the lock).
packages=(lz4-devel lksctp-tools-devel valgrind-devel zlib-devel xfsprogs-devel)
gcc -dumpfullversion
rpm -q "${packages[@]}"
stamp="$( (cat "$root/dependency-lock.json" "$root"/patches/* "$root/scripts/fetch_sources.py" "$0"
  gcc -dumpfullversion; rpm -q "${packages[@]}") | sha256sum | cut -d' ' -f1)-$arch"
if [[ "$(cat "$prefix/.stamp" 2>/dev/null)" != "$stamp" ]]; then
  rm -rf "$prefix" "$work/build"
  mkdir -p "$prefix" "$work/build"
  python "$root/scripts/fetch_sources.py" "$work/sources"
  src="$work/sources"
  build="$work/build"

  # ragel generates Seastar's HTTP parsers (a build tool only).
  (cd "$src/ragel" && ./configure --prefix="$prefix" --disable-manual CXXFLAGS="-O2 -std=gnu++11" \
    && make -j "$jobs" && make install)

  (cd "$src/boost" && ./bootstrap.sh --with-libraries=filesystem,program_options,thread,test --prefix="$prefix" \
    && ./b2 --build-dir="$build/boost" --prefix="$prefix" \
      --with-filesystem --with-program_options --with-thread --with-test \
      toolset=gcc cxxstd=23 variant=release link=shared threading=multi \
      --layout=system -j "$jobs" install)

  mkdir -p "$build/openssl"
  (cd "$build/openssl" && "$src/openssl/Configure" "linux-$arch" \
    --prefix="$prefix" --openssldir="$prefix/ssl" --libdir=lib shared no-tests \
    && make -j "$jobs" && make install_sw)

  for library in fmt yaml-cpp c-ares protobuf; do
    case "$library" in
      fmt) options=(-DFMT_TEST=OFF -DFMT_DOC=OFF) ;;
      yaml-cpp) options=(-DYAML_CPP_BUILD_TESTS=OFF -DYAML_CPP_BUILD_TOOLS=OFF -DYAML_BUILD_SHARED_LIBS=ON) ;;
      c-ares) options=(-DCARES_SHARED=ON -DCARES_STATIC=OFF -DCARES_BUILD_TESTS=OFF -DCARES_BUILD_TOOLS=OFF) ;;
      protobuf) options=(-Dprotobuf_BUILD_TESTS=OFF -Dprotobuf_BUILD_SHARED_LIBS=ON -Dprotobuf_WITH_ZLIB=OFF) ;;
    esac
    cmake -S "$src/$library" -B "$build/$library" -G Ninja \
      -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX="$prefix" -DCMAKE_INSTALL_LIBDIR=lib \
      -DCMAKE_POLICY_VERSION_MINIMUM=3.5 -DBUILD_SHARED_LIBS=ON -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
      "${options[@]}"
    cmake --build "$build/$library" -j "$jobs"
    cmake --install "$build/$library"
  done

  (cd "$src/liburing" && ./configure --prefix="$prefix" --includedir="$prefix/include" \
    --libdir="$prefix/lib" --libdevdir="$prefix/lib" --mandir="$build/liburing-man" \
    && make -C src -j "$jobs" && make install)

  # Topology only: no plugins (a bundled hwloc would load the host's, built
  # against the host's hwloc, and crash) and none of their libraries (udev, ltdl).
  (cd "$src/hwloc" && ./configure --prefix="$prefix" --enable-shared --disable-static \
    --disable-plugins --disable-libudev --disable-libxml2 --disable-cairo --disable-pci \
    --disable-opencl --disable-cuda --disable-nvml --disable-rsmi --disable-levelzero \
    --disable-gl --disable-io --disable-doxygen \
    && make -j "$jobs" && make install)

  # As scripts/build.sh configures it for development. --as-needed (the
  # default of Ubuntu's linker, not of this image's): Seastar uses only the
  # SCTP header's constants, so libsctp (LGPL) is not linked into the wheels.
  cmake -S "$src/seastar" -B "$build/seastar" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_STANDARD=23 \
    -DCMAKE_CXX_FLAGS="-DSEASTAR_DEFAULT_ALLOCATOR" \
    -DCMAKE_SHARED_LINKER_FLAGS="-Wl,--as-needed" \
    -DCMAKE_PREFIX_PATH="$prefix" -DCMAKE_INSTALL_PREFIX="$prefix" -DCMAKE_INSTALL_LIBDIR=lib \
    -DBOOST_ROOT="$prefix" -DOPENSSL_ROOT_DIR="$prefix" \
    -Dc-ares_LIBRARY="$prefix/lib/libcares.so" -Dc-ares_INCLUDE_DIR="$prefix/include" \
    -DBUILD_SHARED_LIBS=ON -DSeastar_API_LEVEL=9 -DSeastar_IO_URING=ON \
    -DSeastar_LTTNG=OFF -DSeastar_DPDK=OFF -DSeastar_GNUTLS=OFF -DSeastar_DOCS=OFF \
    -DSeastar_APPS=OFF -DSeastar_DEMOS=OFF -DSeastar_TESTING=OFF
  # All of its libraries: the install includes seastar_testing and seastar_perf_testing.
  cmake --build "$build/seastar" -j "$jobs"
  cmake --install "$build/seastar"
  echo "$stamp" > "$prefix/.stamp"
fi
if $prepare_only; then
  exit 0
fi

export LD_LIBRARY_PATH="$prefix/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
mkdir -p "$out" "$work/dist"
for tag in "${pythons[@]}"; do
  python=/opt/python/$tag/bin/python
  cmake -S "$root" -B "$work/build/extension-$tag" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_PREFIX_PATH="$prefix;$("$tools/python" -m pybind11 --cmakedir)" \
    -DOPENSSL_ROOT_DIR="$prefix" \
    -Dc-ares_LIBRARY="$prefix/lib/libcares.so" -Dc-ares_INCLUDE_DIR="$prefix/include" \
    -DPython_EXECUTABLE="$python"
  cmake --build "$work/build/extension-$tag" --target _native -j "$jobs"
  rm -f "$work/dist"/*.whl
  uv build --wheel --python "$python" --out-dir "$work/dist" "$root"
  # Only the tested platform: auditwheel would otherwise add the earlier glibc
  # tags the binaries happen to satisfy (2.27, 2.26), where nothing is tested.
  auditwheel repair --plat "manylinux_2_28_$arch" --only-plat -w "$out" "$work/dist"/*.whl
done
ls -l "$out"
