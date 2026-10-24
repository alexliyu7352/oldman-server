#!/usr/bin/env bash
# Release wheels, built inside a manylinux_2_28 container (the GitHub workflows
# run it there, on x86_64 and on aarch64):
#   1. liburing and Asio from the source archives dependency-lock.json pins
#      (scripts/fetch_sources.py checks their SHA-256); liburing is built into
#      a prefix, for the runtime's io_uring build to link statically;
#   2. for CPython 3.12, 3.13 and 3.14: the extension and that io_uring build,
#      the wheel (hatch_build.py adds both and the tag), and auditwheel repair,
#      which checks what they link and tags the wheel manylinux_2_28.
#
#   scripts/build_wheels.sh [OUT]        repaired wheels go to OUT (default dist)
#
# WHEEL_WORK: scratch directory (default /tmp/oldman-server-wheels).
# WHEEL_JOBS: parallel jobs (default: the CPU count).
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
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
# keeps them within manylinux_2_28; the native code is C++23 (GCC 13 or newer).
if (( $(gcc -dumpversion | cut -d. -f1) < 13 )); then
  echo "GCC 13 or newer is required, found $(gcc -dumpversion)" >&2
  exit 1
fi
gcc -dumpfullversion
"$tools/python" -m pip install --quiet "cmake==4.4.3" "ninja==1.13.2" "pybind11==3.1.0" "uv==0.9.18"
export PATH="$tools:$PATH"

rm -rf "$prefix" "$work/build"
mkdir -p "$prefix" "$work/build"
src="$work/sources"
python "$root/scripts/fetch_sources.py" "$src" liburing asio
(cd "$src/liburing" && ./configure --prefix="$prefix" --includedir="$prefix/include" \
  --libdir="$prefix/lib" --libdevdir="$prefix/lib" --mandir="$work/build/liburing-man" \
  && make -C src -j "$jobs" && make install)

mkdir -p "$out" "$work/dist"
for tag in "${pythons[@]}"; do
  python=/opt/python/$tag/bin/python
  cmake -S "$root" -B "$work/build/extension-$tag" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_PREFIX_PATH="$prefix;$("$tools/python" -m pybind11 --cmakedir)" \
    -DASIO_INCLUDE_DIR="$src/asio/include" \
    -DPython_EXECUTABLE="$python"
  # _native, and the io_uring build of the runtime it depends on.
  cmake --build "$work/build/extension-$tag" --target _native -j "$jobs"
  rm -f "$work/dist"/*.whl
  uv build --wheel --python "$python" --out-dir "$work/dist" "$root"
  # Only the tested platform: auditwheel would otherwise add the earlier glibc
  # tags the binaries happen to satisfy (2.27, 2.26), where nothing is tested.
  auditwheel repair --plat "manylinux_2_28_$arch" --only-plat -w "$out" "$work/dist"/*.whl
done
ls -l "$out"
