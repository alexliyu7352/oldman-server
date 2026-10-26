#!/usr/bin/env bash
# What drive.py runs besides this project: the Python entries of
# web-frameworks-benchmark (the-benchmarker/web-frameworks) at the commit
# measured, unpacked into build/framework-comparison/apps, and one virtual
# environment per service in build/framework-comparison/venvs with the
# versions measured, on CPython 3.14 (as the site's python/config.yaml).
# peregrine (manylinux_2_35 wheels) and socketify (no CPython 3.14 wheel) do
# not install on Ubuntu 20.04 and are left out.
#
#   bash probes/framework_comparison/setup.sh [PYTHON]    (default: python3.14)
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
out="$root/build/framework-comparison"
commit=eb2f9094cb29b8bc8098316f6978544188c56043
python="${1:-python3.14}"
apps=(asgineer blacksheep emmett fastapi fastpysgi-wsgi lihil litestar oxapy robyn sanic starlette
      weft-asgi weft-wsgi wred)
uvicorn='uvicorn[standard]==0.54.0 uvloop==0.22.1'
declare -A packages=(
  [fastpysgi-wsgi]='fastpysgi==0.6'
  [weft]='weft-server==0.1.1'
  [wred]='wred==0.13 weft-server==0.1.1'
  [oxapy]='oxapy==0.11.5'
  [robyn]='robyn==0.88.0 uvloop==0.22.1'
  [emmett]='emmett[uvloop]==2.8.2 granian==2.8.5 uvloop==0.22.1'
  [sanic]='sanic==25.12.1 uvloop==0.22.1'
  [sanic-ext]='sanic==25.12.1 sanic-ext==25.12.0 jinja2==3.1.6 pydantic==2.12.5 uvloop==0.22.1'
  [blacksheep]="blacksheep==2.6.4 $uvicorn"
  [starlette]="starlette==1.10.0 $uvicorn"
  [litestar]="litestar==2.25.0 $uvicorn"
  [fastapi]="fastapi==0.162.0 $uvicorn"
  [lihil]="lihil==0.2.41 $uvicorn"
  [asgineer]="asgineer==0.9.5 $uvicorn"
)

mkdir -p "$out/apps" "$out/venvs"
archive="$out/web-frameworks-$commit.tar.gz"
[[ -f "$archive" ]] || curl -sSfL -o "$archive" \
  "https://codeload.github.com/the-benchmarker/web-frameworks/tar.gz/$commit"
for app in "${apps[@]}"; do
  rm -rf "${out:?}/apps/$app"
  tar -xzf "$archive" -C "$out/apps" --strip-components=2 "web-frameworks-$commit/python/$app"
done
for name in "${!packages[@]}"; do
  venv="$out/venvs/$name"
  [[ -x "$venv/bin/python" ]] || uv venv --quiet --python "$python" "$venv"
  # shellcheck disable=SC2086
  uv pip install --quiet --python "$venv/bin/python" ${packages[$name]}
done
