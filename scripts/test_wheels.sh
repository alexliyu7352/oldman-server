#!/usr/bin/env bash
# Tests built wheels the way users get them: each installed into a fresh
# virtual environment of its CPython (with the test dependencies), then the
# framework suites served by the native host, and the native host's runs
# (workers, unix sockets, reactor backends). Runs from the source tree, whose
# src/ is not on the path, so the installed package is what is tested.
#
#   scripts/test_wheels.sh WHEEL...      (in the manylinux container, as built)
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
export PATH="/opt/python/cp312-cp312/bin:$PATH"  # uv, as scripts/build_wheels.sh installed it
for wheel in "$@"; do
  tag="$(basename "$wheel" | cut -d- -f3)"            # cp312 ...
  python="/opt/python/$tag-$tag/bin/python"
  venv="$(mktemp -d)/venv"
  uv venv --quiet --python "$python" "$venv"
  uv pip install --quiet --python "$venv/bin/python" "$wheel" --group test
  echo "== $(basename "$wheel")"
  # Not the tree's package: the installed one.
  "$venv/bin/python" -I -c "import oldman_server.native._native as n; assert '/site-packages/' in n.__file__, n.__file__"
  OLDMAN_SERVER_NATIVE_SERVER=true OLDMAN_SERVER_NATIVE_SHARDS=1 \
    "$venv/bin/python" -m pytest -q -p no:cacheprovider tests/framework tests/ext
  "$venv/bin/python" -m pytest -q -p no:cacheprovider tests/test_sanic_native_run.py tests/test_sanic_native_workers.py
done
