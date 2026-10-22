#!/usr/bin/env bash
# Tests built wheels the way users get them: each installed into a fresh
# virtual environment of its CPython, first alone (it must import with only its
# declared requirements), then with the test dependencies for the
# framework suites served by the native host, and the native host's runs
# (workers, unix sockets, reactor backends). Runs from the source tree, whose
# src/ is not on the path, so the installed package is what is tested.
#
#   scripts/test_wheels.sh WHEEL...      (in the manylinux container, as built)
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
export PATH="/opt/python/cp312-cp312/bin:$PATH"  # uv, as scripts/build_wheels.sh installed it
failed=()
# Every library a wheel carries comes with its license.
python scripts/check_wheel_licenses.py "$@" || failed+=("licenses")
# Every wheel and every suite runs even after a failure, so that one run shows
# them all; the exit status says whether any failed.
for wheel in "$@"; do
  tag="$(basename "$wheel" | cut -d- -f3)"            # cp312 ...
  python="/opt/python/$tag-$tag/bin/python"
  venv="$(mktemp -d)/venv"
  uv venv --quiet --python "$python" "$venv"
  echo "== $(basename "$wheel")"
  # With only its declared requirements, as pip installs it: the package, the
  # command line and the extensions (whose optional libraries are absent).
  uv pip install --quiet --python "$venv/bin/python" "$wheel"
  "$venv/bin/python" -I -c "import oldman_server.ext; from oldman_server import OldmanServer; OldmanServer('wheel').extend()" \
    && "$venv/bin/oldman-server" --version \
    || failed+=("$tag: declared requirements")
  uv pip install --quiet --python "$venv/bin/python" "$wheel" --group test
  # Not the tree's package: the installed one.
  "$venv/bin/python" -I -c "import oldman_server.native._native as n; assert '/site-packages/' in n.__file__, n.__file__" \
    || failed+=("$tag: import")
  OLDMAN_SERVER_NATIVE_SERVER=true OLDMAN_SERVER_NATIVE_SHARDS=1 \
    "$venv/bin/python" -m pytest -q -p no:cacheprovider -rf tests/framework tests/ext \
    || failed+=("$tag: framework suites")
  "$venv/bin/python" -m pytest -q -p no:cacheprovider -rf tests/test_sanic_native_run.py tests/test_sanic_native_workers.py \
    || failed+=("$tag: native host runs")
done
if (( ${#failed[@]} )); then
  printf 'failed: %s\n' "${failed[@]}"
  exit 1
fi
