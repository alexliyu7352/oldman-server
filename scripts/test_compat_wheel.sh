#!/usr/bin/env bash
# Tests the pure Python wheel where it is meant for (macOS, Windows): installed
# into a fresh virtual environment of the current python, first alone (it must
# import with only its declared requirements, and an app with NATIVE_SERVER set
# must serve, by the asyncio server), then with the test dependencies for the
# framework suites. Runs from the source tree, whose src/ is not on the path.
#
#   scripts/test_compat_wheel.sh WHEEL
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
wheel="$1"
venv="$(mktemp -d)/venv"
# The interpreter's own path (on Windows, Git Bash's has no .exe).
uv venv --quiet --python "$(python -c 'import sys; print(sys.executable)')" "$venv"
if [[ -x "$venv/Scripts/python.exe" ]]; then
  bin="$venv/Scripts" py="$venv/Scripts/python.exe"
else
  bin="$venv/bin" py="$venv/bin/python"
fi
failed=()
uv pip install --quiet --python "$py" "$wheel"
"$py" -I -c "import oldman_server.ext; from oldman_server import OldmanServer; OldmanServer('wheel').extend()" \
  && "$bin/oldman-server" --version \
  || failed+=("declared requirements")
"$py" -I scripts/compat_smoke.py || failed+=("serving")
uv pip install --quiet --python "$py" "$wheel" --group test
"$py" -m pytest -q -p no:cacheprovider -rf --continue-on-collection-errors tests/framework tests/ext \
  || failed+=("framework suites")
if (( ${#failed[@]} )); then
  printf 'failed: %s\n' "${failed[@]}"
  exit 1
fi
