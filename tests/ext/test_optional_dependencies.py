"""oldman_server.ext works with only the package's declared requirements:
pydantic, msgspec, attrs and Jinja2 are optional (validation models of those
kinds, and templating, need them)."""
import subprocess
import sys

import pytest

OPTIONAL = ("pydantic", "msgspec", "attrs", "attr", "jinja2")


def run_without_optional(code):
    # A module set to None in sys.modules cannot be imported, as if not installed.
    hide = "".join(f"sys.modules[{name!r}] = None\n" for name in OPTIONAL)
    return subprocess.run([sys.executable, "-c", f"import sys\n{hide}{code}"],
                          capture_output=True, text=True, timeout=60)


@pytest.mark.parametrize("code", [
    "import oldman_server.ext",
    "from oldman_server.ext import validate",
    "from oldman_server.ext import *",
    "from oldman_server import OldmanServer\nOldmanServer('optional').extend()",
])
def test_ext_imports_without_optional_libraries(code):
    result = run_without_optional(code)
    assert result.returncode == 0, result.stderr


def test_templating_extension_still_needs_jinja2():
    result = run_without_optional("from oldman_server.ext import TemplatingExtension")
    assert result.returncode != 0
    assert "jinja2" in result.stderr
