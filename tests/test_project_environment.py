"""Project activation must never silently select the old dependency venv."""
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class ProjectEnvironmentTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        base = Path(self.directory.name)
        self.project = base / "project"
        (self.project / "scripts").mkdir(parents=True)
        shutil.copy2(ROOT / "scripts/env.sh", self.project / "scripts/env.sh")
        self.pin = self.project / ".python-version"
        self.pin.write_text("{}.{}.{}\n".format(*sys.version_info[:3]))
        (base / "seastar_deps/venv/bin").mkdir(parents=True)
        legacy = base / "seastar_deps/venv/bin/python"
        legacy.write_text("#!/bin/sh\nprintf 'legacy interpreter selected\\n'\n")
        legacy.chmod(0o755)

    def activate(self, command="true"):
        return subprocess.run(
            ["bash", "-c", 'source "$1" && ' + command,
             "project-env", str(self.project / "scripts/env.sh")],
            env={**os.environ, "PYTHONPATH": ""},
            capture_output=True, text=True, timeout=10)

    def install_interpreter(self):
        binary = self.project / ".venv/bin/python"
        binary.parent.mkdir(parents=True)
        binary.symlink_to(sys.executable)
        return binary

    def test_activation_selects_project_interpreter_over_legacy_venv(self):
        binary = self.install_interpreter()
        result = self.activate("command -v python")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), str(binary))

    def test_missing_project_environment_is_rejected(self):
        result = self.activate()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("uv sync", result.stderr)

    def test_wrong_interpreter_version_is_rejected(self):
        self.install_interpreter()
        self.pin.write_text("3.13.9\n")
        result = self.activate()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("3.13.9", result.stderr)


if __name__ == "__main__":
    unittest.main()
