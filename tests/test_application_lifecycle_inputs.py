"""Live process identities and the runtime gate of the comparison drivers."""
import os
import sys

import pytest

from probes.application_lifecycle import inputs


def test_identity_rejects_wrong_parent_and_reused_pid_evidence():
    identity = inputs.process_identity(os.getpid(), os.getppid(), sys.executable)
    inputs.check_identity(identity)
    with pytest.raises(ValueError, match="identity"):
        inputs.process_identity(os.getpid(), os.getppid() + 1, sys.executable)
    changed = identity | {"start_ticks": identity["start_ticks"] + 1}
    with pytest.raises(ValueError, match="identity"):
        inputs.check_identity(changed)


def test_runtime_gate_rejects_disabled_gil(monkeypatch):
    monkeypatch.setattr(sys, "_is_gil_enabled", lambda: False)
    with pytest.raises(ValueError, match="GIL"):
        inputs.runtime_state()
