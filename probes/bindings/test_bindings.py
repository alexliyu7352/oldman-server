"""Checks for the disposable binding experiment, not a new server interface."""
import importlib.util
import pytest


def make_engine(kind):
    assert importlib.util.find_spec("_binding_probe") is not None, "binding probe is not implemented"
    import _binding_probe
    return getattr(_binding_probe, kind)()


@pytest.mark.parametrize("kind", ["PybindFixture", "CapiFixture"])
def test_metadata_preserves_large_ids_types_and_bounds(kind):
    engine = make_engine(kind)
    rows = engine.drain_requests(64)
    assert len(rows) == 6
    assert rows[0] == (6553600000, 0, "GET", "fixed", 1024, None)
    assert rows[1] == (6553665537, 1, "GET", "fixed", 1024, None)
    assert engine.drain_requests(0) == []
    assert len(engine.drain_requests(2)) == 2
    with pytest.raises((OverflowError, TypeError)):
        engine.drain_requests(-1)


@pytest.mark.parametrize("kind", ["PybindFixture", "CapiFixture"])
def test_decision_conversion_owns_binary_data_and_rejects_partial_batch(kind):
    engine = make_engine(kind)
    decisions = [(6553600000, 0, 1, b"\x00\xffhello"), (6553665537, 1, 0, b"")]
    engine.resolve_requests(decisions)
    decisions.clear()
    assert engine.snapshot() == (1, 2, [(6553600000, 0, 1, b"\x00\xffhello"), (6553665537, 1, 0, b"")])
    for bad in ((1, 0, 1, "not bytes"), (1, 0, 1), (-1, 0, 1, b""), (1, 2**40, 1, b"")):
        with pytest.raises((TypeError, ValueError, OverflowError, RuntimeError)):
            engine.resolve_requests([(2, 0, 1, b"valid"), bad])
        assert engine.snapshot()[0:2] == (1, 2)
    engine.resolve_requests([])
    assert engine.snapshot() == (2, 2, [])
