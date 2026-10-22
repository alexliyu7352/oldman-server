"""Instrument only the real drain, using the unchanged lifecycle worker."""
import argparse
import importlib
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from probes.drain_allocation.observer import Observer

parser = argparse.ArgumentParser()
parser.add_argument("--snapshot", type=Path, required=True)
parser.add_argument("--observer-library", type=Path, required=True)
parser.add_argument("--observer-out", type=Path, required=True)
args, business = parser.parse_known_args()
sys.path[:0] = [str(args.snapshot.resolve() / part) for part in ("python", "build")]
import _seastar_probe as native  # the frozen snapshot's extension
for name in ("probe_engine", "seastar_http"):
    importlib.import_module(name)
from probes.framework_json import server
from probes.application_lifecycle import server as lifecycle

observer = Observer(args.observer_library, native.__file__)


class AllocationSnapshot(server.comparison.Snapshot):
    def begin(self):
        super().begin()
        observer.start()

    def end(self):
        observer.library.drain_trace_stop()
        result = super().end()
        observer.finish(args.observer_out)
        return result


server.comparison.Snapshot = AllocationSnapshot
sys.argv = [sys.argv[0], "--snapshot", str(args.snapshot), *business]
lifecycle.main()
