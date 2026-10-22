"""Reuse the JSON business while selecting and reporting the actual snapshot."""
import argparse
import importlib
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from probes.application_lifecycle.inputs import fingerprint, runtime_state


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--profile", type=Path)
    args, business_args = parser.parse_known_args()
    selected = args.snapshot.resolve()
    sys.path[:0] = [str(selected / "python"), str(selected / "build")]
    modules = {name: importlib.import_module(name)
               for name in ("_seastar_probe", "probe_engine", "seastar_http")}
    # Existing server.py prepends its own root. Preloading keeps the selected
    # framework modules in sys.modules while the harness/business stays common.
    from probes.framework_json import server
    loaded = {name: fingerprint(module.__file__) for name, module in modules.items()}
    libraries = {line.split(maxsplit=5)[5] for line in Path("/proc/self/maps").read_text().splitlines()
                 if "libseastar.so" in line}
    library_hashes = {path: fingerprint(path)["sha256"] for path in sorted(libraries)}

    class Evidence:
        def info(self):
            return super().info() | {"procfs_pid": int(os.readlink("/proc/self")),
                                    "loaded_modules": loaded, "loaded_seastar": library_hashes,
                                    "runtime_state": runtime_state(),
                                    "loop_environment": {"UV_USE_IO_URING": os.environ.get("UV_USE_IO_URING")}}

    class Candidate(Evidence, server.CandidateService):
        pass

    class Reference(Evidence, server.PythonService):
        pass

    server.CandidateService, server.PythonService = Candidate, Reference
    if args.profile is not None:
        from probes.io_business.diagnostic_profile import ProfilingSnapshot
        os.environ["PROBE_PROFILE_DIR"] = str(args.profile.resolve())
        server.comparison.Snapshot = ProfilingSnapshot
    server.main(business_args)


if __name__ == "__main__":
    main()
