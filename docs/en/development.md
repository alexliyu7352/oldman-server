English | [简体中文](../zh/development.md)

# Development

## Development environment

- Host: Ubuntu 20.04.
- Python: CPython 3.14.7 (the regular GIL build; 3.12–3.14 are supported), managed by uv and installed in uv's shared Python directory; it needs a uv that knows this version.
- Compiler: the pinned GCC 14, in `/opt/tools/gcc-14` (`PROBE_TOOLCHAIN`).
- Native dependencies: Asio and liburing, in `build/deps` (`PROBE_DEPS`).

The first time, run `bash scripts/bootstrap.sh`: following `dependency-lock.json`, it downloads the compiler (skipped if already present) and the dependencies, and builds liburing, the extension and the load generator used for the benchmarks (`build/http_load`, also based on Asio). It requires the host's system packages to match the versions in the lock file's `host_requirements` one by one (a mismatch is refused; it never switches to another version). For other environments, use the container flow of the release build (see [Releases](#releases); `scripts/build_wheels.sh` builds all dependencies from source in a manylinux_2_28 container).

```sh
bash scripts/bootstrap.sh               # first time: the pinned toolchain and native dependencies, the extension, the load generator
uv sync --locked                        # Python dependencies (the official PyPI); this package is installed in editable mode
source scripts/env.sh                   # native library paths
bash scripts/build.sh                   # build the native extension oldman_server.native._native (about 10 seconds incremental)
# Other Python versions: their own environment and build directory, building only the extension
UV_PROJECT_ENVIRONMENT=build/venv312 uv sync --locked --python 3.12
PROBE_PYTHON=$PWD/build/venv312/bin/python PROBE_PROJECT_BUILD=$PWD/build/cmake312 bash scripts/build.sh _native
```

## Tests and measurement

`tests/framework` comes from Sanic 25.12.1's test suite (MIT), changed to this package's import name and behavior; it must pass in full both through the asyncio server and through the native host:

```sh
python scripts/run_framework_correctness.py --sanic --out build/correct-<dir>    # this package's native host tests, each case in its own process
OLDMAN_SERVER_NATIVE_SERVER=false .venv/bin/python -m pytest tests/framework   # framework tests: the asyncio server
OLDMAN_SERVER_NATIVE_SERVER=true OLDMAN_SERVER_NATIVE_SHARDS=1 .venv/bin/python -m pytest tests/framework   # framework tests: through the native host
OLDMAN_SERVER_NATIVE_SERVER=true OLDMAN_SERVER_NATIVE_SAME_THREAD=true .venv/bin/python -m pytest tests/framework   # the same, in same-thread mode
OLDMAN_SERVER_NATIVE_SAME_THREAD=true python scripts/run_framework_correctness.py --sanic --out build/correct-<dir>   # native host tests, in same-thread mode
python scripts/calibrate.py build/calib-<dir> candidate:small candidate:body      # server throughput and main-thread µs
python probes/sanic_comparison/run.py --python-only --out build/compare-<new-dir>     # stage acceptance run: paired comparison with stock Sanic
python probes/wrk_comparison/run.py --out build/wrk-<new-dir> --quiet                # 4 cores on one machine: each layout, the bare function (the native host's own ceiling) and stock Sanic, under wrk
```

For the full benchmarks and the commands to reproduce them, see [Benchmarks](benchmarks.md#reproducing).

## Releases

After a tag `v<version>` is pushed (matching `src/oldman_server/__version__.py`, such as `v0.1.0`), `.github/workflows/release.yml`:

1. Builds wheels for x86_64 and aarch64 in manylinux_2_28 containers (`scripts/build_wheels.sh`): it downloads the liburing and Asio sources per `dependency-lock.json` and verifies their SHA-256 (`scripts/fetch_sources.py`), builds liburing, then for each of CPython 3.12–3.14 builds the extension and the runtime's io_uring build (with liburing linked statically) and packages them (`hatch_build.py` puts both into the wheel and tags it with the interpreter and platform; with no extension in the tree it refuses to build a wheel, and only `OLDMAN_SERVER_PURE_WHEEL=1` builds a pure-Python wheel that can only use the asyncio server); auditwheel checks the linked libraries and tags the wheel manylinux_2_28. The container images (compiler included) are pinned by tag and digest (`wheels.yml`).
2. Checks that every library a wheel carries comes with its license (`scripts/check_wheel_licenses.py`), then installs each wheel into a fresh environment: it must import with only the declared dependencies installed; then, with the test dependencies added, the framework tests run through the asyncio server and through the native host, along with the native host's run tests (`scripts/test_wheels.sh`). auditwheel tags only manylinux_2_28 (`--only-plat`: the tests run only in the glibc 2.28 image, so no earlier glibc is tagged). Each wheel is also installed in the `python:3.12/3.13/3.14-slim` images and its native extension loaded: manylinux lets a wheel depend on a few system libraries (libstdc++, libgcc_s, libz, libatomic) without bundling them, and slim images may not have all of them.
3. Publishes through PyPI trusted publishing (environment `pypi`, which accepts deployments only from `v*` tags), then creates a GitHub release with the runner's own `gh`, attaching the wheels and SHA256SUMS. Every action in the workflows is pinned to a commit.

Every push and PR runs `.github/workflows/ci.yml`, which builds and tests the same wheels without publishing. The wheels carry the licenses of the third-party libraries they contain (`LICENSES/`, as the package's license files): pybind11 and Asio, compiled into the extension, and liburing, compiled into the io_uring build, must all have license files; if auditwheel copies a library into a wheel, it too must be in the list of the script above and have a license file, or the test fails.

## Repository layout

- `docs/`: documentation, English in `docs/en` and Simplified Chinese in `docs/zh`; the full benchmarks against stock Sanic, with the commands to reproduce them, are in its `benchmarks.md`.
- `src/oldman_server/`: the framework (a fork of Sanic 25.12.1).
- `src/oldman_server/native/host.py`: the adapter between the application and the native host, covering requests, responses, streams, files, protocol errors and WebSocket.
- `src/oldman_server/native/engine.py`: the Python side of the native runtime (taking events, the per-request Task, reply batches, backpressure, file operations).
- `native/`: the native host (`server.cc`) and runtime (`runtime.cc`, plus the io_uring build `liboldman_native_uring.so`), based on Asio; HTTP/1.1 parsing and writing (`http1.cc`), multipart parsing (`multipart.hh`), the Python bindings (`bridge.cc`, built as `oldman_server.native._native`).
- `src/oldman_server/routing/`: the router (from sanic-routing 23.12.0; its license is in `LICENSE.sanic-routing`), with its tests in `tests/routing`.
- `src/oldman_server/testing/`: the test client (from sanic-testing 24.6.0; its license is in `LICENSE.sanic-testing`).
- `upstream/`: the upstream versions this is based on (archive URL, commit) and each file's SHA-256. Comparing performance with stock Sanic first needs the archive from `upstream/sanic.json` unpacked into `build/sanic-upstream/` (giving `sanic-<commit>/`), with every file checked before the comparison.
- `tests/`: this package's tests, with `tests/framework` coming from Sanic's test suite; `scripts/`: build, test and measurement tools; `probes/`: the servers and drivers used for comparisons and diagnostics.
