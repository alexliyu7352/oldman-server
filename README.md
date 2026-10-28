# oldman-server

English | [简体中文](https://github.com/alexliyu7352/oldman-server/blob/main/README.zh-CN.md)

`oldman-server` (import name `oldman_server`) is a Python web framework with a native C++ HTTP/1.1 host built on Asio. It is made mainly for [oldman](https://github.com/alexliyu7352/oldman) and also works on its own. The framework layer is a fork of Sanic 25.12.1 (MIT), and its interface largely follows Sanic.

On Linux the native host serves by default: native shards do the network I/O, HTTP parsing and writing, while routing, middleware, exception handling, the lifecycle and your code still run in Python, each request in its own Task as before. In a single process the shards are separate threads, and the Python side is one process with one regular GIL interpreter and one uvloop event loop. With several workers, each worker gets one CPU by default and runs its shard on its Python thread (same-thread mode).

## Performance

Paired against stock Sanic 25.12.1 on the same machine and the same server CPUs, both on uvloop (AMD Ryzen 7 3800X, Linux 5.15, CPython 3.14, 2026-10-27):

| Scenario | oldman-server RPS | Stock Sanic RPS | Ratio |
|---|---:|---:|---:|
| Single process (3 cores), small JSON | 159,028 | 76,469 | 2.07 |
| Single process (3 cores), echo a 1 KiB body | 132,466 | 65,534 | 2.01 |
| Single process (3 cores), handler waits a 1 ms timer | 117,421 | 84,784 | 1.39 |
| 4 workers (4 cores), small JSON | 251,376 | 100,814 | 2.50 |
| 4 workers (4 cores), echo a 1 KiB body | 223,347 | 86,306 | 2.59 |
| 4 workers (4 cores), handler waits a 1 ms timer | 186,170 | 104,483 | 1.76 |

Against other Python frameworks (the apps and routes of web-frameworks-benchmark, driven by wrk, ranked by CPU per request with the server's 2 CPUs saturated): oldman-server takes 13.7 µs, first among the frameworks, ahead of wred 14.3, oxapy 15.8, stock Sanic 29.4 and FastAPI 69.3. A bare function served without the framework takes 9.05 µs, also first (weft-wsgi 10.05, fastpysgi 10.13).

CPU per request, latency, io_uring, static files, the changes by stage and how to reproduce them are in [Benchmarks](https://github.com/alexliyu7352/oldman-server/blob/main/docs/en/benchmarks.md).

## Installation

```sh
pip install oldman-server               # or: uv add oldman-server
```

Native wheels cover Linux x86_64 and aarch64 (glibc 2.28 or later) on CPython 3.12–3.14. Other platforms, such as macOS and Windows, get the compatibility build: no native host, served by the asyncio server, meant for local development and small services that do not need the speed. See [Installation](https://github.com/alexliyu7352/oldman-server/blob/main/docs/en/installation.md).

## Quick start

```python
from oldman_server import OldmanServer
from oldman_server.response import json

app = OldmanServer("hello")


@app.get("/")
async def index(request):
    return json({"hello": "world"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)   # fast=True runs one worker per CPU
```

Routing, middleware, blueprints and signals work as in Sanic; see the [Sanic documentation](https://sanic.dev/). The names differ: the application class is `OldmanServer`, the environment prefix for settings is `OLDMAN_SERVER_`, and the loggers are `oldman_server.*`. The other differences are listed in [Differences from Sanic](https://github.com/alexliyu7352/oldman-server/blob/main/docs/en/differences.md).

This package is only the server and the core framework on the request path. CORS is built in and applied by the server itself, configured at startup ([Configuration](https://github.com/alexliyu7352/oldman-server/blob/main/docs/en/configuration.md#cors)). Templates, automatic HEAD / OPTIONS, OpenAPI, dependency injection and worker health monitoring (which Sanic gets from sanic-ext) are not included; a framework on top, such as oldman, or the application provides them.

## Documentation

| Page | Contents |
|---|---|
| [Installation](https://github.com/alexliyu7352/oldman-server/blob/main/docs/en/installation.md) | Native wheels and the compatibility build, choosing the server, building from source |
| [Configuration](https://github.com/alexliyu7352/oldman-server/blob/main/docs/en/configuration.md) | How Sanic's settings apply under the native host, native host settings, memory allocators |
| [Native host](https://github.com/alexliyu7352/oldman-server/blob/main/docs/en/native-host.md) | File responses, listening sockets, multiple workers, the fast path, fixed responses and the native micro-cache |
| [Deployment](https://github.com/alexliyu7352/oldman-server/blob/main/docs/en/deployment.md) | Behind nginx |
| [Differences from Sanic](https://github.com/alexliyu7352/oldman-server/blob/main/docs/en/differences.md) | Interface differences, removed features, behavior fixes, differences under the native host |
| [Benchmarks](https://github.com/alexliyu7352/oldman-server/blob/main/docs/en/benchmarks.md) | Full comparison with stock Sanic and other Python frameworks, with the commands to reproduce it |
| [Development](https://github.com/alexliyu7352/oldman-server/blob/main/docs/en/development.md) | Development environment, tests and measurement, releases, repository layout |

## License

MIT (`LICENSE`). The framework layer derives from Sanic 25.12.1 and `oldman_server.testing` from sanic-testing 24.6.0, both MIT (their licenses ship in the package). The licenses of the third-party code in the published wheels (Asio, pybind11, liburing) are in `LICENSES/`, so the package's license expression is `MIT AND BSD-3-Clause AND BSL-1.0`.
