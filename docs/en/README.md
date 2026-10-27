English | [简体中文](../zh/README.md)

# oldman-server documentation

| Page | Contents |
|---|---|
| [Installation](installation.md) | Native wheels and the compatibility build, choosing the server, building from source |
| [Configuration](configuration.md) | How Sanic's settings behave on the native host, native host settings, the memory allocator |
| [Native host](native-host.md) | File responses, listening sockets, multiple workers, the fast path, fixed responses and the native micro-cache |
| [Deployment](deployment.md) | Behind nginx |
| [Differences from Sanic](differences.md) | Interface differences, removed features, behavior fixes, differences under the native host |
| [Benchmarks](benchmarks.md) | The full comparison with stock Sanic and other Python frameworks, and the commands to reproduce it |
| [Development](development.md) | Development environment, tests and measurement, releases, repository layout |

Routing, middleware, blueprints, signals and the rest of the framework work as in Sanic 25.12.1; see [Sanic's documentation](https://sanic.dev/). Where this package differs, see [Differences from Sanic](differences.md).
