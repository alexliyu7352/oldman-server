English | [简体中文](../zh/installation.md)

# Installation

```sh
pip install oldman-server               # or uv add oldman-server
```

## Native wheels

PyPI has prebuilt wheels: Linux x86_64 and aarch64 (manylinux_2_28, i.e. glibc 2.28 or later, such as Ubuntu 20.04, Debian 10, RHEL 8 and later), CPython 3.12–3.14. The wheels depend only on system libraries that manylinux allows; nothing else needs to be installed.

## Compatibility build

Other platforms (macOS, Windows, and Linux without a matching wheel, such as Alpine) install the compatibility build: a pure-Python wheel of the same package, without the native host, served by the asyncio server (with uvloop on macOS, with the default selector event loop on Windows). It is meant for local development and small services that do not need the performance (a local MCP server, a dashboard); its throughput is on par with stock Sanic. On systems with a native wheel, pip always prefers the native one. Unix sockets are not available on Windows.

## Choosing the server

The setting `NATIVE_SERVER` decides (the environment variable `OLDMAN_SERVER_NATIVE_SERVER` works too):

| `NATIVE_SERVER` | Linux | macOS, Windows |
|---|---|---|
| Not set (default) | The native host when the native extension is present; without it, a warning is logged and the asyncio server serves | The asyncio server |
| `True` | The native host; without the native extension, startup fails instead of silently falling back to the slower server | A warning is logged and the asyncio server serves as usual, so a development machine and Linux production can share one configuration |
| `False` | The asyncio server | The asyncio server |

```python
app.config.NATIVE_SERVER = False        # or the environment variable OLDMAN_SERVER_NATIVE_SERVER=false
```

## Building from source

No source distribution is published: a source distribution does not build the native extension. Building from source needs a C++23 compiler, Asio and liburing; see [Development](development.md). A source distribution built in the source tree contains only the public sources that `pyproject.toml` lists (`[tool.hatch.build.targets.sdist]`).
