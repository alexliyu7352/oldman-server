[English](../en/installation.md) | 简体中文

# 安装

```sh
pip install oldman-server               # 或 uv add oldman-server
```

## 原生 wheel

PyPI 上是编译好的 wheel：Linux x86_64 与 aarch64（manylinux_2_28，即 glibc 2.28 及以上，如 Ubuntu 20.04、Debian 10、RHEL 8 及以后），CPython 3.12–3.14。wheel 只依赖 manylinux 允许的系统库，不需要另装别的库。

## 兼容版

其他平台（macOS、Windows，以及没有对应 wheel 的 Linux，如 Alpine）装的是兼容版：同一个包的纯 Python wheel，没有原生主机，由 asyncio 服务器服务（macOS 上用 uvloop，Windows 上用默认的 selector 事件循环），供本机开发和不追求性能的小服务（本地的 MCP 服务器、面板）使用，吞吐与原版 Sanic 相当。pip 在有原生 wheel 的系统上总是优先选原生的。unix socket 在 Windows 上不可用。

## 选择服务器

由设置 `NATIVE_SERVER` 决定（也可用环境变量 `OLDMAN_SERVER_NATIVE_SERVER`）：

| `NATIVE_SERVER` | Linux | macOS、Windows |
|---|---|---|
| 不设（默认） | 有原生扩展就由原生主机服务，缺少时记一条警告、由 asyncio 服务器服务 | asyncio 服务器 |
| `True` | 原生主机；缺少原生扩展则启动报错，不会悄悄退回较慢的服务器 | 记一条警告后照常由 asyncio 服务器服务，开发机与 Linux 生产环境因此可以用同一份配置 |
| `False` | asyncio 服务器 | asyncio 服务器 |

```python
app.config.NATIVE_SERVER = False        # 或环境变量 OLDMAN_SERVER_NATIVE_SERVER=false
```

## 从源码构建

不提供源码包：源码包不构建原生扩展。从源码构建需要 C++23 编译器、Asio 与 liburing，见[开发](development.md)。在源码树里构建的源码包只含 `pyproject.toml` 列出的公开源码（`[tool.hatch.build.targets.sdist]`）。
