# oldman-server

[English](https://github.com/alexliyu7352/oldman-server/blob/main/README.md) | 简体中文

`oldman-server`（import 名 `oldman_server`）是带 C++ 原生 HTTP/1.1 主机（基于 Asio）的 Python Web 框架，主要配合 [oldman](https://github.com/alexliyu7352/oldman) 使用，也可单独使用。框架层源自 Sanic 25.12.1 的 fork（MIT），接口大体沿用 Sanic。

在 Linux 上默认由原生主机服务：网络 IO、HTTP 解析与写出由原生 shard 承担，路由、中间件、异常处理、生命周期和业务代码仍在 Python 中执行，每个请求照旧在自己的 Task 里运行。单进程时 shard 是另外的线程，Python 端只有一个进程、一个普通 GIL 解释器和一个 uvloop 事件循环；多 worker 时默认每个 worker 一个 CPU，shard 就在它的 Python 线程上运行（同线程模式）。

## 性能

与原版 Sanic 25.12.1 在同一台机器、同样的服务端 CPU 上配对对照，两边都用 uvloop（AMD Ryzen 7 3800X，Linux 5.15，CPython 3.14，2026-10-27）：

| 场景 | 本项目 RPS | 原版 Sanic RPS | 倍数 |
|---|---:|---:|---:|
| 单进程（3 核），小 JSON | 159,028 | 76,469 | 2.07 |
| 单进程（3 核），回显 1 KiB 请求体 | 132,466 | 65,534 | 2.01 |
| 单进程（3 核），handler 等 1 ms 计时器 | 117,421 | 84,784 | 1.39 |
| 4 个 worker（4 核），小 JSON | 251,376 | 100,814 | 2.50 |
| 4 个 worker（4 核），回显 1 KiB 请求体 | 223,347 | 86,306 | 2.59 |
| 4 个 worker（4 核），handler 等 1 ms 计时器 | 186,170 | 104,483 | 1.76 |

与其他 Python 框架（web-frameworks-benchmark 的应用与路由，wrk，按服务端 2 个 CPU 满载时的每请求 CPU）：本项目 13.7 µs，在框架里排第一，wred 14.3、oxapy 15.8、原版 Sanic 29.4、FastAPI 69.3；不经框架的裸函数 9.05 µs，也排第一（weft-wsgi 10.05、fastpysgi 10.13）。

每请求 CPU、延迟、io_uring、静态文件、各阶段的变化与复现方法见[性能对照](https://github.com/alexliyu7352/oldman-server/blob/main/docs/zh/benchmarks.md)。

## 安装

```sh
pip install oldman-server               # 或 uv add oldman-server
```

Linux x86_64 与 aarch64（glibc 2.28 及以上）、CPython 3.12–3.14 有原生 wheel。macOS、Windows 等其他平台装的是兼容版：没有原生主机，由 asyncio 服务器服务，供本机开发和不追求性能的小服务使用。详见[安装](https://github.com/alexliyu7352/oldman-server/blob/main/docs/zh/installation.md)。

## 快速开始

```python
from oldman_server import OldmanServer
from oldman_server.response import json

app = OldmanServer("hello")


@app.get("/")
async def index(request):
    return json({"hello": "world"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)   # 加 fast=True 则每个 CPU 一个 worker
```

路由、中间件、蓝图、信号等用法与 Sanic 相同，见 [Sanic 的文档](https://sanic.dev/)。名称不同：应用类是 `OldmanServer`，配置环境变量前缀是 `OLDMAN_SERVER_`，日志名是 `oldman_server.*`；其他差异见[与 Sanic 的差异](https://github.com/alexliyu7352/oldman-server/blob/main/docs/zh/differences.md)。

本包只是服务器与请求路径上的核心框架。CORS 内置，在启动时配置、由服务器自己附加（见[配置](https://github.com/alexliyu7352/oldman-server/blob/main/docs/zh/configuration.md#cors)）；HEAD 与 OPTIONS 由路由层为每个路由回答（见[配置](https://github.com/alexliyu7352/oldman-server/blob/main/docs/zh/configuration.md#自动-head-与-options)）；模板、OpenAPI、依赖注入、worker 存活监视这类配套（Sanic 由 sanic-ext 提供）不在包里，由上层框架（如 oldman）或应用自己提供。

## 文档

| 页面 | 内容 |
|---|---|
| [安装](https://github.com/alexliyu7352/oldman-server/blob/main/docs/zh/installation.md) | 原生 wheel 与兼容版、选择服务器、从源码构建 |
| [配置](https://github.com/alexliyu7352/oldman-server/blob/main/docs/zh/configuration.md) | Sanic 的设置在原生主机上的行为、原生主机的设置、内存分配器 |
| [原生主机](https://github.com/alexliyu7352/oldman-server/blob/main/docs/zh/native-host.md) | 文件响应、监听 socket、多 worker、快速路径、固定响应与原生微缓存 |
| [部署](https://github.com/alexliyu7352/oldman-server/blob/main/docs/zh/deployment.md) | 放在 nginx 之后 |
| [与 Sanic 的差异](https://github.com/alexliyu7352/oldman-server/blob/main/docs/zh/differences.md) | 接口的差异、已移除的功能、行为修正、经原生主机时的差异 |
| [性能对照](https://github.com/alexliyu7352/oldman-server/blob/main/docs/zh/benchmarks.md) | 与原版 Sanic 及其他 Python 框架的完整对照与复现命令 |
| [开发](https://github.com/alexliyu7352/oldman-server/blob/main/docs/zh/development.md) | 开发环境、测试与测量、发布、目录 |

## 许可证

MIT（`LICENSE`）。框架层源自 Sanic 25.12.1，`oldman_server.testing` 源自 sanic-testing 24.6.0，均为 MIT（各自的许可证在包内）；发布的 wheel 所含第三方代码（Asio、pybind11、liburing）的许可证见 `LICENSES/`，包的许可证表达式因此是 `MIT AND BSD-3-Clause AND BSL-1.0`。
