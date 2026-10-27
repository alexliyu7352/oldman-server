[English](../en/README.md) | 简体中文

# oldman-server 文档

| 页面 | 内容 |
|---|---|
| [安装](installation.md) | 原生 wheel 与兼容版、选择服务器、从源码构建 |
| [配置](configuration.md) | Sanic 的设置在原生主机上的行为、原生主机的设置、内存分配器 |
| [原生主机](native-host.md) | 文件响应、监听 socket、多 worker、快速路径、固定响应与原生微缓存 |
| [部署](deployment.md) | 放在 nginx 之后 |
| [与 Sanic 的差异](differences.md) | 接口的差异、已移除的功能、行为修正、经原生主机时的差异 |
| [性能对照](benchmarks.md) | 与原版 Sanic 及其他 Python 框架的完整对照与复现命令 |
| [开发](development.md) | 开发环境、测试与测量、发布、目录 |

路由、中间件、蓝图、信号等框架用法与 Sanic 25.12.1 相同，见 [Sanic 的文档](https://sanic.dev/)；本包与它的差异见[与 Sanic 的差异](differences.md)。
