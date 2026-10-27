[English](../en/native-host.md) | 简体中文

# 原生主机

原生主机在当前进程内服务明文 HTTP/1.1，监听 TCP 或 unix socket，包括 keep-alive、pipelining、chunked 请求体、流式请求与流式响应、背压、100-continue 和 WebSocket。网络 IO、HTTP 解析与写出由原生 shard 承担，路由、中间件、异常处理、生命周期和业务代码仍在 Python 中执行。

`app.run()`、`app.create_server()`、测试客户端 `app.test_client`（`oldman_server.testing`）都会走原生主机，默认运行的 `main_process_*` 监听器照常触发。要用 asyncio 服务器，见[安装](installation.md#选择服务器)。

## 文件响应

`file()`、`file_stream()` 与 `app.static()` 的 stat 和读取由原生主机的文件线程完成（在 shard 的 CPU 上，读盘时不阻塞 shard），不再经 aiofiles 线程池。

- `file_stream()` 与 128 KiB 以上的 `file()` 的文件内容由文件线程读出、shard 直接写入连接，不经 Python（`file()` 因此带来的差异见[与 Sanic 的差异](differences.md#经原生主机时)）。`file_stream()` 的分块与原版相同（带 Range 时的修正见[行为修正](differences.md#行为修正)）。
- 更小的文件由 `file()` 读进响应，这样更省。
- 响应头、304/206、错误类型与原版一致。

## multipart 表单

`request.form` / `request.files` 的解析由 C++ 完成（`native/multipart.hh`），结果与 Sanic 的 `parse_multipart_form` 相同；遇到需要 Python 编解码或 Unicode 处理的表单（非 ASCII 的段头、非 UTF-8 字段、`filename*`、没有 name 的段、格式错误）则照旧由 Sanic 的解析器处理。

## 监听 socket

监听 socket 由 Sanic 照原样绑定（unix socket 用临时名、权限默认 0o666（`UNIX_SOCKET_MODE`）、改名替换，退出时删除），原生主机在这个 socket 上接受连接（shard 0 接受后分给各 shard）。它在服务期间保持打开、随服务器关闭，所以传入的 `sock`（TCP 或 AF_UNIX）、`app.serve_location` 与原版一致。

多 worker 时与原版的 TCP 一样，由主进程绑定并监听一次，各 worker 都在这一个 socket 上接受连接（unix socket 也是，退出时由主进程删除）。主进程从启动到退出一直在监听：

- 还没有 worker 在接受时（启动时、所有 worker 一起重启时），新连接在队列里等，而不是被拒绝；别的程序这时也绑定不了这个地址。
- 一个 worker 停止（重启、缩减）不会关闭其他 worker 仍在用的这个 socket，已在队列里的连接由其他 worker 接走。
- 原版的 unix socket 是每个 worker 各自绑定再改名覆盖路径，结果只有最后改名的 worker 收到连接。

各 worker 经主进程分配的共享内存公布自己的连接数，轮流接受：连接数不是最少的 worker 暂不 accept（每 50 µs 起再看一次，连接等了 1 ms 仍没有人接时照接；退出的 worker 立即让出，崩溃或卡住的 200 ms 后不再等它；重启的 worker 接替原来的位置），否则 shard 正忙（轮询而不睡眠）的那个 worker 几乎总是先接到新连接。用户传入的共用 socket 同样如此。放在 nginx 之后的部署见[部署](deployment.md)。

## 只服务明文 HTTP/1.1

TLS 由前端代理（如 nginx）终止，本包不提供 TLS 与 HTTP/3（见[与 Sanic 的差异](differences.md#已移除)）。

## 多 worker

会启动 worker 进程的运行（`workers > 1`、`fast`、`auto_reload`、`dev`）照原版由 Sanic 的 worker manager 管理（重启、扩缩、`app.m` 都相同），每个 worker 进程各自运行原生主机：

- 主进程把 CPU 分成每个 worker 一组，worker 的 Python 线程固定在组内第一个 CPU，shard 用其余的（按 worker 编号取组；编号见 [`NATIVE_WORKER_LISTEN`](configuration.md#原生主机的设置)）。
- CPU 不够每个 worker 再配一个 shard CPU 时（`fast=True` 每个 CPU 一个 worker，或 `workers` 多于 CPU 数的一半），每个 worker 只占一个 CPU，shard 在它的 Python 线程上运行（同线程模式，`NATIVE_SAME_THREAD`）：同样的核上跑 Python 的进程多了一倍，也省掉线程之间的交接。4 个核上，同一个空文本路由约 21.5 万对 2 个 worker × (1 + 1) 的 16.7 万 RPS；handler 自己花 25 µs CPU 时 9.0 万对 6.2 万（原版 Sanic 4 worker 12.9 万、7.0 万）。
- worker 数多于 CPU 组时，多出的 worker 与前面的共用 CPU 组（记一条警告）。

用 `fork` 启动方式、且父进程已经运行过原生主机时（一个进程只启动一次原生运行时，fork 出的子进程有它的状态却没有它的线程），子进程改用 asyncio 服务器并记一条警告；默认的 `spawn` 不受影响。

每个进程只有一个原生服务器：同一进程同一事件循环上的第二个服务器（如第二个应用的服务器）使用 asyncio 服务器（记一条警告），两者都正常服务。

## 快速路径

普通应用的请求由原生主机一次建好请求对象、由原生代码收尾与记账，每请求比一般路径（与 Sanic 的 `handle_request` 相同）少数微秒。以下情况退回一般路径，行为相同，启动时在 `oldman_server.server` 记一条 INFO 说明原因：

- debug 模式（TOUCHUP 关闭）；
- 换了 `request_class` 或 router；
- 覆写了 `handle_request` 等方法；
- 注册了带参数的 `http.*` 信号。

## 协议错误

协议错误（如非法请求行、请求头过大、坏的 chunked 编码、请求体超限）与原版报出相同的状态码和消息，并交给应用的错误处理器。

## 不经 Python 的回答

两种方式，都要明确设置。

### 固定响应

`app.add_fixed_response("/health", "ok")`（可选 `status`、`content_type`、`headers`）。这个路径的 GET 与 HEAD 请求（不管查询字符串）在一切之前回答：不改写路径、不路由、不跑中间件、不触发信号、没有 Request 对象、不记访问日志；带请求体的请求和其他方法照常路由，同一路径的路由收不到这两个方法。原生主机直接写出，不进 Python；asyncio 服务器按同样的规则回答（只在读到请求行之前触发的连接级信号除外）。在服务器启动之前设置。

### 原生微缓存

`@native_cache(seconds)`（`from oldman_server.native import native_cache`，加在 handler 上、路由装饰器之下）。这个 handler 对 GET 的完整 200 响应，没有 Set-Cookie、Vary，Cache-Control 也允许共享缓存保存（没有 private、no-store、no-cache）时，原生主机保存 `seconds` 秒，期间同一请求目标（路径加查询字符串）的 GET 直接由它回答：不进 Python，所以中间件（包括认证）、信号、handler 都不运行，请求头也不看；带 Cookie 或 Authorization 的请求既不从缓存回答，响应也不保存。只适合公开内容。每个 shard（同线程模式即每个 worker 进程）各有一份，最多 1024 条、每条 1 MiB。asyncio 服务器忽略它。
