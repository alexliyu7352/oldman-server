# oldman-server

`oldman-server`（import 名 `oldman_server`）是带 C++ / Seastar 原生 HTTP/1.1 主机的 Python Web 框架，主要配合 [oldman](https://github.com/alexliyu7352/oldman) 使用，也可单独使用。框架层源自 Sanic 25.12.1 的 fork（MIT，见 `src/oldman_server/LICENSE.sanic`），接口大体沿用 Sanic，差异见下文。开启 `NATIVE_SERVER` 后，网络 IO、HTTP 解析与写出由原生 shard 线程承担，路由、中间件、异常处理、生命周期和业务代码仍在 Python 中执行。Python 端只有一个进程、一个普通 GIL 解释器和一个 uvloop 事件循环。

## 现状

- **吞吐**：单进程同 3 核（1 个 Python 线程 + 2 个 Seastar shard）对原版 Sanic 3 个 worker 进程，两边都用 uvloop 和各自默认的 `json()`（原版为 ujson，本包为 orjson）：小 JSON 约 10.6 万对 7.4 万 RPS（1.42 倍），回显请求体约 8.3 万对 6.4 万（1.32 倍），handler 等 1 ms 计时器的 I/O 等待型业务约 8.8 万对 8.0 万（1.10 倍），每请求总 CPU 约为原版的 0.6–0.74 倍，每组配对都领先。
- **多 worker**：同 4 个物理核，2 个 worker 进程（各 1 个 Python 线程 + 1 个 shard）对原版 Sanic 4 个 worker：小 JSON 16.9 万对 9.5 万 RPS（1.80 倍），回显请求体 14.3 万对 8.0 万（1.76 倍），sleep 14.0 万对 10.1 万（1.38 倍），每请求总 CPU 约为原版的 0.51–0.66 倍。（AMD Ryzen 7 3800X，Ubuntu 20.04，2026-10-22；客户端为 Seastar 的 HTTP 压测工具，占另外 3 个核。）
- **测试**：Sanic 与 sanic-ext 的测试集已并入本项目（`tests/framework`、`tests/ext`），按本包的行为维护，经 asyncio 服务器和经原生主机（`OLDMAN_SERVER_NATIVE_SERVER=true`）两种方式都全部通过。与 Sanic 的差异见下文。

## 安装

```sh
pip install oldman-server               # 或 uv add oldman-server
```

PyPI 上是编译好的 wheel：Linux x86_64 与 aarch64（manylinux_2_28，即 glibc 2.28 及以上，如 Ubuntu 20.04、Debian 10、RHEL 8 及以后），CPython 3.12–3.14。Seastar 及其依赖的库已在 wheel 里，不需要另装。不提供源码包：从源码构建需要 Seastar 的整套依赖（见下文开发环境与「发布」）。在源码树里构建的源码包只含 `pyproject.toml` 列出的公开源码（`[tool.hatch.build.targets.sdist]`）。

## 快速开始

开发环境：Ubuntu 20.04 宿主；CPython 3.14.7（普通 GIL 构建；支持 3.12–3.14），由 uv 管理；Seastar 等原生依赖位于 `../seastar_deps`。第一次先运行 `bash scripts/bootstrap.sh`：它按 `dependency-lock.json` 下载编译器与依赖到 `../seastar_deps` 并构建 Seastar，要求宿主的系统包与锁文件 `host_requirements` 的版本逐个相同（不同即拒绝，不会换用别的版本）。其他环境请用发布构建的容器流程（见「发布」，`scripts/build_wheels.sh` 在 manylinux_2_28 容器里从源码构建全部依赖）。

```sh
bash scripts/bootstrap.sh               # 第一次：锁定的工具链与原生依赖、Seastar、扩展
uv sync --locked                        # Python 依赖（官方 PyPI），本包以可编辑方式安装
source scripts/env.sh                   # 原生库路径
bash scripts/build.sh                   # 构建原生扩展 oldman_server.native._native（增量约 10 秒）
# 其他 Python 版本：各自的环境与构建目录，只构建扩展
UV_PROJECT_ENVIRONMENT=build/venv312 uv sync --locked --python 3.12
PROBE_PYTHON=$PWD/build/venv312/bin/python PROBE_PROJECT_BUILD=$PWD/build/cmake312 bash scripts/build.sh _native
```

开启原生主机（二选一）：

```python
from oldman_server import OldmanServer

app = OldmanServer("app")
app.config.NATIVE_SERVER = True         # 或环境变量 OLDMAN_SERVER_NATIVE_SERVER=true
app.run(host="0.0.0.0", port=8000)
```

`app.run()`、`oldman-server` 命令行（即 Sanic 的 CLI）、`app.create_server()`、测试客户端 `app.test_client`（`oldman_server.testing`）都会走原生主机，默认运行的 `main_process_*` 监听器照常触发。

Sanic Extensions（sanic-ext 25.12.0，MIT）随包提供为 `oldman_server.ext`：依赖注入、OpenAPI、HTTP（CORS、自动 HEAD/OPTIONS 等）、健康检查、日志、模板与请求校验。可选的库按需另装：模板需要 Jinja2，用 pydantic、msgspec 或 attrs 的模型做校验时需要对应的库；不装它们，其余功能照常可用。与没有安装 sanic-ext 的 Sanic 一样，应用默认不被扩展；`app.extend()`、`app.ext` 或 `Extend(app, ...)` 时才启用。

名称与 Sanic 不同：应用类是 `OldmanServer`，配置环境变量前缀是 `OLDMAN_SERVER_`，日志名是 `oldman_server.*`（对照见「与 Sanic 的差异」）。

## 配置

Sanic 的设置照常生效：

| 设置 | 原生主机的行为 |
|---|---|
| `REQUEST_MAX_SIZE`、`REQUEST_MAX_HEADER_SIZE`、`REQUEST_BUFFER_SIZE` | 请求体与请求头上限，与 Sanic 的计算相同 |
| `REQUEST_TIMEOUT`、`KEEP_ALIVE`、`KEEP_ALIVE_TIMEOUT` | 由原生层执行 |
| `RESPONSE_TIMEOUT` | 到期时取消 handler，由错误处理器回 503 “Response Timeout” |
| `ACCESS_LOG`、`WEBSOCKET_*`、错误处理器、`FALLBACK_ERROR_FORMAT` | 与原版相同 |

原生专属设置（都在 `DEFAULT_CONFIG` 里有默认值，`app.config.NATIVE_*` 总能读到；也可用 `OLDMAN_SERVER_NATIVE_*` 环境变量设置）：

| 设置 | 含义 |
|---|---|
| `NATIVE_SHARDS` / `NATIVE_SHARD_CPUS` | shard 数量（默认最多 2，取进程 CPU 集合中最后几个）或指定 CPU（只用于单进程） |
| `NATIVE_WORKER_CPUS` | 多 worker 时每个 worker 的 CPU 组，如 `"0,1,2;3,4,5"`（每组第一个给 Python，其余给 shard）；默认由主进程按 1 + `NATIVE_SHARDS` 个 CPU 一组依次切分 |
| `NATIVE_MAX_REQUESTS` | 同时在处理的请求上限（默认 4096，超出回 503） |
| `NATIVE_MAX_RESPONSE_SIZE` | 单个完整响应或流片段的上限（默认 128 MiB） |
| `NATIVE_MAX_MEMORY` | 在途请求体可占用的内存 |
| `NATIVE_REACTOR_BACKEND` | Seastar 的 reactor 后端：`epoll`（默认）、`linux-aio`、`io_uring`；空字符串由 Seastar 选第一个可用的（io_uring、linux-aio、epoll 依次）。shard 吃满时 linux-aio 比 epoll 略快（多 worker 对照中 small/body 约 +1–2%，1024 连接的 sleep 约 +8%），但每个 shard 能挂起的连接数受 aio 配额限制（约 2 万，系统 `fs.aio-max-nr` 紧张时更少），超出时进程会中止；io_uring 在 Linux 5.15 上比 epoll 慢约 7% |
| `NATIVE_WORKER_LISTEN` | 每个服务进程另外监听的地址，`{n}` 换成 worker 编号（单进程为 0；n 个 worker 的编号是 0 到 n−1，缩减后再扩容的新 worker 取空出的编号，所以各地址与 CPU 组不变）：unix socket 路径（`"/run/app/{n}.sock"`，也可加 `unix:` 前缀）或 `host:port`（`"127.0.0.1:81{n:02d}"` 即 8100、8101……）。供前端代理按请求均衡各 worker（见「放在 nginx 之后」）；运行本身的地址照常服务。默认不设 |

## 原生主机与 asyncio 服务器

原生主机在当前进程内服务明文 HTTP/1.1，监听 TCP 或 unix socket，包括 keep-alive、pipelining、chunked 请求体、流式请求与流式响应、背压、100-continue 和 WebSocket。

- 文件响应：`file()`、`file_stream()` 与 `app.static()` 的 stat 和读取由 shard 上的 Seastar 文件 API 完成（走页缓存；不在缓存中的读交给 Seastar 的 syscall 线程，不阻塞 reactor），不再经 aiofiles 线程池；`file_stream()` 的文件内容由 shard 直接读出并写入连接，分块与原版相同。响应头、304/206、错误类型与原版一致。
- multipart 表单：`request.form` / `request.files` 的解析由 C++ 完成（`native/multipart.hh`），结果与 Sanic 的 `parse_multipart_form` 相同；遇到需要 Python 编解码或 Unicode 处理的表单（非 ASCII 的段头、非 UTF-8 字段、`filename*`、没有 name 的段、格式错误）则照旧由 Sanic 的解析器处理。
- 监听 socket：由 Sanic 照原样绑定（unix socket 用临时名、权限默认 0o666（`UNIX_SOCKET_MODE`）、改名替换，退出时删除），原生主机在这个 socket 上接受连接（shard 0 接受后分给各 shard），它在服务期间保持打开、随服务器关闭，所以传入的 `sock`（TCP 或 AF_UNIX）、`app.serve_location` 与原版一致。TCP 地址由 Sanic 只做预留（绑定、不监听，并允许 SO_REUSEPORT），每个服务进程在同一地址上开自己的 SO_REUSEPORT 监听 socket，由内核按连接的四元组哈希把连接分给各进程（连接少时进程之间会不均）；地址已被占用时照样在绑定时报错。unix socket 没有 SO_REUSEPORT：多 worker 时由主进程绑定一次，各 worker 都在这一个 socket 上接受连接，退出时由主进程删除；各 worker 经主进程分配的共享内存公布自己的连接数，轮流接受：连接数不是最少的 worker 暂不 accept（每 50 µs 起再看一次，连接等了 1 ms 仍没有人接时照接；退出的 worker 立即让出，崩溃或卡住的 200 ms 后不再等它；重启的 worker 接替原来的位置），否则 shard 正忙（轮询而不睡眠）的那个 worker 几乎总是先接到新连接。一个 worker 停止（重启、缩减）不会关闭其他 worker 仍在用的这个 socket。用户传入的共用 socket 同样如此。原版是每个 worker 各自绑定再改名覆盖路径，结果只有最后改名的 worker 收到连接。放在 nginx 之后的部署见下一节。
- 只服务明文 HTTP/1.1：TLS 由前端代理（如 nginx）终止，本包不提供 TLS 与 HTTP/3（见「与 Sanic 的差异」）。
- 多 worker：会启动 worker 进程的运行（`workers > 1`、`fast`、`auto_reload`、`dev`）照原版由 Sanic 的 worker manager 管理（重启、扩缩、`app.m` 都相同），每个 worker 进程各自运行原生主机：主进程把 CPU 分成每个 worker 一组，worker 的 Python 线程固定在组内第一个 CPU，shard 用其余的（按 worker 编号取组；编号见 `NATIVE_WORKER_LISTEN`）。`fast=True` 启动能放下的 worker 数（CPU 数 ÷ (1 + `NATIVE_SHARDS`)），而不是每个 CPU 一个。worker 数多于 CPU 组时，多出的 worker 与前面的共用 CPU 组（记一条警告）。
- 用 `fork` 启动方式、且父进程已经运行过原生主机时（Seastar 不能在 fork 出的子进程里再次启动），子进程改用 asyncio 服务器并记一条警告；默认的 `spawn` 不受影响。
- 每个进程只有一个原生服务器：同一进程同一事件循环上的第二个服务器（如第二个应用的服务器）使用 asyncio 服务器（记一条警告），两者都正常服务。

协议错误（如非法请求行、请求头过大、坏的 chunked 编码、请求体超限）与原版报出相同的状态码和消息，并交给应用的错误处理器。

## 放在 nginx 之后

多 worker 放在 nginx（或 OpenResty）之后时，推荐让每个 worker 另外监听自己的地址，由 nginx 按请求均衡：

```python
app.config.NATIVE_SERVER = True
app.config.NATIVE_WORKER_LISTEN = "/run/app/{n}.sock"   # worker 0 是 /run/app/0.sock，依次类推
app.config.TRUSTED_PROXIES = "unix"                     # 只信任经 unix socket 来的转发头
app.config.UNIX_SOCKET_MODE = 0o660                     # 只有服务的用户与同组（nginx）能连上
app.config.PROXIES_COUNT = 1
app.run(unix="/run/app/app.sock", workers=2)            # 运行本身的地址照常服务（健康检查、直接访问）
```

```nginx
upstream app {
    least_conn;
    server unix:/run/app/0.sock;
    server unix:/run/app/1.sock;
    keepalive 32;
}
server {
    location / {
        proxy_pass http://app;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

原因（OpenResty 1.31 实测）：新版 nginx 不写任何设置时也对上游用 HTTP/1.1 并复用连接，且优先取最近释放的空闲连接。负载不高时少数几条“热”连接承担大部分请求，所以只给 nginx 一个地址时，请求集中在这几条连接所在的 worker：约四成负载（2 万请求/秒，两个 worker 共约 5 万/秒的能力）时，较闲 worker 与较忙 worker 的请求比，TCP 地址最低 0.10，unix socket 最低 0.72，较忙 worker 的 Python 线程到 0.9 核；每个 worker 一个地址并用 `least_conn` 时为 0.97–1.0，两边各约 0.65 核。服务器只能决定连接落在哪个进程，决定不了 nginx 在已有连接上怎么分请求，所以要让 nginx 分别看待各 worker。

- `TRUSTED_PROXIES = "unix"` 信任所有能连上这些 socket 的进程。socket 的权限默认与 Sanic 一样是 0o666，本机任何账号都能连上并伪造 `X-Forwarded-For`；所以同时设 `UNIX_SOCKET_MODE = 0o660`，并让 nginx 的用户属于服务进程的组（或把 socket 放在只有两者能进入的目录里）。
- 同机部署用 unix socket，省掉 loopback TCP 的开销；`NATIVE_WORKER_LISTEN` 也可以是 `"127.0.0.1:81{n:02d}"` 这样的 TCP 地址（8100、8101……）。
- worker 退出时删除自己的 socket 文件；重启的 worker 绑定同一地址。重启期间连接不上的那个地址被 nginx 暂时标记为失败（`max_fails`、`fail_timeout`，默认 1 次、10 秒），请求改走其他 worker。
- worker 数改变时，nginx 的 `upstream` 要同时增减。
- 监听 socket 的 backlog 默认 2048：nginx 同时打开上百条上游连接时，unix socket 不会因队列满而让 nginx 回 502（Sanic 的 100 会）。

## 与 Sanic 的差异

本包不再追求与 Sanic 的公开接口一致。下面列出刻意的差异，oldman 等按 Sanic 写的代码据此调整；每处改动在同一个 commit 里改测试。

接口：

- 包名 `oldman_server`，不再提供 `import sanic` 的别名。类名去掉 Sanic：`Sanic` → `OldmanServer`、`SanicException` → `OldmanServerException`、`DefaultSanic` → `DefaultServer`、`SanicTestClient` → `TestClient`（内部类同样去掉 Sanic 字样）。配置环境变量前缀 `SANIC_` → `OLDMAN_SERVER_`（如 `OLDMAN_SERVER_NATIVE_SERVER`；不用 `OLDMAN_`，因为 oldman 自己的 `OLDMAN_*` 变量会被读成设置）；日志名 `sanic.root` / `error` / `access` / `server` / `websockets` → `oldman_server.*`；进程名 `Sanic-Main`、`Sanic-Server-*` → `Oldman-Main`、`Oldman-Server-*`。命令行程序名、`--version`、启动信息（MOTD）与 HTML 错误页显示 oldman-server，不再带 Sanic 的标志和 sanic.dev 链接；颜色 `Colors.SANIC` → `Colors.PINK`；健康检查端点（`oldman_server.ext`）的蓝图 `SanicHealth` → `OldmanServerHealth`（路由名与 OpenAPI 标签随之改变）。日志与报错里仍指向 sanic.dev 的说明，是原版 Sanic 的文档（框架层源自它）。
- Sanic Extensions 随包提供为 `oldman_server.ext`，只在应用要求时启用：`app.extend()`、`app.ext` 或 `Extend(app, ...)`。不再查找 `sanic_ext` 模块，服务器启动时也不会自动扩展应用（原版装了 sanic-ext 时会）；`AUTO_EXTEND = False` 时 `app.extend()` 抛 `RuntimeError`。
- 不再把 setuptools 列为运行依赖（包里没有用到它；原版 Sanic 列着）；`oldman_server.ext` 在没有 pydantic 时也能导入（原版 sanic-ext 会因此导入失败）。
- 测试客户端随包提供为 `oldman_server.testing`（源自 sanic-testing 24.6.0），`app.test_client` 需要 httpx；不再需要 sanic-testing。
- 新增 `response.abort()`：对 `request.respond()` 得到的流式响应，在已发出的数据之后关闭连接、不完成响应（没有结束块，或少于 Content-Length），请求方因此知道内容不完整；之后再 `send()` 会抛出，handler 在下一次 await 被取消（与请求方断开时相同）。还没发出任何数据时抛 `ServerError`（这时应抛异常或返回错误响应），响应已结束时什么都不做。asyncio 服务器与原生主机行为相同；oldman 代理里的 `request.transport.get_protocol().close()`（在原生主机上不起作用）可以换成它。
- 新增 `@app.rewrite_path`：注册的函数在路由与所有中间件之前对每个请求执行，返回新路径（以 `/` 开头）或 None；按注册顺序依次执行。之后请求的 `path` 和路由都用新路径，查询串保留，`raw_url` 仍是请求方发来的。抛出的异常交给错误处理器。oldman 的 `I18nRequest`（重写私有的 `_parsed_url`）可以换成它：自定义请求类会让原生主机走一般路径，用这个钩子则保留快速路径。
- 新增设置 `UNIX_SOCKET_MODE`：服务器绑定的 unix socket（`unix=`、`NATIVE_WORKER_LISTEN`）的权限位，默认 0o666（与 Sanic 相同）；可写 `0o660` 或八进制字符串（环境变量写 `OLDMAN_SERVER_UNIX_SOCKET_MODE=0o660`）。不是 0–0o777 的值在绑定时报 `ValueError`。
- 新增设置 `TRUSTED_PROXIES`：地址或网段（列表，或逗号分隔的字符串，如 `"127.0.0.1, 10.0.0.0/8"`），`"unix"` 表示 unix socket 连接（能连上 socket 的进程都被信任，权限见 `UNIX_SOCKET_MODE`）。设置后，只有来自这些来源的连接，其 `Forwarded` / `X-Forwarded-*` 等转发头才按 `FORWARDED_SECRET`、`REAL_IP_HEADER`、`PROXIES_COUNT` 生效；其他连接的 `remote_addr` 为空，`client_ip`、`scheme`、`host` 取实际连接。双栈监听收到的 IPv4 映射地址（`::ffff:a.b.c.d`）按 IPv4 网段比较。格式错误在服务器启动时报 `ValueError`。不设置时与原版相同：转发头对所有连接都按上述设置生效。
- 新增 `HTTPMethodView.before_dispatch(request, *args, **kwargs)`：覆写后在方法 handler 之前以同样的参数执行（可为同步或协程），返回响应则直接用它回应，返回 None 才进入 handler；抛出的异常交给错误处理器。不覆写时没有额外开销。oldman 的 `HTTPMethodView` 不必再复制整个 `dispatch_request`，把登录与权限检查放进这里即可。
- 已移除：coffee 模式（`app.make_coffee()`、`prepare(coffee=...)`、`--coffee`，Sanic 标志的彩蛋）；REPL（`--repl`、`app.repl_ctx`）；后台运行（`--daemon` 及 `kill` / `status` / `restart` / `stop` 命令，进程管理交给 systemd 或 oldman 自己的服务命令）；inspector（`INSPECTOR*` 设置、`app.inspector`、`inspect` 命令）；TLS（`run()` / `prepare()` / `create_server()` 的 `ssl`、`auto_tls`，`--cert` / `--key` / `--tls` / `--auto-tls`，`LOCAL_CERT_CREATOR` 等设置，`certloader_class`，`conn_info.ssl` / `cert` / `server_name`）与 HTTP/3（`version` 参数、`--http` / `-1` / `-3`、`request.stream_id`、`Alt-Svc`）：TLS 由前端代理终止，`request.scheme` 照旧取 `Forwarded` / `X-Forwarded-Proto`；ASGI（应用不再是 ASGI 可调用对象，`app.asgi`、`app.asgi_client`、`request.scope` 一并移除），只由本包自己的服务器运行；Windows 支持（原生主机只能运行在 Linux 上）。
- 请求头的值去掉两端的空白（RFC 9110 的可选空白；原版只去掉开头，`X-A: v␠␠` 原版得到 `"v  "`），两种服务器相同；迭代 `request.headers` 得到的名字都是小写（原版的解析器本来如此，原生主机现在也一样）。
- 监听 socket 的 backlog 默认 2048（`oldman_server.constants.DEFAULT_BACKLOG`，与 Gunicorn、Uvicorn 相同），Sanic 为 100：nginx 等代理同时打开上百条上游连接时，unix socket 的 `connect()` 超出 backlog 即失败，nginx 回 502；仍可用 `run(backlog=...)` 指定，内核以 `net.core.somaxconn` 为上限。
- JSON 默认用 orjson（`json()`、`request.json`、JSON 错误页与 JSON 日志），不再用 ujson：输出为紧凑的 UTF-8（非 ASCII 字符不再转成 `\uXXXX`）；datetime、UUID、dataclass、enum 可直接编码；非 str 的键照旧转成字符串；超过 64 位的整数会抛 `TypeError`；`NaN`、`Infinity` 输出为 `null`（原版输出不合法的 `NaN`）；不认识的对象不再调用 `__json__` 或 `toDict()`，`Decimal`（数据库驱动的 DECIMAL 列）与含孤立代理项的字符串也会抛 `TypeError`，要编码它们就用 `json(..., default=...)`（如 `default=str`，或把 `Decimal` 转成 `float`）。`request.json` 用 orjson 解析：超出 64 位的整数会变成 `float`（失去精度），`NaN`、`Infinity` 不被接受（400）；要精确的大整数就用 `OldmanServer(loads=json.loads)`。JSON 与文本错误页里编码器不认识的上下文值显示为它的 `str()`，不会让错误响应本身变成 500。`json()` 的额外关键字参数交给 orjson（`default`、`option`，如 `option=orjson.OPT_SORT_KEYS`）；`json_dumps` 返回 bytes。要换编码器仍可用 `json(..., dumps=...)` 或 `OldmanServer(dumps=..., loads=...)`。

行为修正（oldman 原先为此打的补丁，切换后可以删除）：

- worker 启动期间收到 SIGTERM 时，主进程结束服务，而不是一直等待 worker 确认（Sanic 25.12.1 会空转；oldman 的 `_wait_for_ack`）。
- worker 在已确认启动之后、开始服务之前失败（`after_server_start`，或原生主机启动失败）时，主进程结束服务，而不是把这条消息当作要重启的进程名、留下没有 worker 的主进程（oldman 的 `_handle_message`）。只有启动阶段的失败才结束服务：已经服务过的 worker 在停止时（重启、缩减）出错只记日志；重启的 worker 在确认之前失败（如 auto-reload 读到有语法错误的文件）时记一条错误，其他 worker 照常服务（与原版相同）。服务器启动失败时 `app.run()` 抛出异常（原版只记日志、正常返回，退出码 0）。
- multiprocessing 的启动方式已被设为本包需要的那一种时不再报错（oldman 的 `prepare_process_context` 直接设 `START_METHOD_SET`）。
- 服务器监听器成对执行：`before_server_start` 失败时仍执行 `after_server_stop`；`after_server_start` 失败时照常停止服务器并执行 `before_server_stop`、`after_server_stop`；`before_server_stop` 失败时清理与 `after_server_stop` 仍执行；失败最后照样抛出。监听器抛出的 `RuntimeError` 也不再被当成事件循环提前停止而吞掉或再执行一次。（oldman 的 `_services_*_server_*` 里的 except 清理）
- 服务进程的 SIGINT / SIGTERM 处理在开始应答之前就装好，并且是 Python 层的处理：启动期间收到的信号等启动完成再停止（第二个信号立即停止），停止阶段照常执行。原版用事件循环的 `add_signal_handler`，uvloop 会丢掉循环没在运行时（启动的各步骤之间）到达的信号，进程就停不下来；原生主机还会在注册之前就应答请求，这时的 SIGTERM 直接杀死进程、不执行停止监听器。`loop.add_signal_handler` 不再被调用。
- worker 进程在主进程退出（包括被 SIGKILL）时收到 SIGTERM 而停止，不再成为无人管理、仍占着端口的孤儿进程（Linux；oldman 的 `_process_group` 检查仍可保留给其他子进程）。
- worker manager 结束服务时（worker 启动失败、关机时再收到信号等）只杀 worker 进程：原版对 worker 所在的进程组 `killpg`，而 worker 与主进程同组，主进程也随之被 SIGKILL，`main_process_stop` 监听器、套接字与 unix socket 文件的清理都不执行。现在主进程照常收尾，以退出码 1 结束；自成进程组的 worker 仍连同其进程组一起杀掉。
- `Authorization` 的认证方案取头部第一个词、不分大小写比较，不再按子字符串匹配：`request.token` 只认 `Bearer` / `Token`，其他方案（如 `Basic`）得到 None，不再返回整个头部；`request.credentials` 的 Basic 密码可以含冒号，非法 base64 视为没有凭据（oldman 的 `bearer_credential`、`basic_credentials`；只接受 Bearer 时用 `parse_credentials(header, ("Bearer",))` 并检查返回的方案）。不带方案的单个令牌仍按令牌处理；只有方案名、没有凭据（`Authorization: Bearer`）时没有令牌（`token` 为 None，原版是空字符串）；方案与凭据之间的任意空白（如 Tab）都算分隔。
- 响应中间件抛出的异常与 handler 抛出的一样交给错误处理器，送出错误处理器的响应，这个响应不再经过响应中间件；原版只记一条日志、照样送出原响应（oldman session 的 `save_session` 为此自己把响应换成 500，切换后直接抛出即可）。流式响应在 `request.respond()` 处抛出，handler 不拦截时同样交给错误处理器；拦截之后可以改回别的响应（它不再经过响应中间件）。送错误响应时（包括错误处理器自己调用 `request.respond()`）响应中间件再出错，仍只记日志、照送错误响应（与原版相同）。
- `content_type=None` 的响应不发送 `Content-Type`，不再发送字面值 `None`（oldman 代理里的默认值）。
- 内置扩展类 `HealthExtension`、`HTTPExtension`、`InjectionExtension`、`OpenAPIExtension`、`TemplatingExtension` 从 `oldman_server.ext` 导出（oldman 原先从内部模块路径导入）。`TemplatingExtension` 需要 Jinja2，用到时才导入，不在 `from oldman_server.ext import *` 里。

经原生主机时：

- `request.headers` 是 `NativeHeaders`：`[]`、`get`、`getone`、`getall`、`in` 与属性访问（`headers.content_type`）直接查原生请求；迭代、`items()`、`len()`、`==`、`copy()` 与修改在第一次用到时建一个 `Header`，之后都在它上面进行，结果与 `Header` 相同。它是 `multidict.MultiMapping`（也是 `Mapping`），但不是 `CIMultiDict`：`isinstance(request.headers, CIMultiDict)` 为 False。
- `request.head`、`raw_headers`、`request_line` 由解析出的请求行与字段重建（冒号后一个空格、值已去掉两端空白、名字保留请求方的大小写），不是请求方发来的原始字节。
- 响应头最多 128 个字段、合计约 8 KiB，超出时交给错误处理器（500）；asyncio 服务器没有这个上限（放在 nginx 之后时，nginx 默认的 `proxy_buffer_size` 也只容纳 4 或 8 KiB 的上游响应头）。
- `request.protocol` 是原生连接的替身：提供 `state`、`version`、`conn_info`，不是 `HttpProtocol` 实例。
- 原生服务的默认运行没有 worker manager，所以没有 `app.m`（multiplexer）。这与原版 `single_process=True` 相同。
- 原生 WebSocket 不构造 `WebSocketProtocol`，但同样由 `WebsocketImplProtocol` 处理，`WEBSOCKET_*` 设置照样生效。
- `fast=True` 启动 CPU 数 ÷ (1 + `NATIVE_SHARDS`) 个 worker，而不是每个 CPU 一个：每个原生 worker 要占一组 CPU。
- `app.create_server()` 创建后即开始服务：`asyncio_server_kwargs`（如 `start_serving=False`）只用于 asyncio 服务器。
- 关机时被截断的请求不另记 `Request: ... stopped. Transport is closed.` 日志（asyncio 服务器会记）。
- 流式响应开始后不再受 `RESPONSE_TIMEOUT` 约束；原版在两次发送之间计时。

## 测试与测量

`tests/framework`、`tests/ext` 源自 Sanic 25.12.1 与 sanic-ext 25.12.0 的测试集（MIT），已改为本包的 import 名和行为，两种运行方式都要全部通过：

```sh
python scripts/run_framework_correctness.py --sanic --out build/correct-<目录>    # 本项目原生主机测试，每个用例独立进程
.venv/bin/python -m pytest tests/framework tests/ext                              # 框架测试：asyncio 服务器
OLDMAN_SERVER_NATIVE_SERVER=true OLDMAN_SERVER_NATIVE_SHARDS=1 .venv/bin/python -m pytest tests/framework tests/ext   # 框架测试：经原生主机
python scripts/calibrate.py build/calib-<目录> candidate:small candidate:body      # 服务端吞吐与主线程 µs
python probes/sanic_comparison/run.py --python-only --out build/compare-<新目录>     # 阶段验收：与原版 Sanic 配对对照
```

## 发布

推送 tag `v<版本>`（与 `src/oldman_server/__version__.py` 一致，如 `v0.1.0`）后，`.github/workflows/release.yml`：

1. 在 manylinux_2_28 容器里分别为 x86_64 与 aarch64 构建 wheel（`scripts/build_wheels.sh`）：按 `dependency-lock.json` 下载依赖源码并校验 SHA-256（`scripts/fetch_sources.py`，Seastar 打上 `patches/` 里的补丁），构建 Boost、OpenSSL、fmt、yaml-cpp、c-ares、protobuf、liburing、hwloc（不带插件）与 Seastar，再为 CPython 3.12–3.14 各构建扩展、打包（`hatch_build.py` 把扩展放进 wheel 并标上解释器与平台；树里没有扩展时拒绝构建 wheel，`OLDMAN_SERVER_PURE_WHEEL=1` 才构建只能用 asyncio 服务器的纯 Python wheel），由 auditwheel 把依赖库复制进 wheel、标为 manylinux_2_28。容器镜像按标签与摘要固定（`wheels.yml`）；lz4 与构建所需的头文件取自镜像的发行版仓库（签名的 RPM，版本不在锁文件里，记入构建日志与缓存键）。依赖与 Seastar 的构建结果按锁文件、补丁、脚本、镜像、编译器与这些 RPM 的版本缓存。
2. 核对每个 wheel 所带的库都附有许可证（`scripts/check_wheel_licenses.py`），再把每个 wheel 装进全新环境：只装声明的依赖时能导入，再装上测试依赖，跑经 asyncio 服务器与经原生主机的框架测试、原生主机的运行测试（`scripts/test_wheels.sh`）。auditwheel 只标 manylinux_2_28（`--only-plat`：测试只在 glibc 2.28 的镜像里做，不标更早的 glibc）。另在 `python:3.12/3.13/3.14-slim` 镜像里安装每个 wheel 并加载原生扩展：manylinux 允许 wheel 依赖少数系统库（libstdc++、libgcc_s、libz、libatomic）而不自带，精简镜像未必都有。
3. 经 PyPI trusted publishing 发布（环境 `pypi`，只接受 `v*` tag 的部署），再用 runner 自带的 `gh` 建 GitHub release，附上 wheel 与 SHA256SUMS。工作流里的 action 都固定到提交。

每次推送与 PR 由 `.github/workflows/ci.yml` 构建并测试同样的 wheel，不发布。wheel 带上所含第三方库的许可证（`LICENSES/`，作为包的 license files）：auditwheel 复制进 wheel 的每个库、编进扩展的 pybind11，都要在上述脚本的清单里并有许可证文件，否则测试失败。Seastar 以 `--as-needed` 链接：它只用到 SCTP 头文件里的常量，所以 wheel 不带 libsctp（LGPL）。

## 目录

- `src/oldman_server/`：框架（源自 Sanic 25.12.1 的 fork）。
- `src/oldman_server/native/host.py`：应用与原生主机之间的适配层，包括请求、响应、流、文件、协议错误和 WebSocket。
- `src/oldman_server/native/engine.py`：原生运行时的 Python 端（事件取出、每请求 Task、回传批次、背压、文件操作）。
- `native/`：Seastar 服务（`server.cc`）、HTTP/1.1 解析与写出（`http1.cc`）、multipart 解析（`multipart.hh`）、Python 绑定（`bridge.cc`，构建为 `oldman_server.native._native`）。
- `src/oldman_server/ext/`：Sanic Extensions（源自 sanic-ext 25.12.0，许可见 `LICENSE.sanic-ext`）。
- `src/oldman_server/testing/`：测试客户端（源自 sanic-testing 24.6.0，许可见 `LICENSE.sanic-testing`）。
- `upstream/`：所依据的上游版本（归档地址、提交）与各文件的 SHA-256。与原版 Sanic 的性能对照要先把 `upstream/sanic.json` 的归档解压到 `build/sanic-upstream/`（得到 `sanic-<提交>/`），对照前逐个文件核对。
- `tests/`：本项目测试，其中 `tests/framework`、`tests/ext` 源自 Sanic 与 sanic-ext 的测试集；`scripts/`：构建、测试与测量工具；`probes/`：对照与诊断用的服务端和驱动；`python/seastar_http.py`：不经框架的轻量 App（研究用）。

## 许可证

MIT（`LICENSE`）。框架层源自 Sanic 25.12.1，`oldman_server.ext` 源自 sanic-ext 25.12.0，`oldman_server.testing` 源自 sanic-testing 24.6.0，均为 MIT（各自的许可证在包内）；发布的 wheel 所含第三方库（Seastar、Boost、OpenSSL、pybind11 等）的许可证见 `LICENSES/`，包的许可证表达式因此是 `MIT AND Apache-2.0 AND BSD-2-Clause AND BSD-3-Clause AND BSL-1.0`。wheel 里的 Seastar 打过一个补丁（`patches/`，说明见 `LICENSES/seastar-CHANGES.txt`）。
