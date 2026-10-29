[English](../en/configuration.md) | 简体中文

# 配置

设置用 `app.config.X = ...` 或环境变量 `OLDMAN_SERVER_X` 给出。

## 框架的设置

框架的设置在两种服务器上都生效，原生主机按下表执行：

| 设置 | 原生主机的行为 |
|---|---|
| `REQUEST_MAX_SIZE`、`REQUEST_MAX_HEADER_SIZE`、`REQUEST_BUFFER_SIZE` | 请求体与请求头上限，与 asyncio 服务器的计算相同（请求体上限在 handler 读取时检查，`stream=True` 的 handler 不受它约束）；还在到达的请求体按 `REQUEST_BUFFER_SIZE` 流控：交给 Python 而 handler 还没读走的部分不超过它，handler 读得慢，连接就收得慢 |
| `REQUEST_TIMEOUT`、`KEEP_ALIVE`、`KEEP_ALIVE_TIMEOUT` | 由原生层执行，按阶段计时：两个请求之间是 `KEEP_ALIVE_TIMEOUT`，请求头到达期间是 `REQUEST_TIMEOUT`（到期由错误处理器回 408），handler 运行期间（包括请求体还在到达时）是 `RESPONSE_TIMEOUT`。与 asyncio 服务器一样，小于 0.1 s（包括 0）按 0.1 s，无穷大表示不限；超过 24 h 按 24 h（写出的期限也是） |
| `RESPONSE_TIMEOUT` | 到期时取消 handler，由错误处理器回 503 “Response Timeout”；请求体还在到达时，每收到数据就重新计时（与 asyncio 服务器相同）。只计到响应开始为止，流式响应开始之后不再计时（两种服务器相同）。超过 24 h 按 24 h |
| `GRACEFUL_SHUTDOWN_TIMEOUT` | 停止时不再接受连接、关闭空闲连接，在这段时间内等在途请求结束、响应写完，之后取消与切断剩下的（见[与 Sanic 的差异](differences.md#经原生主机时)） |
| `KEEP_ALIVE_TIMEOUT` + `GRACEFUL_TCP_CLOSE_TIMEOUT` | 写出的期限：请求方这么久不接收响应（一次写出，大响应按每 1 MiB 计）就断开连接，handler 像请求方断开时一样被取消；asyncio 服务器相同 |
| `ACCESS_LOG`、`WEBSOCKET_*`、错误处理器、`FALLBACK_ERROR_FORMAT` | 与 asyncio 服务器相同 |

## 原生主机的设置

都在 `DEFAULT_CONFIG` 里有默认值，`app.config.NATIVE_*` 总能读到；也可用 `OLDMAN_SERVER_NATIVE_*` 环境变量设置。

| 设置 | 含义 |
|---|---|
| `NATIVE_SERVER` | 是否由原生主机服务：不设（默认）为 Linux 上有原生扩展时；`True` 为 Linux 上总是（缺少扩展时启动报错）；`False` 为从不（见[安装](installation.md#选择服务器)） |
| `NATIVE_SHARDS` / `NATIVE_SHARD_CPUS` | shard 线程数量（默认最多 2，取进程 CPU 集合中最后几个）或指定 CPU（只用于单进程）。多 worker 时设了 `NATIVE_SHARDS` 就保留 shard 线程，不自动用同线程模式 |
| `NATIVE_SAME_THREAD` | 同线程模式：不另开 shard 线程，进程唯一的 shard 在 Python 线程上运行，一个进程一个 CPU，省掉两个线程之间交接请求与响应。默认不设：worker 进程的 CPU 不够每个再配一个 shard CPU 时自动启用（`fast=True` 即每个 CPU 一个 worker），除非设了 `NATIVE_SHARDS`；单进程默认不用。设 `True` / `False` 则总是 / 从不 |
| `NATIVE_WORKER_CPUS` | 多 worker 时每个 worker 的 CPU 组，如 `"0,1,2;3,4,5"`（每组第一个给 Python，其余给 shard），或每组一个 CPU（`"0;1;2;3"`，同线程模式）；默认由主进程按 1 + `NATIVE_SHARDS` 个 CPU 一组依次切分，同线程模式每个 CPU 一组 |
| `NATIVE_GC_THRESHOLD` | 原生主机开始服务时，把 Python 垃圾回收的第一个阈值（年轻代）设为它（默认 10000），只在进程仍是 Python 自己的默认值（3.12 为 700，3.13 起为 2000）时；设 `None` 或 `0`（环境变量写 `0`）则不动。在途请求多时，默认阈值让年轻代每几毫秒回收一次、把在途请求的对象提升，很快引发扫描整个程序对象的全量回收（每次数毫秒，在途请求一起等，什么也回收不到）；调高后单进程 sleep 业务的主线程每请求少约 8%，p99 也更低。代价是循环引用的垃圾晚一些回收（最多多占约一万个对象） |
| `NATIVE_MAX_REQUESTS` | 同时在处理的请求上限（默认 4096，超出时由原生层直接回 503，不经错误处理器：这时 Python 已经满载） |
| `NATIVE_MAX_RESPONSE_SIZE` | 单个完整响应或流片段的上限（默认 128 MiB；从文件直接写入连接的 `file()` 不受它限制）。完整响应或流式响应的开头超出时交给错误处理器（500）；流式响应开始之后的片段超出时，连接在已送出的数据之后关闭 |
| `NATIVE_MAX_MEMORY` | 在途请求随请求头一起到达的请求体可占用的内存（之后到达的部分受 `REQUEST_BUFFER_SIZE` 流控） |
| `NATIVE_REACTOR_BACKEND` | shard 的 I/O：`epoll`（默认，空字符串相同）或 `io_uring`。io_uring 用 wheel 里另一份按 io_uring 构建的运行时（`liboldman_native_uring.so`，用到时才载入）；系统不允许 io_uring 时（容器默认的 seccomp 配置、`kernel.io_uring_disabled`、Linux 5.10 以前、内核分不出 ring 的内存），改用 epoll 并记一条警告。同线程模式两种都能用。其他取值启动时报错（原来的 `linux-aio` 已去掉） |
| `NATIVE_WORKER_LISTEN` | 每个服务进程另外监听的地址，`{n}` 换成 worker 编号（单进程为 0；n 个 worker 的编号是 0 到 n−1，缩减后再扩容的新 worker 取空出的编号，所以各地址与 CPU 组不变）：unix socket 路径（`"/run/app/{n}.sock"`，也可加 `unix:` 前缀，最长 63 字节）或 `host:port`（`"127.0.0.1:81{n:02d}"` 即 8100、8101……）。供前端代理按请求均衡各 worker（见[部署](deployment.md)）；运行本身的地址照常服务。默认不设 |

## 自动 HEAD 与 OPTIONS

| 设置 | 默认 | 含义 |
|---|---|---|
| `HTTP_AUTO_HEAD` | `True` | 路由组有 GET 而没有 HEAD 时，HEAD 请求由 GET 的那条路由回答（同样的 handler 与中间件），不送响应体 |
| `HTTP_AUTO_OPTIONS` | `True` | 路径有路由而没有 OPTIONS 时，OPTIONS 请求得到 204 与 `Allow`（这条路径的方法，排好序），只经过应用级的中间件 |

两者都不另外登记路由：显式登记的 HEAD、OPTIONS 路由优先，WebSocket 路由两者都没有，来源匹配的 CORS 预检在路由之前先回答（见下一节）。405 的 `Allow` 同样列出自动的 HEAD 与 OPTIONS。设为 `False` 则只回答登记了的方法，其余回 405。

## worker 存活监视

| 设置 | 默认 | 含义 |
|---|---|---|
| `WORKER_HEALTH_TIMEOUT` | 不设（不监视） | 秒数，至少 1：worker 的事件循环这么久没有心跳，worker manager 就重启它；不设或 `0` 不监视 |

每个 worker 在自己的事件循环上每 `WORKER_HEALTH_TIMEOUT` ÷ 4 秒（至少 0.5 秒）把单调时钟写进主进程分配的共享内存。执行不让出的代码（同步的 IO、死循环、长时间的计算）阻塞事件循环时，心跳停止。worker manager 发现某个已确认启动的 worker 超过期限没有心跳，在 `oldman_server.root` 记一条 WARNING（worker 名与多久没有心跳），先让它把各线程的调用栈写到它自己的 stderr（faulthandler，信号 SIGUSR2），约 0.2 秒后以 SIGKILL 结束它，再经与 `app.m.restart()` 相同的路径启动替换的进程。不是优雅停止：它的事件循环已经阻塞超过期限，上面的请求都已停住，而有自己 shard 线程的 worker 还会继续接受连接却不再回答，所以立即结束它。扩容、重启后的 worker 确认启动之后自动受监视；启动中、重启中的 worker 不检查；服务开始停止之后不再重启任何进程。只监视 worker manager 管理的服务 worker：单进程运行没有 worker manager，不监视；`app.manager.manage()` 启动的进程也不在其内。原生主机与 asyncio 服务器的 worker 相同。

## CORS

跨域资源共享在启动时汇总：下面的设置是应用的默认策略，路由、静态文件、固定响应、蓝图、蓝图的注册与蓝图组可以用 `cors=` 设自己的（`False` 为不做；`CORS(...)`（`from oldman_server import CORS`）没给的字段取这些设置的值）。路由表定下之后，所有策略合成一张表交给服务器，服务器写出每个响应时按请求的 `Origin` 附加头：handler 与错误处理器的响应、固定响应、原生缓存命中、过载的 503 都有，不经过中间件，也不在每个请求上运行 Python。

| 设置 | 默认 | 含义 |
|---|---|---|
| `CORS_ORIGINS` | 不设（关闭） | `"*"`，或允许的来源（列表，或逗号分隔的字符串，如 `"https://a.example, https://*.b.example"`；`https://*.b.example` 匹配 b.example 的任意子域，不含它本身） |
| `CORS_ALLOW_CREDENTIALS` | `False` | 发 `Access-Control-Allow-Credentials: true`。这时任何 CORS 头都不发 `*`：来源与 `CORS_EXPOSE_HEADERS`、`CORS_ALLOW_METHODS` 要写出名字（`*` 在启动时报错），`CORS_ALLOW_HEADERS` 的 `*` 按回显处理 |
| `CORS_ALLOW_METHODS` | `GET, HEAD, POST, PUT, PATCH, DELETE` | 预检回答的 `Access-Control-Allow-Methods` |
| `CORS_ALLOW_HEADERS` | 不设 | 预检回答的 `Access-Control-Allow-Headers`；不设时回显请求的 `Access-Control-Request-Headers` |
| `CORS_EXPOSE_HEADERS` | 不设 | `Access-Control-Expose-Headers` |
| `CORS_MAX_AGE` | `600` | 预检回答的 `Access-Control-Max-Age`（秒），`0` 不发 |

- 允许的来源得到回显的 `Access-Control-Allow-Origin`，不允许的不得到；按来源回显的策略在每个响应上都加 `Vary: Origin`（请求没带 `Origin` 也加，共享缓存因此按来源分开保存）。`"*"` 在每个响应上都发 `Access-Control-Allow-Origin: *`，不加 `Vary`。
- 预检（`OPTIONS` 带 `Origin` 与 `Access-Control-Request-Method`）来自允许的来源时，在路由之前回答 204，不运行中间件与 handler。只有默认策略时对任何路径都这样回答；有路由级策略时按 `Access-Control-Request-Method` 会到达的路由取策略。来源不允许、或路由不做 CORS 时，按普通的 `OPTIONS` 请求处理。
- 自己写 CORS 头的路由设 `cors=False`，服务器就不再附加。
- 设置不合法（凭据与 `"*"`、来源格式不对）在应用启动时报 `ValueError`；WebSocket 路由不接受 `cors=`。

```python
from oldman_server import CORS, OldmanServer
from oldman_server.response import json

app = OldmanServer("api")
app.config.CORS_ORIGINS = "https://app.example"


@app.get("/reports", cors=CORS(credentials=True))
async def reports(request):
    return json([])
```

## 内存分配器

Python 与原生主机的内存分配大多经过 glibc 的 malloc，换成 tcmalloc 后，在性能对照用的机器上，服务端每请求 CPU 少约 7–11%（小 JSON 单进程 23.1 → 20.6 µs；jemalloc、mimalloc 介于两者之间，见[性能对照](benchmarks.md#内存分配器)）。用 `LD_PRELOAD` 载入即可，worker 进程随之继承：

```sh
sudo apt install libtcmalloc-minimal4   # Debian / Ubuntu
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libtcmalloc_minimal.so.4 python app.py
```

也可以在应用里设 `PRELOAD`（库路径，多个用空格或冒号分隔；只在 Linux 上生效）：`app.run()`（及 `app.prepare()`）发现 `LD_PRELOAD` 里缺少它们时，带上它们把本程序重新执行一次（同样的解释器、选项与参数，之前的输出与副作用会再发生一次），worker 进程随之继承。路径不存在时启动报错；交互环境或从标准输入读入的程序无法重新执行，也报错，这时请在环境里设 `LD_PRELOAD`。
