[English](../en/configuration.md) | 简体中文

# 配置

设置照 Sanic 的方式给出：`app.config.X = ...`，或环境变量 `OLDMAN_SERVER_X`。

## Sanic 的设置

Sanic 的设置照常生效，原生主机按下表执行：

| 设置 | 原生主机的行为 |
|---|---|
| `REQUEST_MAX_SIZE`、`REQUEST_MAX_HEADER_SIZE`、`REQUEST_BUFFER_SIZE` | 请求体与请求头上限，与 Sanic 的计算相同（请求体上限在 handler 读取时检查，`stream=True` 的 handler 不受它约束）；还在到达的请求体按 `REQUEST_BUFFER_SIZE` 流控：交给 Python 而 handler 还没读走的部分不超过它，handler 读得慢，连接就收得慢 |
| `REQUEST_TIMEOUT`、`KEEP_ALIVE`、`KEEP_ALIVE_TIMEOUT` | 由原生层执行，按 Sanic 的阶段计时：两个请求之间是 `KEEP_ALIVE_TIMEOUT`，请求头到达期间是 `REQUEST_TIMEOUT`（到期由错误处理器回 408），handler 运行期间（包括请求体还在到达时）是 `RESPONSE_TIMEOUT`。与原版一样，小于 0.1 s（包括 0）按 0.1 s，无穷大表示不限；超过 24 h 按 24 h（写出的期限也是） |
| `RESPONSE_TIMEOUT` | 到期时取消 handler，由错误处理器回 503 “Response Timeout”；请求体还在到达时，每收到数据就重新计时（与原版相同）。超过 24 h 按 24 h |
| `GRACEFUL_SHUTDOWN_TIMEOUT` | 停止时不再接受连接、关闭空闲连接，在这段时间内等在途请求结束、响应写完，之后取消与切断剩下的（见[与 Sanic 的差异](differences.md#经原生主机时)） |
| `KEEP_ALIVE_TIMEOUT` + `GRACEFUL_TCP_CLOSE_TIMEOUT` | 写出的期限：请求方这么久不接收响应（一次写出，大响应按每 1 MiB 计）就断开连接 |
| `ACCESS_LOG`、`WEBSOCKET_*`、错误处理器、`FALLBACK_ERROR_FORMAT` | 与原版相同 |

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

## 内存分配器

Python 与原生主机的内存分配大多经过 glibc 的 malloc，换成 tcmalloc 后，在性能对照用的机器上，服务端每请求 CPU 少约 7–11%（小 JSON 单进程 23.1 → 20.6 µs；jemalloc、mimalloc 介于两者之间，见[性能对照](benchmarks.md#内存分配器)）。用 `LD_PRELOAD` 载入即可，worker 进程随之继承：

```sh
sudo apt install libtcmalloc-minimal4   # Debian / Ubuntu
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libtcmalloc_minimal.so.4 python app.py
```

也可以在应用里设 `PRELOAD`（库路径，多个用空格或冒号分隔；只在 Linux 上生效）：`app.run()`（及 `app.prepare()`）发现 `LD_PRELOAD` 里缺少它们时，带上它们把本程序重新执行一次（同样的解释器、选项与参数，之前的输出与副作用会再发生一次），worker 进程随之继承。路径不存在时启动报错；交互环境或从标准输入读入的程序无法重新执行，也报错，这时请在环境里设 `LD_PRELOAD`。
