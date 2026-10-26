# 性能对照

oldman-server 与原版 Sanic 25.12.1 在同一台机器、同样 CPU 下的完整对照，以及本项目各阶段的变化。README「现状」的数字取自这里。吞吐（RPS）越高越好，每请求 CPU 与延迟越低越好；比原版差的格子标 **↓**。

## 一览

2026-10-25，代码为 `7efd8fe`（之后只改了测量工具）；两行静态文件是 `378f09e`（大文件改由原生主机直接写入连接）之后重量的。

| 场景 | 本项目 RPS | 原版 Sanic RPS | 倍数 |
|---|---:|---:|---:|
| 单进程（3 核），小 JSON | 115,938 | 74,339 | 1.55 |
| 单进程（3 核），回显 1 KiB 请求体 | 90,074 | 64,344 | 1.42 |
| 单进程（3 核），handler 等 1 ms 计时器 | 93,301 | 81,467 | 1.15 |
| 4 个 worker（4 核），小 JSON | 196,320 | 93,625 | 2.10 |
| 4 个 worker（4 核），回显 1 KiB 请求体 | 167,530 | 83,078 | 2.03 |
| 4 个 worker（4 核），handler 等 1 ms 计时器 | 160,055 | 100,815 | 1.59 |
| wrk，空文本路由，256 连接 | 214,974 | 129,263 | 1.66 |
| wrk，handler 自己花 25 µs CPU | 89,876 | 69,958 | 1.28 |
| wrk，每个请求一条新连接，256 并发 | 43,877 | 25,040 | 1.75 |
| wrk，静态文件 4 KiB | 23,512 | 11,346 | 2.07 |
| wrk，静态文件 256 KiB | 10,057 | 7,092 | 1.42 |

每请求的服务端 CPU 都低于原版（原版的 0.48–0.78 倍）。

2026-10-26 加入 `ad2e4ce`–`b6ee4a6`（快速路径上的信号与更便宜的 CORS、Linux 默认用原生主机、固定响应与原生缓存、删去用不到的 Sanic 遗留）后，在桌面安静时（服务端核上其他程序 0–0.2 核）重跑了两组门槛，每一对仍领先：单进程小 JSON / 回显 / 等计时器 111,973 / 90,554 / 93,761 对原版 75,677 / 65,165 / 83,423 RPS（1.48 / 1.39 / 1.12 倍），4 个 worker 195,010 / 171,126 / 161,286 对 97,603 / 84,341 / 102,648（2.00 / 2.03 / 1.57 倍）。单进程小 JSON 比上表低约 3%；同一时段与 `7c6113b`（2026-10-24 的最好成绩）交替量单进程主线程，小 JSON 8.89 对 9.03 µs、回显 11.21 对 11.29 µs，差在噪声内。README「现状」的门槛数字取这一轮。

## 环境

- 机器：AMD Ryzen 7 3800X（8 核 16 线程），32 GB 内存，Ubuntu 20.04.6，Linux 5.15。
- CPython 3.14.7（普通 GIL 构建），两边都运行在 uvloop 0.22.1 上（对照驱动逐个窗口检查）。
- 本项目 0.1.0，原生主机用默认的 epoll，另测了 io_uring。原版 Sanic 25.12.1（`upstream/sanic.json` 的归档，对照前逐个文件核对）。
- `json()` 用各自的默认：本项目 orjson 3.13.0，原版 ujson 6.0.0。
- 服务端、客户端、驱动各自绑定不同的物理核。测量时机器空闲：配对对照每个窗口服务端核上其他进程的占用最多 0.94 核，wrk 最多 0.13 核。

## 方法

**配对对照**（`probes/sanic_comparison/run.py --python-only`）：

- 客户端是本仓库的 `build/http_load`（基于 Asio）：持久连接、闭环，逐个校验每个响应（状态、正文、进程号）。
- 每种业务先跑 2 个 A/A 窗，再让本项目与原版交替跑 4 对；每窗 2 秒预热、5 秒测量。倍数是 4 对吞吐比的几何平均。
- 业务：
  - 小 JSON：返回 `{"message":"Hello, World!"}`。
  - 回显：POST 1 KiB 请求体，响应带回它。
  - 等计时器：handler `await asyncio.sleep(0.001)` 后返回小 JSON，代表等数据库或其他服务的 I/O 型业务，不含客户端库自身的开销（那部分对任何框架都一样）。
- 单进程：服务端 3 核。本项目 1 个进程（1 个 Python 线程 + 2 个 shard 线程），原版 3 个 worker。客户端 3 核，128 条连接（等计时器 512 条）。
- 多 worker：服务端 4 核。本项目 4 个 worker（默认的同线程布局，每个 worker 一个 CPU），原版 4 个 worker。客户端 3 核，256 条连接（等计时器 1024 条）。
- 每请求 CPU：服务端进程树用掉的 CPU 时间除以请求数。延迟：客户端记录的每个请求的 p50 / p99；闭环测量下主要是排队（连接数 ÷ 吞吐）。

**wrk 参照**（`probes/wrk_comparison/run.py`）：

- 路由与 [web-frameworks-benchmark](https://web-frameworks-benchmark.netlify.app/) 相同（`/` 空文本），另加 handler 自己花 25 µs CPU 的 `/work` 和 `app.static` 的两个文件。
- 服务端 4 核，wrk 在另外 3 核。wrk 不校验内容，只统计非 2xx（全部为 0）。
- 每格 2 轮、交替顺序，每轮 3 秒预热、10 秒测量，取平均。
- 服务：
  - 本项目 4 个 worker（默认的同线程布局）；
  - 本项目 2 个 worker × (1 个 Python 线程 + 1 个 shard)；
  - 裸函数：不经框架、直接挂在原生主机上的一个 Python 函数，即原生主机自己的上限，两种布局都测；
  - 原版 4 个 worker。

## 配对对照

### 单进程（3 核）

| 业务 | 后端 | 本项目 RPS | 原版 RPS | 倍数（4 对最低–最高） | 每请求 CPU µs（本项目 / 原版） | p50 ms（本项目 / 原版） | p99 ms（本项目 / 原版） |
|---|---|---:|---:|---:|---:|---:|---:|
| 小 JSON | epoll | 115,938 | 74,339 | 1.55（1.49–1.60） | 21.4 / 40.3 | 1.07 / 1.58 | 1.71 / 3.46 |
| 回显 | epoll | 90,074 | 64,344 | 1.42（1.37–1.45） | 27.1 / 46.6 | 1.39 / 1.82 | 2.02 / 4.13 |
| 等计时器 | epoll | 93,301 | 81,467 | 1.15（1.14–1.15） | 25.7 / 36.8 | 5.43 / 6.02 | 7.15 / 9.46 |
| 小 JSON | io_uring | 111,745 | 73,566 | 1.51（1.45–1.54） | 21.8 / 40.8 | 1.10 / 1.62 | 1.77 / 3.59 |
| 回显 | io_uring | 92,806 | 64,623 | 1.43（1.42–1.44） | 26.1 / 46.4 | 1.35 / 1.80 | 2.04 / 3.96 |
| 等计时器 | io_uring | 92,838 | 81,191 | 1.15（1.13–1.15） | 25.4 / 36.9 | 5.44 / 6.02 | 7.42 / 9.64 |

每一对都领先原版。单进程的吞吐由 Python 主线程决定（它总是接近满一核），约为 1e6 ÷ 主线程每请求的 µs。

### 多 worker（4 核）

| 业务 | 后端 | 本项目 RPS | 原版 RPS | 倍数（4 对最低–最高） | 每请求 CPU µs（本项目 / 原版） | p50 ms（本项目 / 原版） | p99 ms（本项目 / 原版） |
|---|---|---:|---:|---:|---:|---:|---:|
| 小 JSON | epoll | 196,320 | 93,625 | 2.10（2.00–2.20） | 20.4 / 42.7 | 1.26 / 2.52 | 1.84 / 5.74 |
| 回显 | epoll | 167,530 | 83,078 | 2.03（1.98–2.06） | 23.8 / 48.1 | 1.49 / 2.83 | 2.22 / 6.06 |
| 等计时器 | epoll | 160,055 | 100,815 | 1.59（1.58–1.61） | 25.0 / 39.6 | 6.20 / 9.96 | 8.17 / 15.23 |
| 小 JSON | io_uring | 182,365 | 96,192 | 1.93（1.82–2.08） | 21.9 / 41.5 | 1.28 / 2.45 | 2.26 / 5.63 |
| 回显 | io_uring | 170,954 | 83,372 | 2.06（2.03–2.09） | 23.4 / 47.9 | 1.45 / 2.82 | 2.27 / 6.07 |
| 等计时器 | io_uring | 152,222 | 98,858 | 1.54（1.52–1.60） | 26.2 / 40.4 | 6.38 / 10.56 | 10.08 / 15.30 |

每一对都领先原版。小 JSON 的 19.6 万已接近客户端 3 核的上限（按每请求的客户端 CPU 估算约 21–23 万），所以这一项的倍数是下限。Linux 5.15 上 io_uring 与 epoll 互有高低，默认仍是 epoll。

## wrk 参照

### 空文本路由（长连接）

| 服务 | 64 连接 | 256 连接 | 512 连接 | 每请求 CPU µs（256） | p99 ms（256） |
|---|---:|---:|---:|---:|---:|
| 本项目，4 个 worker（默认） | 196,187 | 214,974 | 208,021 | 18.4 | 1.65 |
| 本项目，4 个 worker，io_uring | 198,193 | 222,801 | 218,240 | 17.8 | 1.60 |
| 本项目，2 × (1 + 1) | 165,531 | 167,385 | 163,939 | 22.9 | 2.08 |
| 裸函数，4 个进程 | 249,015 | 271,194 | 268,211 | 14.6 | 1.31 |
| 裸函数，2 × (1 + 1) | 169,365 | 167,154 | 166,115 | 19.9 | 2.00 |
| 原版 Sanic，4 个 worker | 121,335 | 129,263 | 129,023 | 30.7 | 3.69 |

裸函数与本项目之差（256 连接每请求约 4 µs CPU）就是框架层（路由、Request、Response、每请求 Task）的成本。

### handler 自己花 25 µs CPU（`/work`，256 连接）

| 服务 | RPS | 每请求 CPU µs | p99 ms |
|---|---:|---:|---:|
| 本项目，4 个 worker（默认） | 89,876 | 44.1 | 3.59 |
| 本项目，2 × (1 + 1) | 62,233 | 50.1 | 4.52 |
| 原版 Sanic，4 个 worker | 69,958 | 56.7 | 7.51 |

业务本身越重，框架省下的那部分占比越小；2 × (1 + 1) 只有两个核跑 Python，在这里低于原版，所以多 worker 的默认是同线程布局。

### 每个请求一条新连接（`Connection: close`）

| 服务 | 32 并发 | 256 并发 | 每请求 CPU µs（256） | p99 ms（256） |
|---|---:|---:|---:|---:|
| 本项目，4 个 worker（默认） | 29,707 | 43,877 | 81.6 | 6.20 |
| 本项目，4 个 worker，io_uring | 31,109 | 44,577 | 75.8 | 6.20 |
| 本项目，2 × (1 + 1) | 22,795 | 33,386 | 73.2 | 14.59 |
| 裸函数，4 个进程 | 52,190 | 52,487 | 65.7 | 4.28 |
| 原版 Sanic，4 个 worker | 17,822 | 25,040 | 116.0 | 405.62 |

每条连接的建立与关闭（内核 TCP 的成本占大头）远贵于一个请求，所以吞吐只有长连接的约五分之一。部署在 nginx 之后、上游开了 keepalive 时，新连接很少到达应用（见 README「放在 nginx 之后」）。

### 静态文件（`app.static`）

| 服务 | 4 KiB，256 连接 | 每请求 CPU µs | 256 KiB，64 连接 | 每请求 CPU µs |
|---|---:|---:|---:|---:|
| 本项目，4 个 worker（默认） | 23,512 | 169.0 | 10,057 | 397.0 |
| 本项目，4 个 worker，io_uring | 23,489 | 168.9 | 9,982 | 399.8 |
| 本项目，2 × (1 + 1) | 17,378 | 160.3 | 8,970 | 382.9 |
| 原版 Sanic，4 个 worker | 11,346 | 347.4 | 7,092 | 553.0 |

`378f09e` 之前，`file()`（`app.static` 也用它）把整个文件读成 Python 的 bytes，原生主机再复制一份写出，文件内容在内存里多复制了几次，还要为每个响应重新分配大块内存（剖析里缺页与内存分配约占三成 CPU）。256 KiB 的文件在默认布局下因此比原版慢 14%（6,092 对 7,057，每请求 CPU 655.5 对 555.9 µs）。现在 128 KiB 以上的文件不经 Python，由原生主机的文件线程读出、shard 直接写入连接（与 `file_stream()` 相同），是原版的 1.42 倍。更小的文件读进响应反而更省（逐个大小比较，两种做法大约在 100 KiB 打平），所以照旧；4 KiB 一列与改动前相同（之前 23,686 对 11,520）。

## 与其他 Python 框架（wrk，4 核）

2026-10-25，本项目代码为 `793c489`。[web-frameworks-benchmark](https://web-frameworks-benchmark.netlify.app/) 的 Python 应用（the-benchmarker/web-frameworks 的提交 `eb2f909`；网站同样用 CPython 3.14）照它们自己的写法与配置运行，与本项目同机、同 4 个服务端 CPU：

- 三条路由与网站相同：`GET /` 空响应、`GET /user/0` 回 `0`、`POST /user` 空响应。每个服务启动后先核对三条路由的响应，本项目另查运行在 uvloop 上。
- 每个服务 4 个 worker 或进程（按 CPU 数决定的设 `PYTHON_CPU_COUNT=4`）；本项目为 4 个同线程 worker。
- wrk 3 个线程在另外 3 个 CPU；64 与 256 连接，每格 2 轮、交替顺序，每轮 3 秒预热、10 秒测量。全部 2xx，没有 socket 错误；服务端 CPU 上其他程序的占用不超过 0.08 核。
- 版本：fastpysgi 0.6、weft-server 0.1.1、wred 0.13、oxapy 0.11.5、Robyn 0.88.0、Emmett 2.8.2（Granian 2.8.5）、Sanic 25.12.1、BlackSheep 2.6.4、Starlette 1.10.0、Litestar 2.25.0、FastAPI 0.162.0、lihil 0.2.41、asgineer 0.9.5；ASGI 应用由 uvicorn 0.54.0 服务；用 uvloop 的都是 0.22.1。
- 网站上的 peregrine（只有 manylinux_2_35 的 wheel）与 socketify（没有 CPython 3.14 的 wheel）在 Ubuntu 20.04 上装不上，没有测。

按每请求 CPU 排序（服务端进程树的 CPU 时间除以请求数，三条路由、两种连接数的平均）。标 * 的格子 wrk 的 3 个核已跑满（客户端约 2.9–3.0 核），RPS 只是下限，这时应看每请求 CPU。

**不经框架的函数**（服务器上直接挂一个手写判断路径的函数）

| 服务 | 服务器 | 每请求 CPU µs | RPS，64 连接 | RPS，256 连接 |
|---|---|---:|---:|---:|
| weft-wsgi | weft（Rust） | 11.9 | 300,459* | 302,512* |
| fastpysgi-wsgi | fastpysgi（C） | 12.5 | 302,055* | 296,292* |
| weft-asgi | weft（Rust） | 14.5 | 261,770 | 284,567* |
| 本项目裸函数 | 原生主机，4 个同线程进程 | 16.6 | 232,462 | 248,148 |

**框架**

| 服务 | 服务器 | 每请求 CPU µs | RPS，64 连接 | RPS，256 连接 |
|---|---|---:|---:|---:|
| oxapy | oxapy（Rust），同步 handler | 15.6 | 215,614 | 248,789 |
| wred | weft（Rust） | 16.3 | 234,124 | 253,672 |
| 本项目 | 原生主机，4 个同线程 worker | 21.0 | 183,048 | 195,297 |
| Emmett | Granian（Rust） | 28.5 | 126,114 | 145,024 |
| asgineer | uvicorn | 29.4 | 128,267 | 142,974 |
| lihil | uvicorn | 31.1 | 124,454 | 133,077 |
| BlackSheep | uvicorn | 33.0 | 116,150 | 125,317 |
| 原版 Sanic | Sanic | 33.4 | 118,411 | 119,473 |
| Starlette | uvicorn | 38.4 | 102,215 | 104,926 |
| Robyn | Robyn（Rust） | 53.1 | 68,922 | 81,175 |
| Litestar | uvicorn | 58.4 | 67,690 | 68,950 |
| FastAPI | uvicorn | 79.2 | 50,912 | 49,803 |

本项目不是最快的：框架里排第三，在 oxapy 与 wred 之后（两者的路由与请求处理在 Rust 里，Python 端只有 handler，没有 Sanic 那样的中间件、信号、异常处理与请求对象）；不经框架的函数则比 C / Rust 写的 WSGI 服务器每请求多约 5 µs。用 perf 对照本项目裸函数与 fastpysgi（256 连接，CPU 0–3 采样 15 秒），多出的都在用户态：原生主机约 3 对 1.2 µs（请求在 shard 与 Python 之间交接时的容器、请求头的复制、pybind11 调用，没有单个热点），Python 端约 2.5 对 0.8 µs，glibc 的 malloc / free 约 1.1 µs（换成 tcmalloc 能省下大半，见下文「内存分配器」）。

## 挂上 oldman 的五个扩展（wrk，4 核）

同一套 wrk 对照，两边都照 oldman 的方式挂上依赖注入、OpenAPI、HTTP（CORS 等）、健康检查与模板五个扩展：`Extend(app, config=Config(oas=False, oas_autodoc=False, templating_enable_async=True, logging=False), extensions=[...], built_in_extensions=False)`。原版用 sanic-ext 25.12.0（pydantic 2.12.5），本项目用随包的 `oldman_server.ext`；两边的响应头逐条相同，都运行在 uvloop 上。每请求 CPU 为三条路由、两种连接数的平均；每组的四个服务在同一时段交替。

| 服务 | 每请求 CPU µs | RPS，64 连接 | RPS，256 连接 |
|---|---:|---:|---:|
| 2026-10-25，本项目 `793c489` | | | |
| 原版 Sanic | 33.5 | 116,686 | 120,756 |
| 原版 Sanic + sanic-ext | 49.1 | 79,576 | 82,609 |
| 本项目 | 20.8 | 183,615 | 198,520 |
| 本项目 + ext | 48.4 | 81,076 | 83,459 |
| 2026-10-26，本项目 `7e99767` | | | |
| 原版 Sanic | 33.7 | 116,740 | 119,505 |
| 原版 Sanic + sanic-ext | 49.2 | 80,230 | 81,471 |
| 本项目 | 21.0 | 179,426 | 200,845 |
| 本项目 + ext | 30.6 | 126,772 | 134,114 |

`793c489` 挂上扩展后每请求多约 28 µs（原版多约 16 µs），领先完全消失：依赖注入扩展为每个应用注册了一个 `http.routing.after` 信号，原生主机因此从快速路径退回一般路径；CORS 钩子为了检查响应头而建出整份响应头，响应不能再走快速的头部编码。`ad2e4ce` 起快速路径也按一般路径的位置触发这类信号，信号分派先查启动时存下的结果，CORS 不再建响应头：扩展只多约 10 µs，对原版 + sanic-ext 为 1.6 倍。

## 内存分配器

单进程配对对照的设置（服务端 3 核：1 个 Python 线程 + 2 个 shard），本项目 `793c489` 以 `LD_PRELOAD` 换用不同的分配器，3 轮轮换顺序，取中位数。`LD_PRELOAD` 也作用到压测客户端，所以看服务端的每请求 CPU，不看 RPS。

| 分配器 | 小 JSON：服务端 µs | 主线程 µs | 回显请求体：服务端 µs | 主线程 µs |
|---|---:|---:|---:|---:|
| glibc 2.31（默认） | 23.1 | 9.49 | 27.3 | 11.01 |
| tcmalloc（gperftools 2.7，`libtcmalloc_minimal.so.4`） | 20.6 | 8.28 | 24.8 | 10.34 |
| jemalloc 5.2.1 | 21.6 | 8.67 | 26.0 | 10.78 |
| mimalloc 2.1.7 | 21.9 | 8.88 | 25.9 | 10.90 |

tcmalloc 在每一轮都最快，服务端每请求少约 9–11%；前一天另一组 3 轮的结果相同（小 JSON 23.0 → 21.4 µs，回显 27.8 → 25.2 µs）。用法见 README「配置」末尾。

## 各阶段的变化

### 对原版的倍数

不同日期的绝对 RPS 受桌面负载影响（同一份代码可差 10% 左右），只比倍数；同一天两次门槛的倍数也会差 0.05 左右。本页之前各行的客户端是同一个压测工具的 Seastar 版。

| 日期 | 状态 | 单进程 3 核：小 JSON / 回显 / 等计时器 | 4 核：小 JSON / 回显 / 等计时器 |
|---|---|---|---|
| 2026-09-30 | 起点：原版 Sanic 的代码直接挂在原生主机上（Seastar） | 0.46 / — / — | — |
| 2026-10-20 | 第一阶段：Python 层的快速路径 | 1.19 / 1.19 / — | — |
| 2026-10-21 | 第二阶段：原生 HTTP/1.1 层、响应构造 | 1.51 / 1.46 / — | — |
| 2026-10-21 | 多 worker：2 个 worker × (1 + 1) | — | 1.75 / 1.79 / 1.35 |
| 2026-10-22 | 发布前审查的修复之后 | 1.50 / 1.40 / 1.13 | 1.77 / 1.81 / 1.40 |
| 2026-10-24 | 底座从 Seastar 换为 Asio | 1.58 / 1.38 / 1.14 | 1.80 / 1.87 / 1.47 |
| 2026-10-24 | 多 worker 默认同线程布局（4 个 worker） | 1.57 / 1.45 / 1.16 | 1.90 / 1.91 / 1.52 |
| 2026-10-25 | 本页 | 1.55 / 1.42 / 1.15 | 2.10 / 2.03 / 1.59 |

起点那一行只量了小 JSON（3.05 万对 6.57 万 RPS）。等计时器从 2026-10-21 起才列入对照。2026-10-24 之前 4 核一栏都是 2 个 worker × (1 + 1)。

### 换底座时的同时段对照（2026-10-24）

同一时段交替运行 Seastar 版（tag `seastar-base`）与 Asio 版，各 3 轮取中位数，同样的客户端与布局。主线程 µs 是 Python 线程每请求用掉的 CPU。

单进程（1 个 Python 线程 + 2 个 shard）：

| 业务 | 指标 | Seastar | Asio epoll | Asio io_uring |
|---|---|---:|---:|---:|
| 小 JSON | 吞吐 | 110,901 | 117,779（+6.2%） | 113,300（+2.2%） |
| | 每请求总 CPU µs | 22.83 | 20.95（−8.2%） | 21.57（−5.5%） |
| | 主线程 µs | 9.00 | 8.44（−6.2%） | 8.70（−3.3%） |
| 回显 | 吞吐 | 84,431 | 90,976（+7.8%） | 88,658（+5.0%） |
| | 每请求总 CPU µs | 28.70 | 27.08（−5.6%） | 27.38（−4.6%） |
| | 主线程 µs | 11.79 | 10.92（−7.4%） | 11.16（−5.3%） |
| 等计时器 | 吞吐 | 91,999 | 94,040（+2.2%） | 93,167（+1.3%） |
| | 每请求总 CPU µs | 26.65 | 25.73（−3.5%） | 25.93（−2.7%） |
| | 主线程 µs | 10.84 | 10.61（−2.1%） | 10.72（−1.1%） |

2 个 worker × (1 + 1)：

| 业务 | 指标 | Seastar | Asio epoll | Asio io_uring |
|---|---|---:|---:|---:|
| 小 JSON | 吞吐 | 169,122 | 175,076（+3.5%） | 184,598（+9.2%） |
| | 每请求总 CPU µs | 21.57 | **22.59（+4.7%）↓** | 20.10（−6.8%） |
| | 主线程 µs | 9.85 | **11.23（+14.0%）↓** | 9.55（−3.0%） |
| 回显 | 吞吐 | 148,140 | 156,192（+5.4%） | 158,540（+7.0%） |
| | 每请求总 CPU µs | 25.62 | 25.37（−1.0%） | 23.96（−6.5%） |
| | 主线程 µs | 12.41 | **12.67（+2.1%）↓** | 11.73（−5.5%） |
| 等计时器 | 吞吐 | 141,664 | 153,203（+8.1%） | 158,867（+12.1%） |
| | 每请求总 CPU µs | 25.53 | 23.93（−6.3%） | 23.46（−8.1%） |
| | 主线程 µs | 11.67 | 11.19（−4.1%） | 11.17（−4.3%） |

吞吐在每一格都提高了。标出的主线程 µs 升高不是变慢：主线程每请求的 µs 随它的负载变化，一次醒来处理的请求越多，平均到每个请求越少；Asio epoll 在 2 个 worker 时 shard 先满，Python 线程每次醒来处理的请求少了（约 6–7 个对 io_uring 的 17–19 个），平均到每个请求就多了。同样固定每秒 12 万个请求时，两种后端的主线程都约 16 µs。

## 没有覆盖的

- nginx 等反向代理在前时：代理自身的成本对两边相同；多 worker 在 nginx 之后的请求分配见 README「放在 nginx 之后」。
- 超过 4 核：配对对照的客户端 3 核估计在 21–23 万 RPS 饱和，更多核的服务端在这台机器上测不出差别。
- TLS、HTTP/2、HTTP/3：本项目只服务明文 HTTP/1.1，TLS 由前端代理终止。
- 客户端库（Redis、MySQL、HTTP 客户端）的成本：对任何框架都相同，没有计入。

## 复现

先按 README 准备开发环境（`bash scripts/bootstrap.sh`，再 `source scripts/env.sh`），把 `upstream/sanic.json` 的归档解压到 `build/sanic-upstream/`。每组之前最好等机器安静（桌面程序会占核）：

```sh
python probes/sanic_comparison/run.py --python-only --out build/gate-single                              # 单进程 3 核
python probes/sanic_comparison/run.py --python-only --server-cores 4 --workers 4 --out build/gate-workers # 4 个 worker
OLDMAN_SERVER_NATIVE_REACTOR_BACKEND=io_uring python probes/sanic_comparison/run.py --python-only --out build/gate-single-io_uring
python probes/wrk_comparison/run.py --out build/wrk-root --services candidate,candidate-shards,bare,bare-shards,stock,candidate-io_uring
python probes/wrk_comparison/run.py --out build/wrk-work --path /work --connections 256 --services candidate,candidate-shards,stock
python probes/wrk_comparison/run.py --out build/wrk-close --close --connections 32,256 --services candidate,candidate-shards,bare,stock,candidate-io_uring
python probes/wrk_comparison/run.py --out build/wrk-small --path /static/small.bin --connections 256 --services candidate,candidate-shards,stock,candidate-io_uring
python probes/wrk_comparison/run.py --out build/wrk-large --path /static/large.bin --connections 64 --services candidate,candidate-shards,stock,candidate-io_uring
bash probes/framework_comparison/setup.sh                                   # web-frameworks-benchmark 的应用与各自的环境（build/framework-comparison）
python probes/framework_comparison/drive.py build/frameworks --services oldman-server-bare,fastpysgi-wsgi,weft-wsgi,weft-asgi,oldman-server,wred,oxapy,robyn,emmett,sanic,blacksheep,starlette,litestar,fastapi,lihil,asgineer
python probes/framework_comparison/drive.py build/frameworks-ext --services sanic,oldman-server,sanic-ext5,oldman-server-ext5   # 挂上扩展的一对
```

配对对照的每个窗口都写入 `row.json`（吞吐、延迟、每请求 CPU、服务端核上其他进程的占用），wrk 的每个窗口写入 `rows.jsonl`。
