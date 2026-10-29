English | [简体中文](../zh/benchmarks.md)

# Benchmarks

A full comparison of oldman-server with stock Sanic 25.12.1 on the same machine and the same CPUs, plus how oldman-server changed from stage to stage. The README numbers come from here. Higher throughput (RPS) is better; lower CPU per request and latency are better. Cells worse than stock Sanic are marked **↓**.

## At a glance

The first six rows (paired comparison) are from 2026-10-27, code `956ef97`. The five wrk rows are from 2026-10-25, code `7efd8fe`; the two static-file rows were measured again after `378f09e` (large files are now written to the connection directly by the native host). They have not been measured again since.

| Scenario | oldman-server RPS | Stock Sanic RPS | Ratio |
|---|---:|---:|---:|
| Single process (3 cores), small JSON | 159,028 | 76,469 | 2.07 |
| Single process (3 cores), echo a 1 KiB request body | 132,466 | 65,534 | 2.01 |
| Single process (3 cores), handler waits a 1 ms timer | 117,421 | 84,784 | 1.39 |
| 4 workers (4 cores), small JSON | 251,376 | 100,814 | 2.50 |
| 4 workers (4 cores), echo a 1 KiB request body | 223,347 | 86,306 | 2.59 |
| 4 workers (4 cores), handler waits a 1 ms timer | 186,170 | 104,483 | 1.76 |
| wrk, empty text route, 256 connections | 214,974 | 129,263 | 1.66 |
| wrk, handler spends 25 µs of CPU | 89,876 | 69,958 | 1.28 |
| wrk, a new connection per request, 256 concurrent | 43,877 | 25,040 | 1.75 |
| wrk, static file 4 KiB | 23,512 | 11,346 | 2.07 |
| wrk, static file 256 KiB | 10,057 | 7,092 | 1.42 |

Server CPU per request is lower than stock Sanic's everywhere (0.39–0.78 times stock Sanic's).

`9b600a7`–`6fcf554` on 2026-10-26: fewer memory allocations per request on the shards, a few trims of the fast path, and the request object the native host hands to Python changed from a pybind11 class to a plain C extension type. A plain function attached directly to the native host no longer goes through a Task; in same-thread mode the shard calls it and writes the answer back from within the connection loop (framework handlers still each run in a Task). Compared with 2026-10-25, echo improved the most (1.42 → 1.57).

Then `726ee60`–`2eb41f2`: less bookkeeping by the engine for each framework request (a complete request gets its Task directly, answers are queued as they are); the parse of a request target (path and query) is shared between requests, while requests whose path is rewritten still each have their own; finishing common responses is inlined; the status and headers of a framework answer are checked only once. The three single-process ratios are 1.84 / 1.69 / 1.21 (`6fcf554`: 1.60 / 1.57 / 1.14), and 2.28 / 2.33 / 1.66 with 4 workers.

After that, `52d7090`–`16fd826`: the framework coroutine ends the request itself instead of going through an engine callback; in same-thread mode the loop that takes requests creates the Task for framework handlers directly; for applications with no response middleware and no `http.lifecycle.response` signal, building the Request (together with setting `Request._current`) and writing back a plain response and ending the request (together with restoring `Request._current`) each happen in one native host call; the fast path's route lookup results are now kept by the router itself and dropped together when the route table is rebuilt. The three single-process ratios are 2.05 / 2.00 / 1.25, and 2.51 / 2.59 / 1.73 with 4 workers.

`b91b272`–`956ef97` on 2026-10-27: in same-thread mode, when the shard calls a bare function it no longer goes through two layers of Python methods; the checks, running in a copy of the context, the bookkeeping and unpacking the answer all happen in that one native call. The connection loop first writes the buffered response with one non-blocking send and hands only what is left to Asio (epoll only: an io_uring write is already submitted in the same system call as the ring's other operations, so a separate send costs more). When it starts serving, the native host raises Python's garbage-collection first-generation threshold to 10000 (`NATIVE_GC_THRESHOLD`, see [Single process](#single-process-3-cores) below). The three single-process ratios are 2.07 / 2.01 / 1.39, and the 1 ms timer p99 dropped from 8.97 to 5.31 ms; with the server on 2 fully loaded CPUs, the bare function went from 10.06 to 9.05 µs per request.

## Environment

- Machine: AMD Ryzen 7 3800X (8 cores, 16 threads), 32 GB of memory, Ubuntu 20.04.6, Linux 5.15.
- CPython 3.14.7 (regular GIL build); both sides run on uvloop 0.22.1 (the comparison driver checks every window).
- oldman-server 0.1.0, the native host on the default epoll, with io_uring measured as well. Stock Sanic 25.12.1 (the archive in `upstream/sanic.json`, checked file by file before comparing).
- `json()` uses each side's default: orjson 3.13.0 for oldman-server, ujson 6.0.0 for stock Sanic.
- Server, client and driver are each pinned to different physical cores. The machine was idle while measuring: in the paired comparison (2026-10-27) other processes used 0 of the server cores in every window, and at most 0.13 cores in the wrk runs (2026-10-25).

## Method

**Paired comparison** (`probes/sanic_comparison/run.py --python-only`):

- The client is this repository's `build/http_load` (based on Asio): persistent connections, closed loop, every response checked (status, body, process id).
- Each workload first runs 2 A/A windows, then oldman-server and stock Sanic alternate for 4 pairs; each window has 2 seconds of warm-up and 5 seconds of measurement. The ratio is the geometric mean of the 4 pairs' throughput ratios.
- Workloads:
  - Small JSON: returns `{"message":"Hello, World!"}`.
  - Echo: POST a 1 KiB request body; the response carries it back.
  - 1 ms timer: the handler returns small JSON after `await asyncio.sleep(0.001)`, standing for I/O-bound work that waits on a database or another service, without the client library's own cost (that part is the same for any framework).
- Single process: the server has 3 cores. oldman-server runs 1 process (1 Python thread + 2 shard threads), stock Sanic 3 workers. The client has 3 cores and 128 connections (512 for the 1 ms timer).
- Multiple workers: the server has 4 cores. oldman-server runs 4 workers (the default same-thread layout, one CPU per worker), stock Sanic 4 workers. The client has 3 cores and 256 connections (1024 for the 1 ms timer).
- CPU per request: the CPU time used by the server's process tree divided by the number of requests. Latency: the p50 / p99 of every request as recorded by the client; in a closed-loop measurement it is mostly queueing (connections ÷ throughput).

**wrk reference** (`probes/wrk_comparison/run.py`):

- The route is the same as [web-frameworks-benchmark](https://web-frameworks-benchmark.netlify.app/) (`/`, empty text), plus `/work`, where the handler spends 25 µs of CPU itself, and two files served by `app.static`.
- The server has 4 cores, wrk another 3. wrk does not check content; it only counts non-2xx responses (all 0).
- Each cell has 2 rounds in alternating order, each round 3 seconds of warm-up and 10 seconds of measurement, averaged.
- Services:
  - oldman-server, 4 workers (the default same-thread layout);
  - oldman-server, 2 workers × (1 Python thread + 1 shard);
  - bare function: one Python function attached directly to the native host without the framework, i.e. the native host's own ceiling, measured in both layouts;
  - stock Sanic, 4 workers.

## Paired comparison

2026-10-27, code `956ef97`.

### Single process (3 cores)

| Workload | Backend | oldman-server RPS | Stock RPS | Ratio (lowest–highest of 4 pairs) | CPU per request µs (oldman-server / stock) | p50 ms (oldman-server / stock) | p99 ms (oldman-server / stock) |
|---|---|---:|---:|---:|---:|---:|---:|
| Small JSON | epoll | 159,028 | 76,469 | 2.07 (2.06–2.08) | 16.5 / 39.2 | 0.80 / 1.51 | 0.97 / 3.44 |
| Echo | epoll | 132,466 | 65,534 | 2.01 (2.00–2.02) | 19.0 / 45.8 | 0.96 / 1.78 | 1.22 / 3.96 |
| 1 ms timer | epoll | 117,421 | 84,784 | 1.39 (1.38–1.39) | 21.3 / 35.3 | 4.34 / 5.63 | 5.31 / 8.76 |
| Small JSON | io_uring | 154,626 | 76,391 | 2.02 (2.00–2.03) | 16.1 / 39.2 | 0.80 / 1.52 | 1.13 / 3.48 |
| Echo | io_uring | 132,528 | 65,903 | 2.00 (1.99–2.02) | 18.6 / 45.5 | 0.95 / 1.83 | 1.32 / 3.89 |
| 1 ms timer | io_uring | 117,890 | 84,241 | 1.39 (1.37–1.40) | 21.1 / 35.6 | 4.32 / 5.96 | 5.26 / 9.62 |

Throughput leads stock Sanic in every pair. The 1 ms timer p99 is lower than stock Sanic's in every pair (5.2–5.4 ms across oldman-server's four runs, 8.0–9.9 ms for stock Sanic, the same on both backends). At `16fd826` this cell swung widely between pairs and on epoll was even higher than stock Sanic's (8.97 vs 8.32 ms, 7.9–11.0 ms across oldman-server's four runs). The cause was Python's garbage collection: the objects of 512 in-flight requests are still alive at every young-generation collection; with the default threshold (2000) a collection ran about every 7 ms and promoted them, so a full collection was triggered about every 0.75 seconds, each taking 7–8 ms and reclaiming nothing, while all in-flight requests waited. Now the native host raises the first-generation threshold to 10000 when it starts serving (`NATIVE_GC_THRESHOLD`, see [Configuration](configuration.md#native-host-settings)); full collections no longer occur, and the main thread's time per request also dropped by about 8%. Single-process throughput is set by the Python main thread (it is always close to one full core), roughly 1e6 ÷ the main thread's µs per request.

### Multiple workers (4 cores)

| Workload | Backend | oldman-server RPS | Stock RPS | Ratio (lowest–highest of 4 pairs) | CPU per request µs (oldman-server / stock) | p50 ms (oldman-server / stock) | p99 ms (oldman-server / stock) |
|---|---|---:|---:|---:|---:|---:|---:|
| Small JSON | epoll | 251,376 | 100,814 | 2.50 (2.49–2.52) | 15.9 / 39.7 | 1.01 / 2.39 | 1.12 / 5.38 |
| Echo | epoll | 223,347 | 86,306 | 2.59 (2.57–2.61) | 17.9 / 46.3 | 1.14 / 2.76 | 1.26 / 6.01 |
| 1 ms timer | epoll | 186,170 | 104,483 | 1.76 (1.70–1.80) | 21.4 / 38.2 | 5.44 / 10.83 | 7.44 / 12.97 |
| Small JSON | io_uring | 261,959 | 100,934 | 2.60 (2.58–2.62) | 15.2 / 39.6 | 0.97 / 2.33 | 1.10 / 5.01 |
| Echo | io_uring | 229,672 | 86,023 | 2.67 (2.66–2.69) | 17.4 / 46.4 | 1.11 / 2.67 | 1.28 / 6.45 |
| 1 ms timer | io_uring | 187,502 | 104,689 | 1.79 (1.79–1.79) | 21.3 / 38.1 | 5.40 / 10.69 | 7.05 / 12.98 |

Every pair leads stock Sanic. All 4 server cores are fully used (small JSON 15.9 µs × 251k ≈ 4 cores); the client uses 2.3–2.5 cores, below its limit. On Linux 5.15, io_uring and epoll each win some (in this round epoll was higher for single-process small JSON, io_uring for small JSON and echo with 4 workers, the rest close), so epoll remains the default.

## wrk reference

2026-10-25, code `7efd8fe` (the static-file section is from after `378f09e`). For oldman-server's later changes see [Other Python frameworks](#other-python-frameworks-wrk) below, whose `GET /` is the empty text route here.

### Empty text route (keep-alive)

| Service | 64 connections | 256 connections | 512 connections | CPU per request µs (256) | p99 ms (256) |
|---|---:|---:|---:|---:|---:|
| oldman-server, 4 workers (default) | 196,187 | 214,974 | 208,021 | 18.4 | 1.65 |
| oldman-server, 4 workers, io_uring | 198,193 | 222,801 | 218,240 | 17.8 | 1.60 |
| oldman-server, 2 × (1 + 1) | 165,531 | 167,385 | 163,939 | 22.9 | 2.08 |
| Bare function, 4 processes | 249,015 | 271,194 | 268,211 | 14.6 | 1.31 |
| Bare function, 2 × (1 + 1) | 169,365 | 167,154 | 166,115 | 19.9 | 2.00 |
| Stock Sanic, 4 workers | 121,335 | 129,263 | 129,023 | 30.7 | 3.69 |

The difference between the bare function and oldman-server (about 4 µs of CPU per request at 256 connections) is the cost of the framework layer (routing, Request, Response, a Task per request).

### Handler spends 25 µs of CPU (`/work`, 256 connections)

| Service | RPS | CPU per request µs | p99 ms |
|---|---:|---:|---:|
| oldman-server, 4 workers (default) | 89,876 | 44.1 | 3.59 |
| oldman-server, 2 × (1 + 1) | 62,233 | 50.1 | 4.52 |
| Stock Sanic, 4 workers | 69,958 | 56.7 | 7.51 |

The heavier the work itself, the smaller the share the framework saves; 2 × (1 + 1) has only two cores running Python and falls below stock Sanic here, which is why the multi-worker default is the same-thread layout.

### A new connection per request (`Connection: close`)

| Service | 32 concurrent | 256 concurrent | CPU per request µs (256) | p99 ms (256) |
|---|---:|---:|---:|---:|
| oldman-server, 4 workers (default) | 29,707 | 43,877 | 81.6 | 6.20 |
| oldman-server, 4 workers, io_uring | 31,109 | 44,577 | 75.8 | 6.20 |
| oldman-server, 2 × (1 + 1) | 22,795 | 33,386 | 73.2 | 14.59 |
| Bare function, 4 processes | 52,190 | 52,487 | 65.7 | 4.28 |
| Stock Sanic, 4 workers | 17,822 | 25,040 | 116.0 | 405.62 |

Setting up and closing a connection (mostly the kernel's TCP cost) is far more expensive than a request, so throughput is only about one fifth of keep-alive. Behind nginx with upstream keepalive, new connections rarely reach the application (see [Deployment](deployment.md#behind-nginx)).

### Static files (`app.static`)

| Service | 4 KiB, 256 connections | CPU per request µs | 256 KiB, 64 connections | CPU per request µs |
|---|---:|---:|---:|---:|
| oldman-server, 4 workers (default) | 23,512 | 169.0 | 10,057 | 397.0 |
| oldman-server, 4 workers, io_uring | 23,489 | 168.9 | 9,982 | 399.8 |
| oldman-server, 2 × (1 + 1) | 17,378 | 160.3 | 8,970 | 382.9 |
| Stock Sanic, 4 workers | 11,346 | 347.4 | 7,092 | 553.0 |

Before `378f09e`, `file()` (which `app.static` also uses) read the whole file into Python bytes and the native host copied it again to write it out: the file contents were copied in memory several times, and large blocks of memory were allocated anew for every response (page faults and allocation took about 30% of the CPU in the profile). The 256 KiB file was therefore 14% slower than stock Sanic in the default layout (6,092 vs 7,057, CPU per request 655.5 vs 555.9 µs). Now files of 128 KiB and more bypass Python: the native host's file thread reads them and the shard writes them directly to the connection (the same as `file_stream()`), at 1.42 times stock Sanic. Reading smaller files into the response is actually cheaper (compared size by size, the two approaches break even at about 100 KiB), so that stays; the 4 KiB column is the same as before the change (previously 23,686 vs 11,520).

## Other Python frameworks (wrk)

2026-10-27, oldman-server code `956ef97`. The Python applications of [web-frameworks-benchmark](https://web-frameworks-benchmark.netlify.app/) (commit `eb2f909` of the-benchmarker/web-frameworks; the site also uses CPython 3.14) run as written and configured there, on the same machine and the same server CPUs as oldman-server:

- The three routes are the same as on the site: `GET /` empty response, `GET /user/0` answering `0`, `POST /user` empty response. After each service starts, the responses of the three routes are checked first; for oldman-server, that it runs on uvloop is checked as well.
- One set with the server on 2 CPUs and one with 4 CPUs. Each service's number of workers or processes equals the number of server CPUs (`PYTHON_CPU_COUNT` is set for those that go by the CPU count); oldman-server runs same-thread workers, one per CPU.
- wrk runs 3 threads on another 3 CPUs; 64 and 256 connections, 2 rounds per cell in alternating order, each round 3 seconds of warm-up and 10 seconds of measurement. All responses 2xx, no socket errors; other programs used no more than 0.10 cores of the server CPUs.
- Versions: fastpysgi 0.6, weft-server 0.1.1, wred 0.13, oxapy 0.11.5, Robyn 0.88.0, Emmett 2.8.2 (Granian 2.8.5), Sanic 25.12.1, BlackSheep 2.6.4, Starlette 1.10.0, Litestar 2.25.0, FastAPI 0.162.0, lihil 0.2.41, asgineer 0.9.5; ASGI applications are served by uvicorn 0.54.0; those using uvloop all use 0.22.1.
- peregrine (only manylinux_2_35 wheels) and socketify (no CPython 3.14 wheels) from the site do not install on Ubuntu 20.04 and were not measured.

Sorted by CPU per request (the CPU time of the server's process tree divided by the number of requests, averaged over the three routes and both connection counts).

### Server on 2 CPUs

Every service fully uses the server's 2 CPUs (the client uses at most 2.6 cores), so CPU per request is the cost at full load, which best compares the servers and frameworks themselves.

**Without a framework** (a hand-written function that checks the path, attached directly to the server)

| Service | Server | CPU per request µs | RPS, 64 connections | RPS, 256 connections |
|---|---|---:|---:|---:|
| oldman-server bare function | native host, same-thread process | 9.1 | 220,543 | 219,136 |
| weft-wsgi | weft (Rust) | 10.1 | 197,795 | 197,653 |
| fastpysgi-wsgi | fastpysgi (C) | 10.1 | 196,730 | 195,818 |
| weft-asgi | weft (Rust) | 12.7 | 154,517 | 157,706 |

**Frameworks**

| Service | Server | CPU per request µs | RPS, 64 connections | RPS, 256 connections |
|---|---|---:|---:|---:|
| oldman-server | native host, same-thread workers | 13.7 | 143,704 | 147,356 |
| wred | weft (Rust) | 14.3 | 138,037 | 141,441 |
| oxapy | oxapy (Rust), synchronous handlers | 15.8 | 124,114 | 124,550 |
| asgineer | uvicorn | 25.0 | 78,064 | 81,212 |
| lihil | uvicorn | 26.6 | 74,038 | 76,643 |
| BlackSheep | uvicorn | 27.9 | 70,227 | 72,612 |
| Stock Sanic | Sanic | 29.4 | 67,752 | 67,657 |
| Starlette | uvicorn | 32.6 | 60,730 | 61,581 |
| Emmett | Granian (Rust) | 35.9 | 61,632 | 52,439 |
| Litestar | uvicorn | 50.9 | 39,005 | 39,411 |
| Robyn | Robyn (Rust) | 54.3 | 36,647 | 36,525 |
| FastAPI | uvicorn | 69.3 | 28,746 | 29,019 |

### Server on 4 CPUs

In cells marked * wrk's 3 cores are saturated (the client uses 2.9–3.0 cores) and the server is not fully loaded, so the RPS is only a lower bound; the server then handles fewer requests each time it wakes up, so CPU per request is also higher than at full load.

**Without a framework**

| Service | Server | CPU per request µs | RPS, 64 connections | RPS, 256 connections |
|---|---|---:|---:|---:|
| oldman-server bare function | native host, same-thread process | 10.9 | 313,580* | 314,501* |
| weft-wsgi | weft (Rust) | 11.1 | 319,003* | 320,171* |
| fastpysgi-wsgi | fastpysgi (C) | 11.8 | 315,732* | 314,585* |
| weft-asgi | weft (Rust) | 13.4 | 282,546 | 309,023* |

**Frameworks**

| Service | Server | CPU per request µs | RPS, 64 connections | RPS, 256 connections |
|---|---|---:|---:|---:|
| oxapy | oxapy (Rust), synchronous handlers | 14.5 | 222,822 | 267,294 |
| oldman-server | native host, same-thread workers | 14.5 | 262,354 | 285,686 |
| wred | weft (Rust) | 15.1 | 250,295 | 275,270 |
| asgineer | uvicorn | 26.8 | 141,445 | 155,921 |
| lihil | uvicorn | 28.6 | 132,737 | 147,416 |
| BlackSheep | uvicorn | 30.4 | 124,361 | 137,918 |
| Emmett | Granian (Rust) | 31.1 | 113,007 | 143,263 |
| Stock Sanic | Sanic | 31.2 | 123,800 | 131,300 |
| Starlette | uvicorn | 35.0 | 109,269 | 118,350 |
| Robyn | Robyn (Rust) | 48.4 | 75,229 | 89,167 |
| Litestar | uvicorn | 53.7 | 72,430 | 76,074 |
| FastAPI | uvicorn | 72.8 | 53,806 | 55,904 |

oxapy does not fully use the server's 4 CPUs (3.1–3.8 cores), so its CPU per request is lower while its RPS is below oldman-server's and wred's.

### Ranking

- Without a framework: with the server fully loaded (2 CPUs) oldman-server ranks first at 9.05 µs, against weft-wsgi 10.05 and fastpysgi 10.13, about 10% less (at `16fd826` it was 10.06 vs 10.02, a tie). The roughly 1 µs saved comes from the shard completing the bare function's call and bookkeeping itself in same-thread mode, and from the connection loop writing the response with one send first. On 4 CPUs all three are limited by wrk (about 310–320k RPS), and oldman-server's CPU per request is still the lowest.
- Frameworks: with the server fully loaded (2 CPUs) oldman-server ranks first at 13.7 µs, against wred 14.3 and oxapy 15.8. Its lead over wred, about 4%, is mostly in `GET /user/0` (13.9 vs 15.0 µs); `GET /` and `POST /user` are about 0.3 µs less. On 4 CPUs it ties with oxapy for first by CPU per request (14.54 vs 14.51 µs, oxapy not fully using the 4 CPUs), wred 15.14, and oldman-server has the highest RPS at both connection counts. oxapy's routing and request handling are in Rust, with only synchronous handlers on the Python side; wred is an ASGI micro-framework of about 120 lines (running on weft) that finds the handler by looking up the path in a dict, and the handler returns (body, headers, status) directly, with no middleware, signals, exception handling or request lifecycle; weft drives the ASGI coroutines itself and creates no asyncio Task per request. oldman-server keeps these framework features and one Task per request (contextvars, cancellation and timeouts all rely on it); at full load its CPU per request is 0.47 times stock Sanic's (29.4 µs).
- perf on oldman-server's framework route at `6fcf554` (2 CPUs fully loaded): the kernel's network send and receive take about 39%, Python about 35% (spread over the framework's fast path, creating and scheduling the per-request Task, and the request and response objects, with no single hot spot), the native host about 14%, libc about 6%. The kernel part is about 6 µs per request, roughly the same for everyone.

## With oldman's five extensions

This package used to ship a fork of sanic-ext 25.12.0 (`oldman_server.ext`), and this section compared the cost of both sides with oldman's five extensions attached (last at `956ef97`, 4 CPUs: oldman-server 21.2 µs, stock Sanic + sanic-ext 45.6 µs). The extensions have since been removed from this package and oldman provides companions such as templates and CORS itself, so this comparison is no longer maintained; the earlier tables are in `BENCHMARKS.md` of this repository at `8018234`.

## Memory allocator

With the single-process paired comparison's setup (server on 3 cores: 1 Python thread + 2 shards), oldman-server `793c489` used different allocators through `LD_PRELOAD`, 3 rounds in rotating order, taking the median. `LD_PRELOAD` also applies to the load client, so compare the server's CPU per request, not the RPS.

| Allocator | Small JSON: server µs | Main thread µs | Echo request body: server µs | Main thread µs |
|---|---:|---:|---:|---:|
| glibc 2.31 (default) | 23.1 | 9.49 | 27.3 | 11.01 |
| tcmalloc (gperftools 2.7, `libtcmalloc_minimal.so.4`) | 20.6 | 8.28 | 24.8 | 10.34 |
| jemalloc 5.2.1 | 21.6 | 8.67 | 26.0 | 10.78 |
| mimalloc 2.1.7 | 21.9 | 8.88 | 25.9 | 10.90 |

tcmalloc was the fastest in every round, with about 9–11% less server CPU per request; another set of 3 rounds the day before gave the same result (small JSON 23.0 → 21.4 µs, echo 27.8 → 25.2 µs). For how to use it see [Configuration](configuration.md#memory-allocator).

## Changes by stage

### Ratio to stock Sanic

Absolute RPS on different dates is affected by desktop load (the same code can differ by about 10%), so only ratios are compared; the ratios of two runs on the same day can also differ by about 0.05. Before 2026-10-25 the client in each row was the Seastar version of the same load tool.

| Date | State | Single process, 3 cores: small JSON / echo / 1 ms timer | 4 cores: small JSON / echo / 1 ms timer |
|---|---|---|---|
| 2026-09-30 | Starting point: stock Sanic's code attached directly to the native host (Seastar) | 0.46 / — / — | — |
| 2026-10-20 | Stage one: fast path in the Python layer | 1.19 / 1.19 / — | — |
| 2026-10-21 | Stage two: native HTTP/1.1 layer, response construction | 1.51 / 1.46 / — | — |
| 2026-10-21 | Multiple workers: 2 workers × (1 + 1) | — | 1.75 / 1.79 / 1.35 |
| 2026-10-22 | After the fixes from the pre-release review | 1.50 / 1.40 / 1.13 | 1.77 / 1.81 / 1.40 |
| 2026-10-24 | Base changed from Seastar to Asio | 1.58 / 1.38 / 1.14 | 1.80 / 1.87 / 1.47 |
| 2026-10-24 | Multiple workers default to the same-thread layout (4 workers) | 1.57 / 1.45 / 1.16 | 1.90 / 1.91 / 1.52 |
| 2026-10-25 | Full re-measurement (the wrk sections come from this round) | 1.55 / 1.42 / 1.15 | 2.10 / 2.03 / 1.59 |
| 2026-10-26 | Signals on the fast path, native host by default on Linux, fixed responses and the native cache | 1.48 / 1.39 / 1.12 | 2.00 / 2.03 / 1.57 |
| 2026-10-26 | Fewer memory allocations, bare functions called directly, a C extension type for the request object | 1.60 / 1.57 / 1.14 | 2.11 / 2.17 / 1.60 |
| 2026-10-26 | Less engine bookkeeping, shared parsing of request targets, inlined response finishing | 1.84 / 1.69 / 1.21 | 2.28 / 2.33 / 1.66 |
| 2026-10-26 | The framework coroutine ends the request itself; building and finishing a request each take one native host call | 2.05 / 2.00 / 1.25 | 2.51 / 2.59 / 1.73 |
| 2026-10-27 | Bare function calls completed natively, the response written with one send first (epoll), a higher garbage-collection first-generation threshold while serving (this page) | 2.07 / 2.01 / 1.39 | 2.50 / 2.59 / 1.76 |

The starting-point row measured only small JSON (30.5k vs 65.7k RPS). The 1 ms timer has been part of the comparison only since 2026-10-21. Before 2026-10-24 the 4-core column is always 2 workers × (1 + 1).

### Same-period comparison when the base changed (2026-10-24)

The Seastar version (tag `seastar-base`) and the Asio version ran alternately in the same period, 3 rounds each, taking the median, with the same client and layout. Main thread µs is the CPU the Python thread used per request.

Single process (1 Python thread + 2 shards):

| Workload | Metric | Seastar | Asio epoll | Asio io_uring |
|---|---|---:|---:|---:|
| Small JSON | Throughput | 110,901 | 117,779 (+6.2%) | 113,300 (+2.2%) |
| | Total CPU per request µs | 22.83 | 20.95 (−8.2%) | 21.57 (−5.5%) |
| | Main thread µs | 9.00 | 8.44 (−6.2%) | 8.70 (−3.3%) |
| Echo | Throughput | 84,431 | 90,976 (+7.8%) | 88,658 (+5.0%) |
| | Total CPU per request µs | 28.70 | 27.08 (−5.6%) | 27.38 (−4.6%) |
| | Main thread µs | 11.79 | 10.92 (−7.4%) | 11.16 (−5.3%) |
| 1 ms timer | Throughput | 91,999 | 94,040 (+2.2%) | 93,167 (+1.3%) |
| | Total CPU per request µs | 26.65 | 25.73 (−3.5%) | 25.93 (−2.7%) |
| | Main thread µs | 10.84 | 10.61 (−2.1%) | 10.72 (−1.1%) |

2 workers × (1 + 1):

| Workload | Metric | Seastar | Asio epoll | Asio io_uring |
|---|---|---:|---:|---:|
| Small JSON | Throughput | 169,122 | 175,076 (+3.5%) | 184,598 (+9.2%) |
| | Total CPU per request µs | 21.57 | **22.59 (+4.7%) ↓** | 20.10 (−6.8%) |
| | Main thread µs | 9.85 | **11.23 (+14.0%) ↓** | 9.55 (−3.0%) |
| Echo | Throughput | 148,140 | 156,192 (+5.4%) | 158,540 (+7.0%) |
| | Total CPU per request µs | 25.62 | 25.37 (−1.0%) | 23.96 (−6.5%) |
| | Main thread µs | 12.41 | **12.67 (+2.1%) ↓** | 11.73 (−5.5%) |
| 1 ms timer | Throughput | 141,664 | 153,203 (+8.1%) | 158,867 (+12.1%) |
| | Total CPU per request µs | 25.53 | 23.93 (−6.3%) | 23.46 (−8.1%) |
| | Main thread µs | 11.67 | 11.19 (−4.1%) | 11.17 (−4.3%) |

Throughput went up in every cell. The marked rises in main thread µs are not slowdowns: the main thread's µs per request varies with its load, and the more requests it handles each time it wakes up, the less it costs per request. With Asio epoll and 2 workers the shards fill up first, so the Python thread handles fewer requests per wake-up (about 6–7 vs 17–19 for io_uring), which is more per request. At the same fixed 120k requests per second, the main thread takes about 16 µs on both backends.

## Not covered

- A reverse proxy such as nginx in front: the proxy's own cost is the same for both sides; for how requests are spread over multiple workers behind nginx see [Deployment](deployment.md#behind-nginx).
- More than 4 cores: on this 8-core machine the server gets at most 4 cores (another 3 go to the client and 1 to the comparison driver), so a server with more cores cannot be measured here. At about 10 µs of client CPU per request, the client's 3 cores are estimated to saturate around 300k RPS.
- TLS, HTTP/2, HTTP/3: oldman-server serves only plain HTTP/1.1; TLS is terminated by a front proxy.
- The cost of client libraries (Redis, MySQL, HTTP clients): the same for any framework, not counted.

## Reproducing

First prepare the development environment as in [Development](development.md#development-environment) (`bash scripts/bootstrap.sh`, then `source scripts/env.sh`), and extract the archive in `upstream/sanic.json` to `build/sanic-upstream/`. Before each set it is best to wait until the machine is quiet (desktop programs take cores):

```sh
python probes/sanic_comparison/run.py --python-only --out build/gate-single                              # single process, 3 cores
python probes/sanic_comparison/run.py --python-only --server-cores 4 --workers 4 --out build/gate-workers # 4 workers
OLDMAN_SERVER_NATIVE_REACTOR_BACKEND=io_uring python probes/sanic_comparison/run.py --python-only --out build/gate-single-io_uring
python probes/wrk_comparison/run.py --out build/wrk-root --services candidate,candidate-shards,bare,bare-shards,stock,candidate-io_uring
python probes/wrk_comparison/run.py --out build/wrk-work --path /work --connections 256 --services candidate,candidate-shards,stock
python probes/wrk_comparison/run.py --out build/wrk-close --close --connections 32,256 --services candidate,candidate-shards,bare,stock,candidate-io_uring
python probes/wrk_comparison/run.py --out build/wrk-small --path /static/small.bin --connections 256 --services candidate,candidate-shards,stock,candidate-io_uring
python probes/wrk_comparison/run.py --out build/wrk-large --path /static/large.bin --connections 64 --services candidate,candidate-shards,stock,candidate-io_uring
bash probes/framework_comparison/setup.sh                                   # web-frameworks-benchmark's applications and their environments (build/framework-comparison)
python probes/framework_comparison/drive.py build/frameworks-2cpu --server-cpus 2   # all services, server on 2 CPUs
python probes/framework_comparison/drive.py build/frameworks-4cpu                   # server on 4 CPUs
```

Each window of the paired comparison writes `row.json` (throughput, latency, CPU per request, other processes' use of the server cores); each wrk window writes `rows.jsonl`.
