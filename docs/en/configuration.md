English | [简体中文](../zh/configuration.md)

# Configuration

Settings are given the Sanic way: `app.config.X = ...`, or the environment variable `OLDMAN_SERVER_X`.

## Sanic settings

Sanic's settings apply as usual; the native host carries them out as follows:

| Setting | Native host behavior |
|---|---|
| `REQUEST_MAX_SIZE`, `REQUEST_MAX_HEADER_SIZE`, `REQUEST_BUFFER_SIZE` | The request body and header limits, computed as in Sanic (the body limit is checked when the handler reads the body; a `stream=True` handler is not bound by it). A body still arriving is flow-controlled by `REQUEST_BUFFER_SIZE`: the part handed to Python but not yet read by the handler never exceeds it, so a handler that reads slowly makes the connection receive slowly |
| `REQUEST_TIMEOUT`, `KEEP_ALIVE`, `KEEP_ALIVE_TIMEOUT` | Enforced by the native layer and timed by Sanic's phases: `KEEP_ALIVE_TIMEOUT` between two requests, `REQUEST_TIMEOUT` while the request header arrives (on expiry the error handler answers 408), `RESPONSE_TIMEOUT` while the handler runs (including while the body is still arriving). As in stock Sanic, values under 0.1 s (including 0) count as 0.1 s, and infinity means no limit; values over 24 h count as 24 h (the write deadline too) |
| `RESPONSE_TIMEOUT` | On expiry the handler is cancelled and the error handler answers 503 "Response Timeout"; while the body is still arriving, the timer restarts whenever data is received (as in stock Sanic). It runs until the response starts: a streaming response is not timed once it has started (both servers). Values over 24 h count as 24 h |
| `GRACEFUL_SHUTDOWN_TIMEOUT` | On stop, the server stops accepting connections and closes idle ones, waits this long for in-flight requests to finish and their responses to be written, then cancels and cuts off the rest (see [Differences from Sanic](differences.md#under-the-native-host)) |
| `KEEP_ALIVE_TIMEOUT` + `GRACEFUL_TCP_CLOSE_TIMEOUT` | The write deadline: a client that does not take the response for this long (per write; large responses per 1 MiB) is disconnected, and the handler is cancelled as when the client leaves; the same on the asyncio server |
| `ACCESS_LOG`, `WEBSOCKET_*`, error handlers, `FALLBACK_ERROR_FORMAT` | As in stock Sanic |

## Native host settings

Each has a default in `DEFAULT_CONFIG`, so `app.config.NATIVE_*` can always be read; they can also be set with `OLDMAN_SERVER_NATIVE_*` environment variables.

| Setting | Meaning |
|---|---|
| `NATIVE_SERVER` | Whether the native host serves: not set (default) means on Linux when the native extension is present; `True` means always on Linux (startup fails without the extension); `False` means never (see [Installation](installation.md#choosing-the-server)) |
| `NATIVE_SHARDS` / `NATIVE_SHARD_CPUS` | The number of shard threads (by default at most 2, taking the last CPUs of the process's CPU set) or the CPUs to use (single process only). With multiple workers, setting `NATIVE_SHARDS` keeps the shard threads, and same-thread mode is not used automatically |
| `NATIVE_SAME_THREAD` | Same-thread mode: no separate shard thread; the process's only shard runs on the Python thread, one CPU per process, which saves handing requests and responses between two threads. Not set by default: enabled automatically when the worker processes do not have enough CPUs for each to get one more for a shard (`fast=True` means one worker per CPU), unless `NATIVE_SHARDS` is set; not used in a single process by default. `True` / `False` means always / never |
| `NATIVE_WORKER_CPUS` | Each worker's CPU group with multiple workers, such as `"0,1,2;3,4,5"` (the first CPU of each group goes to Python, the rest to shards), or one CPU per group (`"0;1;2;3"`, same-thread mode). By default the main process splits the CPUs in order into groups of 1 + `NATIVE_SHARDS`, one CPU per group in same-thread mode |
| `NATIVE_GC_THRESHOLD` | When the native host starts serving, it sets Python's first garbage-collection threshold (the youngest generation) to this value (default 10000), only while the process still has Python's own default (700 in 3.12, 2000 from 3.13). `None` or `0` (write `0` in the environment variable) leaves it alone. With many in-flight requests, the default threshold collects the youngest generation every few milliseconds and promotes the in-flight requests' objects, which soon triggers full collections that scan every object in the program (several milliseconds each; all in-flight requests wait, and nothing is collected). With the higher threshold, the main thread spends about 8% less per request in the single-process sleep workload, and p99 is lower too. The cost is that garbage in reference cycles is collected later (at most about ten thousand more objects held) |
| `NATIVE_MAX_REQUESTS` | The limit on requests being processed at once (default 4096; beyond it the native layer answers 503 directly, without the error handler: Python is already at full load then) |
| `NATIVE_MAX_RESPONSE_SIZE` | The limit on a single complete response or stream chunk (default 128 MiB; a `file()` written from the file straight to the connection is not bound by it). A complete response, or the start of a streamed one, over it goes to the error handler (500); a chunk over it once a stream has started closes the connection after the data already sent |
| `NATIVE_MAX_MEMORY` | The memory that request bodies arriving together with the headers of in-flight requests may occupy (what arrives later is flow-controlled by `REQUEST_BUFFER_SIZE`) |
| `NATIVE_REACTOR_BACKEND` | The shards' I/O: `epoll` (default; an empty string is the same) or `io_uring`. io_uring uses a second runtime in the wheel, built for io_uring (`liboldman_native_uring.so`, loaded only when used); when the system does not allow io_uring (the default seccomp profile of containers, `kernel.io_uring_disabled`, Linux before 5.10, a kernel that cannot allocate memory for the ring), epoll is used instead and a warning is logged. Both work in same-thread mode. Other values fail at startup (the former `linux-aio` has been removed) |
| `NATIVE_WORKER_LISTEN` | An extra address each serving process listens on, with `{n}` replaced by the worker number (0 for a single process; n workers are numbered 0 to n−1, and a new worker after scaling down and up again takes the freed number, so each address and CPU group stays the same): a unix socket path (`"/run/app/{n}.sock"`, optionally with a `unix:` prefix, at most 63 bytes) or `host:port` (`"127.0.0.1:81{n:02d}"`, i.e. 8100, 8101, …). It lets a front proxy balance requests across the workers (see [Deployment](deployment.md)); the run's own address keeps serving. Not set by default |

## Automatic HEAD and OPTIONS

| Setting | Default | Meaning |
|---|---|---|
| `HTTP_AUTO_HEAD` | `True` | When a route group has GET but no HEAD, a HEAD request is answered by that GET route (the same handler and middleware), without a body |
| `HTTP_AUTO_OPTIONS` | `True` | When a path has routes but no OPTIONS, an OPTIONS request gets 204 with `Allow` (the path's methods, sorted), passing only through the application-level middleware |

Neither registers routes of its own: routes registered for HEAD or OPTIONS take precedence, WebSocket routes have neither, and a CORS preflight from an allowed origin is answered before routing (see the next section). A 405's `Allow` lists the automatic HEAD and OPTIONS too. Set to `False`, only the registered methods are answered and the rest get 405.

## Worker health

| Setting | Default | Meaning |
|---|---|---|
| `WORKER_HEALTH_TIMEOUT` | Unset (not watched) | Seconds, at least 1: when a worker's event loop gives no heartbeat for this long, the worker manager restarts it; unset or `0` watches none |

Every worker writes the monotonic clock to shared memory the main process allocates, from its own event loop, every `WORKER_HEALTH_TIMEOUT` ÷ 4 seconds (at least 0.5 s). Code that does not yield (blocking I/O, an endless loop, a long computation) blocks the event loop and stops the heartbeat. When the worker manager finds an acknowledged worker silent past the timeout, it logs one WARNING in `oldman_server.root` (the worker's name and how long it has been silent) and has it write the stacks of its threads to its own stderr (faulthandler, signal SIGUSR2), kills it with SIGKILL about 0.2 s later, and starts its replacement through the same path as `app.m.restart()`. This is not a graceful stop: its event loop has been blocked longer than the timeout, every request on it is stalled, and a worker with shard threads of its own would keep accepting connections it never answers, so it is ended at once. Workers added by scaling and restarted workers are watched once they acknowledge their start; workers starting or restarting are not checked; nothing is restarted once the service has begun to stop. Only the server workers the worker manager runs are watched: a single-process run has no worker manager and is not watched, and processes started with `app.manager.manage()` are not included. Native host and asyncio server workers alike.

## CORS

Cross-origin resource sharing is collected at startup: the settings below are the application's default policy, and a route, static route, fixed response, blueprint, blueprint registration or blueprint group can set its own with `cors=` (`False` for none; `CORS(...)` (`from oldman_server import CORS`) takes the fields it leaves out from these settings). Once the route table is final, every policy is compiled into one table for the server, which appends the headers to every response it writes, according to the request's `Origin`: responses of handlers and error handlers, fixed responses, native cache hits and the overload 503 alike, without middleware and without running Python per request.

| Setting | Default | Meaning |
|---|---|---|
| `CORS_ORIGINS` | Not set (off) | `"*"`, or the allowed origins (a list, or a comma-separated string such as `"https://a.example, https://*.b.example"`; `https://*.b.example` matches any subdomain of b.example, not b.example itself) |
| `CORS_ALLOW_CREDENTIALS` | `False` | Sends `Access-Control-Allow-Credentials: true`. No CORS header then carries `*`: origins, `CORS_EXPOSE_HEADERS` and `CORS_ALLOW_METHODS` must name names (`*` fails at startup), and a `*` in `CORS_ALLOW_HEADERS` is echoed instead |
| `CORS_ALLOW_METHODS` | `GET, HEAD, POST, PUT, PATCH, DELETE` | A preflight answer's `Access-Control-Allow-Methods` |
| `CORS_ALLOW_HEADERS` | Not set | A preflight answer's `Access-Control-Allow-Headers`; not set, the request's `Access-Control-Request-Headers` are echoed |
| `CORS_EXPOSE_HEADERS` | Not set | `Access-Control-Expose-Headers` |
| `CORS_MAX_AGE` | `600` | A preflight answer's `Access-Control-Max-Age` in seconds; `0` sends none |

- An allowed origin gets `Access-Control-Allow-Origin` echoed back, any other gets none; a policy that echoes origins adds `Vary: Origin` to every response (also without an `Origin` in the request, so that shared caches keep one answer per origin). `"*"` sends `Access-Control-Allow-Origin: *` on every response, without `Vary`.
- A preflight (`OPTIONS` with `Origin` and `Access-Control-Request-Method`) from an allowed origin is answered with 204 before routing, without middleware or handler. With the default policy only, that is so for any path; with route policies, the route that `Access-Control-Request-Method` would reach decides. From an origin that is not allowed, or for a route without CORS, it is an ordinary `OPTIONS` request.
- A route that writes its own CORS headers sets `cors=False`, and the server adds none.
- Invalid settings (credentials with `"*"`, a malformed origin) raise `ValueError` when the application starts; WebSocket routes take no `cors=`.

```python
from oldman_server import CORS, OldmanServer
from oldman_server.response import json

app = OldmanServer("api")
app.config.CORS_ORIGINS = "https://app.example"


@app.get("/reports", cors=CORS(credentials=True))
async def reports(request):
    return json([])
```

## Memory allocator

Most memory allocation in Python and the native host goes through glibc's malloc. With tcmalloc instead, on the machine used for the benchmarks, the server's CPU per request is about 7–11% lower (small JSON, single process, 23.1 → 20.6 µs; jemalloc and mimalloc fall between the two; see [Benchmarks](benchmarks.md#memory-allocator)). Load it with `LD_PRELOAD`; worker processes inherit it:

```sh
sudo apt install libtcmalloc-minimal4   # Debian / Ubuntu
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libtcmalloc_minimal.so.4 python app.py
```

The application can also set `PRELOAD` (library paths, separated by spaces or colons; Linux only): when `app.run()` (or `app.prepare()`) finds them missing from `LD_PRELOAD`, it re-executes the program once with them added (same interpreter, options and arguments; earlier output and side effects happen again), and worker processes inherit them. A path that does not exist fails at startup. An interactive session or a program read from standard input cannot be re-executed and fails too; in that case set `LD_PRELOAD` in the environment.
