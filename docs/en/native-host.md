English | [简体中文](../zh/native-host.md)

# Native host

The native host serves plain HTTP/1.1 inside the current process, listening on TCP or a unix socket, including keep-alive, pipelining, chunked request bodies, streaming requests and streaming responses, backpressure, 100-continue and WebSocket. Native shards do the network IO and the HTTP parsing and writing; routing, middleware, exception handling, the lifecycle and application code still run in Python.

`app.run()`, `app.create_server()` and the test client `app.test_client` (`oldman_server.testing`) all go through the native host, and the default run's `main_process_*` listeners fire as usual. To use the asyncio server, see [Installation](installation.md#choosing-the-server).

## File responses

The stat and reads of `file()`, `file_stream()` and `app.static()` are done by the native host's file thread (on the shard's CPU; reading the disk does not block the shard), no longer through the aiofiles thread pool.

- The contents of `file_stream()`, and of `file()` for files of 128 KiB or more, are read by the file thread and written by the shard straight to the connection, without Python (for the resulting differences of `file()`, see [Differences from Sanic](differences.md#under-the-native-host)). `file_stream()` chunks as stock Sanic does (for the fix with Range, see [Behavior fixes](differences.md#behavior-fixes)).
- Smaller files are read into the response by `file()`, which is cheaper.
- Response headers, 304/206 and error types match stock Sanic.

## Multipart forms

`request.form` / `request.files` are parsed in C++ (`native/multipart.hh`), with the same results as Sanic's `parse_multipart_form`; forms that need Python decoding or Unicode handling (non-ASCII part headers, non-UTF-8 fields, `filename*`, parts without a name, malformed input) are still handled by Sanic's parser.

## Listening sockets

Sanic binds the listening socket as it always has (a unix socket gets a temporary name and permissions 0o666 by default (`UNIX_SOCKET_MODE`), is renamed into place, and is removed on exit), and the native host accepts connections on that socket (shard 0 accepts and hands them out to the shards). The socket stays open while serving and closes with the server, so a passed-in `sock` (TCP or AF_UNIX) and `app.serve_location` behave as in stock Sanic.

With multiple workers, as with stock Sanic's TCP, the main process binds and listens once, and every worker accepts on that one socket (unix sockets too; the main process removes it on exit). The main process listens from startup until exit:

- While no worker is accepting yet (at startup, or when all workers restart together), new connections wait in the queue instead of being refused, and no other program can bind the address in the meantime.
- One worker stopping (restart, scale-down) does not close the socket that the other workers still use; connections already in the queue are taken by the other workers.
- Stock Sanic binds a unix socket in each worker and renames it over the path, so only the worker that renamed last receives connections.

The workers publish their connection counts through shared memory that the main process allocates, and take turns accepting: a worker that does not have the fewest connections holds off accepting (it checks again after 50 µs, then at doubling intervals up to 1 ms, and accepts anyway once a connection has waited 1 ms without anyone taking it; a worker that exits yields at once, and one that has crashed or hung is no longer waited for after 200 ms; a restarted worker takes over the old one's place). Otherwise the worker whose shard is busy (polling instead of sleeping) would almost always get new connections first. A shared socket passed in by the user works the same way. For deployment behind nginx, see [Deployment](deployment.md).

## Plain HTTP/1.1 only

A front proxy (such as nginx) terminates TLS; this package provides neither TLS nor HTTP/3 (see [Differences from Sanic](differences.md#removed)).

## Multiple workers

Runs that start worker processes (`workers > 1`, `fast`, `auto_reload`, `dev`) are managed by Sanic's worker manager as in stock Sanic (restarts, scaling and `app.m` are all the same), and each worker process runs its own native host:

- The main process splits the CPUs into one group per worker; the worker's Python thread is pinned to the group's first CPU and the shards use the rest (groups are taken by worker number; for the numbering see [`NATIVE_WORKER_LISTEN`](configuration.md#native-host-settings)).
- When there are not enough CPUs to give each worker one more for a shard (`fast=True` with one worker per CPU, or `workers` more than half the CPU count), each worker takes only one CPU and its shard runs on its Python thread (same-thread mode, `NATIVE_SAME_THREAD`): twice as many processes run Python on the same cores, and the hand-off between threads is saved. On 4 cores, the same empty-text route serves about 215k RPS against 167k for 2 workers × (1 + 1); with the handler spending 25 µs of CPU itself, 90k against 62k (stock Sanic with 4 workers: 129k and 70k).
- When there are more workers than CPU groups, the extra workers share CPU groups with the earlier ones (a warning is logged).

With the `fork` start method, when the parent process has already run the native host (a process starts the native runtime only once, and a forked child has its state but not its threads), the child uses the asyncio server instead and logs a warning; the default `spawn` is not affected.

There is one native server per process: a second server on the same event loop in the same process (such as a second application's server) uses the asyncio server (a warning is logged), and both serve normally.

## Fast path

For an ordinary application, the native host builds the request object in one step and native code finishes the request and does its accounting, which saves a few microseconds per request over the full path (the same as Sanic's `handle_request`). The following fall back to the full path, with the same behavior, and an INFO record in `oldman_server.server` at startup says why:

- debug mode (TOUCHUP off);
- a replaced `request_class` or router;
- an overridden `handle_request` or similar method;
- a registered parameterized `http.*` signal.

## Protocol errors

Protocol errors (such as a malformed request line, oversized headers, bad chunked encoding, an oversized body) report the same status codes and messages as stock Sanic and go to the application's error handler.

## Answers without Python

Two ways, both set explicitly.

### Fixed responses

`app.add_fixed_response("/health", "ok")` (optional `status`, `content_type`, `headers`). GET and HEAD requests for this path (whatever the query string) are answered before anything else: no path rewriting, no routing, no middleware, no signals, no Request object, no access log. Requests with a body and other methods are routed as usual, and a route on the same path never receives these two methods. The native host writes the answer directly without entering Python; the asyncio server answers by the same rules (except for connection-level signals fired before the request line is read). Set it before the server starts.

### Native micro-cache

`@native_cache(seconds)` (`from oldman_server.native import native_cache`, on the handler, below the route decorator). When this handler's complete 200 response to a GET has no Set-Cookie or Vary, and its Cache-Control allows a shared cache to store it (no private, no-store, no-cache), the native host keeps it for `seconds` seconds, and meanwhile answers GETs for the same request target (path plus query string) itself: Python is not entered, so middleware (authentication included), signals and the handler do not run, and the request headers are not looked at. Requests with a Cookie or Authorization header are neither answered from the cache nor have their responses stored. Suitable for public content only. Each shard (each worker process in same-thread mode) has its own cache, at most 1024 entries of 1 MiB each. The asyncio server ignores it.
