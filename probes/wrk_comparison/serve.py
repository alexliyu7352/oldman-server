"""One service of the comparison, run under taskset by run.py:
  serve.py stock|candidate PORT WORKERS    the framework app (app.run)
  serve.py bare PORT WORKERS SAME_THREAD   a bare function on the native host
Candidate settings come from OLDMAN_SERVER_NATIVE_* (NATIVE_SERVER is set here)."""
import asyncio
import ctypes
import multiprocessing
import os
import signal
import socket
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def bare_handler(request):
    # As the site's bare WSGI entries: a hand-written check of the path.
    if request.method == "GET" and request.target == b"/":
        return 200, b"", ()
    if request.target == b"/loop":
        return 200, type(asyncio.get_running_loop()).__module__.encode(), ()
    return 404, b"", ()


async def bare_worker(index, group, same_thread, listen_fd, table):
    from oldman_server.native.accept import SLOTS
    from oldman_server.native.engine import Config, Engine, ServerConfig
    server = ServerConfig(
        listen_fd=listen_fd, listen_shared=True, accept_counts=ctypes.addressof(table),
        accept_slots=SLOTS, accept_slot=index, application_dispatch_all=True, framework_errors=True,
        max_requests=4096, max_bytes=1 << 30, max_body_bytes=1 << 20, max_response_bytes=1 << 20,
        decision_timeout_ms=60000, generate_date_header=False, coalesce_notifications=True,
        probe_headers="none", keep_alive_timeout_ms=120000, request_timeout_ms=60000)
    config = Config(shard_cpus=tuple(group[:1] if same_thread else group[1:]), server=server,
                    same_thread=same_thread,
                    reactor_backend=os.environ.get("OLDMAN_SERVER_NATIVE_REACTOR_BACKEND", "epoll"))
    # The shards' CPUs stay this thread's until the runtime has started them.
    engine = await Engine.open(config, application_handler=bare_handler,
                               opened=lambda _: os.sched_setaffinity(0, {group[0]}))
    stop = asyncio.Event()
    for number in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(number, stop.set)
    await stop.wait()
    await engine.aclose()


def bare(port, workers, same_thread):
    import uvloop
    from oldman_server.native.accept import counts
    cpus = sorted(os.sched_getaffinity(0))
    size = 1 if same_thread else len(cpus) // workers
    groups = [cpus[i * size:(i + 1) * size] for i in range(workers)]
    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", port))
    listener.listen(4096)
    context = multiprocessing.get_context("fork")
    table = counts(context)
    signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT, signal.SIGTERM, signal.SIGCHLD})
    children = [context.Process(target=lambda i=i: (
        signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT, signal.SIGTERM, signal.SIGCHLD}),
        uvloop.run(bare_worker(i, groups[i], same_thread, listener.fileno(), table))))
        for i in range(workers)]
    for child in children:
        child.start()
    try:
        signal.sigwait({signal.SIGINT, signal.SIGTERM, signal.SIGCHLD})  # a failed child ends it too
    finally:
        for child in children:
            child.terminate()
        for child in children:
            child.join(10)


# At module level: the spawned worker processes import this module (with these arguments).
if sys.argv[1:2] in (["candidate"], ["stock"]):
    from probes.wrk_comparison.apps import make_app
    app = make_app(stock=sys.argv[1] == "stock")
    if sys.argv[1] == "candidate":
        app.config.NATIVE_SERVER = True


def main():
    mode, port, workers = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
    if mode == "bare":
        return bare(port, workers, sys.argv[4] == "1")
    app.run(host="127.0.0.1", port=port, workers=workers, access_log=False)


if __name__ == "__main__":
    main()
