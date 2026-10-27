"""Sanic's standard entry points serve through the native host (NATIVE_SERVER).

Each scenario runs in its own interpreter: a process starts one native runtime.
"""
from collections import Counter
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import textwrap
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]

APP = textwrap.dedent('''
    import asyncio, json, os, sys
    from oldman_server import OldmanServer
    from oldman_server.response import json as json_response, text

    app = OldmanServer("native_run", configure_logging=False)
    app.config.ACCESS_LOG = False
    events = []

    @app.get("/json")
    async def handler(request):
        loop = type(asyncio.get_running_loop())
        return json_response({"ok": True, "ip": request.ip, "host": request.host,
                              "loop": f"{loop.__module__}.{loop.__name__}"})

    @app.post("/echo")
    async def echo(request):
        return text(request.body.decode())

    def record(name):
        if name.startswith("main_process"):
            # One argument, as Sanic's documentation writes these listeners.
            def listener(app):
                events.append(name)
        else:
            def listener(app, *_):
                events.append(name)
        return listener

    for name in ("main_process_start", "main_process_ready", "before_server_start",
                 "after_server_start", "before_server_stop", "after_server_stop",
                 "main_process_stop"):
        getattr(app, name)(record(name))

    main_loops = []

    @app.main_process_start
    def main_loop(app, loop):
        main_loops.append([type(app).__name__, f"{type(loop).__module__}.{type(loop).__name__}"])

    async def exchange(port, raw):
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(raw)
        head = await reader.readuntil(b"\\r\\n\\r\\n")
        length = int(head.lower().split(b"content-length: ")[1].split(b"\\r\\n")[0])
        body = await reader.readexactly(length)
        writer.close()
        return head.split(b" ")[1].decode(), body.decode()

    def drive(port):
        @app.after_server_start
        async def requests(app):
            results = [
                await exchange(port, b"GET /json HTTP/1.1\\r\\nHost: example.test\\r\\n\\r\\n"),
                await exchange(port, b"POST /echo HTTP/1.1\\r\\nHost: x\\r\\nContent-Length: 2\\r\\n\\r\\nhi"),
            ]
            print("RESULTS " + json.dumps(results), flush=True)
            app.stop()
''')


def run(script, env=None):
    result = subprocess.run([sys.executable, "-c", APP + textwrap.dedent(script)],
                            capture_output=True, text=True, timeout=60, cwd=ROOT,
                            env={**os.environ, **(env or {})})
    assert result.returncode == 0, result.stderr[-3000:]
    lines = {line.split(" ", 1)[0]: line.split(" ", 1)[1]
             for line in result.stdout.splitlines() if " " in line}
    return lines, result


def free_port():
    import socket
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def port_free(port):
    import socket
    with socket.socket() as sock:
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            return False
        return True


def test_run_single_process_serves_natively_with_listener_order():
    port = free_port()
    lines, _ = run(f'''
        app.config.NATIVE_SERVER = True
        app.config.NATIVE_SHARDS = 1
        drive({port})
        app.run(host="127.0.0.1", port={port}, single_process=True)
        print("EVENTS " + json.dumps(events))
        from oldman_server.native import host as seastar_sanic, engine as probe_engine  # the native modules were used
    ''')
    results = json.loads(lines["RESULTS"])
    status, body = results[0]
    # Handlers run on uvloop, as stock Sanic does by default.
    assert status == "200" and json.loads(body) == {"ok": True, "ip": "127.0.0.1", "host": "example.test",
                                                     "loop": "uvloop.Loop"}
    assert results[1] == ["200", "hi"]
    assert json.loads(lines["EVENTS"]) == ["before_server_start", "after_server_start",
                                           "before_server_stop", "after_server_stop"]


def test_environment_switch_with_default_run():
    port = free_port()
    lines, _ = run(f'''
        drive({port})
        app.run(host="127.0.0.1", port={port})
        print("NATIVE " + json.dumps([app.config.NATIVE_SERVER, "oldman_server.native.host" in sys.modules]))
        print("EVENTS " + json.dumps(events))
        print("MAINLOOPS " + json.dumps(main_loops))
    ''', env={"OLDMAN_SERVER_NATIVE_SERVER": "true", "OLDMAN_SERVER_NATIVE_SHARDS": "1"})
    assert json.loads(lines["NATIVE"]) == [True, True]
    # Main process listeners get the application and the main process's loop,
    # a uvloop loop as in Sanic.serve.
    assert json.loads(lines["MAINLOOPS"]) == [["OldmanServer", "uvloop.Loop"]]
    assert json.loads(lines["RESULTS"])[1] == ["200", "hi"]
    # The default run would use the worker manager; serving it from this process
    # still runs the main process listeners (explicit single_process does not).
    assert json.loads(lines["EVENTS"]) == [
        "main_process_start", "main_process_ready", "before_server_start", "after_server_start",
        "before_server_stop", "after_server_stop", "main_process_stop"]


def test_native_server_leaves_worker_processes_to_the_worker_manager():
    lines, result = run('''
        app.config.NATIVE_SERVER = True
        print("SINGLE " + json.dumps([app.native_single_process(), app.native_single_process(workers=2),
                                      app.native_single_process(dev=True)]))
        app.prepare(host="127.0.0.1", port=0, workers=2)
    ''')
    # One worker: served natively from this process; with worker processes
    # Sanic's worker manager runs them, each serving natively (below).
    assert json.loads(lines["SINGLE"]) == [True, False, False]
    assert "asyncio server" not in result.stdout + result.stderr


# Worker processes are spawned: they import the app from a file.
WORKERS_APP = textwrap.dedent('''
    import asyncio, json, os, sys
    from oldman_server import OldmanServer
    from oldman_server.response import json as json_response

    app = OldmanServer("native_workers", configure_logging=False)
    app.config.ACCESS_LOG = False
    app.config.NATIVE_SERVER = True
    UNIX = os.environ.get("UNIX")
    MARK = os.environ.get("RESTART_MARK")
    HOLD = int(os.environ.get("HOLD", 0))
    BURST = int(os.environ.get("BURST", 0))
    WORKER_LISTEN = os.environ.get("WORKER_LISTEN")
    if WORKER_LISTEN:
        app.config.NATIVE_WORKER_LISTEN = WORKER_LISTEN

    @app.get("/who")
    async def who(request):
        from oldman_server.native import host
        loop = type(asyncio.get_running_loop())
        return json_response({
            "worker": os.environ["OLDMAN_SERVER_WORKER_NAME"], "pid": os.getpid(),
            "python": sorted(os.sched_getaffinity(0)),
            "shards": [shard["affinity"] for shard in host._engine.shards()],
            "loop": f"{loop.__module__}.{loop.__name__}", "location": app.serve_location,
            "server": request.conn_info.server})

    async def connect(address=None):
        address = address or (("unix", UNIX) if UNIX else ("tcp", "127.0.0.1", int(os.environ["PORT"])))
        if address[0] == "unix":
            return await asyncio.open_unix_connection(address[1])
        return await asyncio.open_connection(address[1], address[2])

    async def ask(address=None):
        return await exchange(*await connect(address))

    async def exchange(reader, writer):
        writer.write(b"GET /who HTTP/1.1\\r\\nHost: x\\r\\n\\r\\n")
        head = await reader.readuntil(b"\\r\\n\\r\\n")
        length = int(head.lower().split(b"content-length: ")[1].split(b"\\r\\n")[0])
        return writer, json.loads(await reader.readexactly(length))

    async def get():
        writer, body = await ask()
        writer.close()
        return body

    async def held():
        # HOLD connections opened one after another, each kept open.
        writers, results = [], []
        for _ in range(HOLD):
            writer, body = await ask()
            writers.append(writer)
            results.append(body)
        for writer in writers:
            writer.close()
        return results

    async def burst():
        # BURST connections opened at once and kept open, then each asked.
        streams = await asyncio.gather(*(connect() for _ in range(BURST)))
        answers = await asyncio.gather(*(exchange(*stream) for stream in streams))
        for writer, _ in answers:
            writer.close()
        return [body for _, body in answers]

    async def own():
        # Each worker's own address (NATIVE_WORKER_LISTEN) reaches that worker;
        # the shared address still serves.
        from oldman_server.native.listen import address
        results = [dict(body, asked=None) for body in await asyncio.gather(*(get() for _ in range(4)))]
        for number in (0, 1):
            for _ in range(4):
                writer, body = await ask(address(WORKER_LISTEN, number))
                writer.close()
                results.append(dict(body, asked=number))
        return results

    async def drive():
        # Once every worker serves (each leaves a file in the working directory).
        while len([n for n in os.listdir() if n.startswith("ready-")]) < 2:
            await asyncio.sleep(0.01)
        results = await (own() if WORKER_LISTEN else held() if HOLD else burst() if BURST
                         else asyncio.gather(*(get() for _ in range(64))))
        print("RESULTS " + json.dumps(results), flush=True)
        if MARK and not os.path.exists(MARK):
            open(MARK, "w").close()
            # The restarted workers' drive waits for both of them, not for
            # the files this generation left.
            for name in os.listdir():
                if name.startswith("ready-"):
                    os.remove(name)
            app.m.restart(all_workers=True)
        else:
            app.m.terminate()

    @app.after_server_start
    async def start(app):
        from oldman_server.native import host
        # The socket each worker accepts on.
        listener = os.fstat(host._engine.config.server.listen_fd).st_ino
        print("LISTENER " + json.dumps([os.environ["OLDMAN_SERVER_WORKER_NAME"], listener]), flush=True)
        open(f"ready-{os.getpid()}", "w").close()
        # One worker drives, once its startup is complete. Named, Sanic keeps
        # the task: the loop holds tasks weakly, and an unreferenced one waiting
        # on its own server's sockets can be collected (closing them).
        if os.environ["OLDMAN_SERVER_WORKER_NAME"].startswith("Oldman-Server-0-"):
            app.add_task(drive(), name="drive")

    if __name__ == "__main__":
        if UNIX:
            app.run(unix=UNIX, workers=2)
        else:
            app.run(host="127.0.0.1", port=int(os.environ["PORT"]), workers=2)
''')


def run_workers(directory, env):
    script = directory / "workers_app.py"
    script.write_text(WORKERS_APP)
    # Its own session: on a timeout the worker processes go too.
    process = subprocess.Popen([sys.executable, str(script)], stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True, cwd=directory,
                               env={**os.environ, **env}, start_new_session=True)
    try:
        stdout, stderr = process.communicate(timeout=60)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.communicate()
        raise
    assert process.returncode == 0, stderr[-3000:]
    assert "timed out" not in stderr
    results, listeners = [], {}
    for line in stdout.splitlines():
        if line.startswith("RESULTS "):
            results.append(json.loads(line.split(" ", 1)[1]))
        elif line.startswith("LISTENER "):
            worker, listener = json.loads(line.split(" ", 1)[1])
            listeners.setdefault(worker, set()).add(listener)
    return results, listeners


# The worker layouts: as planned for two workers (shard threads where the CPUs
# leave room, see oldman_server.native.cpus), and the same-thread mode's.
LAYOUTS = pytest.mark.parametrize("same_thread", [False, True], ids=["planned", "same-thread"])


def layout_env(same_thread):
    return {"OLDMAN_SERVER_NATIVE_SAME_THREAD": "true"} if same_thread else {}


def expected_groups(same_thread=False):
    from oldman_server.config import Config
    from oldman_server.native.cpus import worker_groups
    config = Config()
    if same_thread:
        config.NATIVE_SAME_THREAD = True
    return worker_groups(config, 2)


def check_worker_cpus(results, same_thread=False):
    # Python on its group's first CPU, a shard on each other one (none in
    # the same-thread mode, whose shard runs on the Python thread).
    groups = expected_groups(same_thread)
    for result in results:
        group = groups[int(result["worker"].split("-")[2]) % len(groups)]
        assert result["python"] == [group[0]]
        assert result["shards"] == [[cpu] for cpu in group[1:]]
        assert result["loop"] == "uvloop.Loop"


@LAYOUTS
def test_workers_serve_natively_on_their_own_cpus(short_dir, same_thread):
    # The workers accept on the main process's socket, as in Sanic, and each
    # serves through its own native host, Python and shards on the CPU group
    # the main process planned for it.
    port = free_port()
    [results], listeners = run_workers(short_dir, {"PORT": str(port), **layout_env(same_thread)})
    assert {result["worker"] for result in results} == {"Oldman-Server-0-0", "Oldman-Server-1-0"}
    check_worker_cpus(results, same_thread)
    assert {result["location"] for result in results} == {f"http://127.0.0.1:{port}"}
    assert {result["server"] for result in results} == {f"127.0.0.1:{port}"}
    assert listeners.keys() == {"Oldman-Server-0-0", "Oldman-Server-1-0"}
    assert len(set.union(*listeners.values())) == 1
    # Taking turns (oldman_server.native.accept), both get part of a burst.
    assert min(Counter(result["worker"] for result in results).values()) >= 16


@LAYOUTS
def test_restarted_workers_serve_natively_again(short_dir, same_thread):
    port = free_port()
    (first, second), _ = run_workers(short_dir, {"PORT": str(port), "RESTART_MARK": str(short_dir / "mark"),
                                                 **layout_env(same_thread)})
    for round_, results in enumerate((first, second)):
        workers = {result["worker"] for result in results}
        assert workers == {"Oldman-Server-0-0", "Oldman-Server-1-0"}, f"round {round_}"
        check_worker_cpus(results, same_thread)
    # New processes, on the same CPU groups.
    assert not {result["pid"] for result in first} & {result["pid"] for result in second}


@LAYOUTS
def test_workers_share_a_unix_socket(short_dir, same_thread):
    # Unix sockets have no SO_REUSEPORT: the workers accept on the main
    # process's socket (instead of each binding its own over the path, so that
    # the last one would get every connection), which it removes at exit.
    path = short_dir / "app.sock"
    [results], listeners = run_workers(short_dir, {"UNIX": str(path), **layout_env(same_thread)})
    check_worker_cpus(results, same_thread)
    assert {result["server"] for result in results} == {str(path)}
    assert listeners.keys() == {"Oldman-Server-0-0", "Oldman-Server-1-0"}
    assert len(set.union(*listeners.values())) == 1
    assert not path.exists()
    # The workers take turns accepting (oldman_server.native.accept); a
    # burst of 64 used to split anywhere from 64:0 to 32:32.
    assert min(Counter(result["worker"] for result in results).values()) >= 16


@LAYOUTS
@pytest.mark.parametrize("family", ["unix", "tcp"])
def test_workers_sharing_an_address_take_turns(short_dir, family, same_thread):
    # Connections one after another, kept open: a worker accepts while no
    # other has fewer open connections. Without turns, the worker whose shard
    # is busy polling (not sleeping) takes nearly every connection.
    address = {"UNIX": str(short_dir / "app.sock")} if family == "unix" else {"PORT": str(free_port())}
    [results], _ = run_workers(short_dir, {**address, "HOLD": "16",
                                           "OLDMAN_SERVER_NATIVE_SHARDS": "1", **layout_env(same_thread)})
    split = Counter(result["worker"] for result in results)
    assert split.keys() == {"Oldman-Server-0-0", "Oldman-Server-1-0"}
    assert max(split.values()) - min(split.values()) <= 2


@LAYOUTS
@pytest.mark.parametrize("family", ["unix", "tcp"])
def test_workers_sharing_an_address_split_a_burst(short_dir, family, same_thread):
    # Hundreds of connections at once: a worker counts each connection as it
    # accepts it, so the other worker's turn comes during the burst rather
    # than after the first has taken all it could.
    address = {"UNIX": str(short_dir / "app.sock")} if family == "unix" else {"PORT": str(free_port())}
    [results], _ = run_workers(short_dir, {**address, "BURST": "512",
                                           "OLDMAN_SERVER_NATIVE_SHARDS": "1", **layout_env(same_thread)})
    split = Counter(result["worker"] for result in results)
    assert split.keys() == {"Oldman-Server-0-0", "Oldman-Server-1-0"}
    assert max(split.values()) - min(split.values()) <= 16, split


# A worker replaced while its predecessor still closes connections.
RESTART_APP = textwrap.dedent('''
    import asyncio, os
    from oldman_server import OldmanServer
    from oldman_server.response import json as json_response

    app = OldmanServer("native_restart", configure_logging=False)
    app.config.ACCESS_LOG = False
    app.config.NATIVE_SERVER = True
    app.config.NATIVE_SHARDS = 1

    @app.get("/who")
    async def who(request):
        return json_response({"worker": os.environ["OLDMAN_SERVER_WORKER_NAME"], "pid": os.getpid()})

    @app.get("/slow")
    async def slow(request):
        # Still streaming when its worker is replaced.
        response = await request.respond(content_type="text/plain")
        await response.send("started\\n")
        await asyncio.sleep(2)
        await response.send("done\\n")
        await response.eof()

    @app.get("/restart")
    async def restart(request):
        app.m.restart(all_workers=True)
        return json_response({})

    @app.main_process_ready
    async def bound(app):
        open("bound", "w").close()

    @app.before_server_start
    async def starting(app):
        # Workers that are slow to start: a while with none accepting.
        await asyncio.sleep(float(os.environ.get("START_DELAY", 0)))

    @app.after_server_start
    async def ready(app):
        open(f"ready-{os.getpid()}", "w").close()

    @app.after_server_stop
    async def stopped(app):
        open(f"stopped-{os.getpid()}", "w").close()

    if __name__ == "__main__":
        if "PORT" in os.environ:
            app.run(host="127.0.0.1", port=int(os.environ["PORT"]), workers=2)
        else:
            app.run(unix=os.environ["UNIX"], workers=2)
''')


def test_a_replaced_worker_does_not_disturb_its_successors_turns(short_dir):
    import socket
    import time
    path = str(short_dir / "app.sock")
    script = short_dir / "restart_app.py"
    script.write_text(RESTART_APP)
    process = subprocess.Popen([sys.executable, str(script)], stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True, cwd=short_dir,
                               env={**os.environ, "UNIX": path}, start_new_session=True)

    def wait_ready(count):
        deadline = time.monotonic() + 30
        while len(list(short_dir.glob("ready-*"))) < count:
            assert process.poll() is None and time.monotonic() < deadline
            time.sleep(0.02)

    def ask(target):
        client = socket.socket(socket.AF_UNIX)
        client.settimeout(10)
        client.connect(path)
        client.sendall(f"GET {target} HTTP/1.1\r\nHost: x\r\n\r\n".encode())
        received = b""
        while b"\r\n\r\n" not in received:
            received += client.recv(4096)
        head, _, body = received.partition(b"\r\n\r\n")
        length = int(head.lower().split(b"content-length: ")[1].split(b"\r\n")[0])
        while len(body) < length:
            body += client.recv(4096)
        return client, json.loads(body)

    try:
        wait_ready(2)
        slow = socket.socket(socket.AF_UNIX)
        slow.settimeout(10)
        slow.connect(path)
        slow.sendall(b"GET /slow HTTP/1.1\r\nHost: x\r\n\r\n")
        received = b""
        while b"started" not in received:
            received += slow.recv(4096)
        client, _ = ask("/restart")
        client.close()
        wait_ready(4)  # the successors have taken over the workers' lines
        while b"0\r\n\r\n" not in received:  # the old worker ends the response
            received += slow.recv(4096)
        slow.close()
        time.sleep(0.5)  # and closes the connection, after its successor took over
        held = [ask("/who") for _ in range(16)]
        split = Counter(body["worker"] for _, body in held)
        for client, _ in held:
            client.close()
    finally:
        os.killpg(process.pid, signal.SIGTERM)
        process.communicate(timeout=30)
    assert len(split) == 2 and max(split.values()) - min(split.values()) <= 2, split


def test_a_tcp_address_queues_connections_while_no_worker_accepts(short_dir):
    # The main process listens from the start: a connection waits until a
    # worker accepts it, as the workers start and as they all restart. (With a
    # listener per worker instead, such a connection was refused.)
    import socket
    import time
    port = free_port()
    script = short_dir / "restart_app.py"
    script.write_text(RESTART_APP)
    process = subprocess.Popen([sys.executable, str(script)], stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True, cwd=short_dir,
                               env={**os.environ, "PORT": str(port), "START_DELAY": "1"},
                               start_new_session=True)

    def wait_for(pattern, count):
        deadline = time.monotonic() + 30
        while len(list(short_dir.glob(pattern))) < count:
            assert process.poll() is None and time.monotonic() < deadline
            time.sleep(0.01)

    def ask(target):
        with socket.create_connection(("127.0.0.1", port), timeout=10) as client:
            client.sendall(f"GET {target} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n".encode())
            received = b""
            while chunk := client.recv(4096):
                received += chunk
        return json.loads(received.partition(b"\r\n\r\n")[2])

    try:
        wait_for("bound", 1)
        assert not list(short_dir.glob("ready-*"))
        first = ask("/who")["pid"]
        wait_for("ready-*", 2)
        ask("/restart")
        wait_for("stopped-*", 2)
        assert len(list(short_dir.glob("ready-*"))) == 2  # the successors still start
        second = ask("/who")["pid"]
    finally:
        os.killpg(process.pid, signal.SIGTERM)
        process.communicate(timeout=30)
    assert second != first


@pytest.mark.parametrize("family", ["unix", "tcp"])
def test_each_worker_listens_on_its_own_address_too(short_dir, family):
    # NATIVE_WORKER_LISTEN: a proxy lists every worker's own address and
    # balances requests over them (nginx: least_conn).
    if family == "unix":
        env = {"UNIX": str(short_dir / "app.sock"), "WORKER_LISTEN": str(short_dir / "w{n}.sock")}
    else:
        while True:  # ports <prefix>0 and <prefix>1, both free
            prefix = free_port() // 10
            if prefix * 10 + 1 < 65536 and all(port_free(prefix * 10 + n) for n in (0, 1)):
                break
        env = {"PORT": str(free_port()), "WORKER_LISTEN": f"127.0.0.1:{prefix}{{n}}"}
    [results], _ = run_workers(short_dir, env)
    shared = [result for result in results if result["asked"] is None]
    assert len(shared) == 4
    for result in results:
        if result["asked"] is not None:
            assert result["worker"] == f"Oldman-Server-{result['asked']}-0"
            if family == "unix":
                assert result["server"] == str(short_dir / f"w{result['asked']}.sock")
    # Each worker removes its own socket file as it exits.
    assert not list(short_dir.glob("w*.sock"))


def test_a_single_process_listens_on_the_worker_0_address(short_dir):
    lines, _ = run(f'''
        app.config.NATIVE_SERVER = True
        app.config.NATIVE_SHARDS = 1
        app.config.NATIVE_WORKER_LISTEN = "{short_dir}/w{{n}}.sock"
        app.config.UNIX_SOCKET_MODE = 0o600

        @app.after_server_start
        async def requests(app):
            print("MODE " + oct(os.stat("{short_dir}/w0.sock").st_mode & 0o777), flush=True)
            reader, writer = await asyncio.open_unix_connection("{short_dir}/w0.sock")
            writer.write(b"GET /json HTTP/1.1\\r\\nHost: own.test\\r\\nConnection: close\\r\\n\\r\\n")
            print("RESPONSE " + (await reader.read()).decode().split("\\r\\n\\r\\n", 1)[1], flush=True)
            app.stop()

        app.run(host="127.0.0.1", port={free_port()}, single_process=True)
    ''')
    assert json.loads(lines["RESPONSE"])["host"] == "own.test"
    assert lines["MODE"] == "0o600"
    assert not list(short_dir.glob("w*.sock"))


def test_tcp_connections_send_each_write_at_once():
    # TCP_NODELAY, as asyncio and uvloop set it: without it a streamed
    # response's next piece waited until the client acknowledged the last
    # (which a client may delay some 40 ms).
    port = free_port()
    lines, _ = run(f'''
        import socket
        app.config.NATIVE_SERVER = True
        app.config.NATIVE_SHARDS = 1

        @app.get("/nodelay")
        async def nodelay(request):
            # This connection's socket among the process's descriptors: on
            # the server's port, from the client's.
            for name in os.listdir("/proc/self/fd"):
                try:
                    sock = socket.socket(fileno=os.dup(int(name)))
                except OSError:
                    continue  # not a socket
                with sock:
                    try:
                        found = (sock.family == socket.AF_INET and sock.getsockname()[1] == {port}
                                 and sock.getpeername()[1] == request.port)
                    except OSError:
                        found = False
                    if found:
                        return text(str(sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY)))
            return text("not found")

        @app.after_server_start
        async def requests(app):
            _, body = await exchange({port}, b"GET /nodelay HTTP/1.1\\r\\nHost: x\\r\\n\\r\\n")
            print("NODELAY " + body, flush=True)
            app.stop()

        app.run(host="127.0.0.1", port={port}, single_process=True)
    ''')
    assert lines["NODELAY"] == "1"


def test_sanic_testing_client_once_per_process():
    lines, _ = run('''
        app.config.NATIVE_SERVER = True
        app.config.NATIVE_SHARDS = 1
        request, response = app.test_client.get("/json", headers={"Host": "client.test"})
        print("STATUS " + json.dumps([response.status, response.json, request.host]))
    ''')
    status, payload, host = json.loads(lines["STATUS"])
    assert status == 200 and payload["ok"] is True and host == "client.test"
    assert payload["loop"] == "uvloop.Loop"


UNIX_DRIVE = '''
    import stat

    @app.get("/where")
    async def where(request):
        return json_response([str(request.conn_info.server), request.ip, request.port,
                              request.conn_info.peername,
                              request.transport.get_extra_info("peername"),
                              request._native_request.shard])

    async def unix_exchange(path, count):
        # `count` connections open at once: shard 0 accepts and hands some to shard 1.
        streams = [await asyncio.open_unix_connection(path) for _ in range(count)]
        results = []
        for reader, writer in streams:
            # Two pipelined requests, the second with a body.
            writer.write(b"GET /where HTTP/1.1\\r\\nHost: x\\r\\n\\r\\n"
                         b"POST /echo HTTP/1.1\\r\\nHost: x\\r\\nContent-Length: 2\\r\\n\\r\\nhi")
            for _ in range(2):
                head = await reader.readuntil(b"\\r\\n\\r\\n")
                length = int(head.lower().split(b"content-length: ")[1].split(b"\\r\\n")[0])
                results.append(json.loads(await reader.readexactly(length)) if not results or
                               len(results) % 2 == 0 else (await reader.readexactly(length)).decode())
        for _, writer in streams:
            writer.close()
        return results

    def drive_unix(path):
        @app.after_server_start
        async def requests(app):
            mode = stat.S_IMODE(os.stat(path).st_mode)
            print("RESULTS " + json.dumps([oct(mode), await unix_exchange(path, 4)]), flush=True)
            app.stop()
'''


@pytest.fixture
def short_dir():
    # Sanic binds a temporary name 44 characters longer than the socket path;
    # AF_UNIX paths are limited to 107 bytes.
    with tempfile.TemporaryDirectory(dir="/tmp", prefix="om") as directory:
        yield Path(directory)


def test_unix_socket_served_natively(short_dir):
    path = short_dir / "app.sock"
    lines, result = run(textwrap.dedent(UNIX_DRIVE) + textwrap.dedent(f'''
        app.config.NATIVE_SERVER = True
        app.config.NATIVE_SAME_THREAD = False  # two shards, whatever the environment says
        drive_unix({str(path)!r})
        app.run(unix={str(path)!r})
        print("AFTER " + json.dumps([os.path.exists({str(path)!r}), events]))
        from oldman_server.native import host as seastar_sanic  # served by the native host
    '''))
    mode, results = json.loads(lines["RESULTS"])
    assert mode == oct(0o666)
    wheres = results[0::2]
    # Sanic's unix socket ConnInfo: the socket path, no client address.
    assert {tuple(where[:5]) for where in wheres} == {(str(path), "", 0, None, "")}
    assert {where[5] for where in wheres} == {0, 1}  # both shards serve
    assert results[1::2] == ["hi"] * 4
    exists, events = json.loads(lines["AFTER"])
    assert not exists  # removed at exit, as Sanic does
    assert events[:3] == ["main_process_start", "main_process_ready", "before_server_start"]
    assert "does not serve" not in result.stdout + result.stderr


def test_create_server_on_a_unix_socket(short_dir):
    path = short_dir / "app.sock"
    lines, _ = run(f'''
        app.config.NATIVE_SERVER = True
        async def main():
            server = await app.create_server(unix={str(path)!r}, return_asyncio_server=True)
            await server.startup()
            await server.before_start()
            await server.after_start()
            reader, writer = await asyncio.open_unix_connection({str(path)!r})
            writer.write(b"GET /json HTTP/1.1\\r\\nHost: unix.test\\r\\nConnection: close\\r\\n\\r\\n")
            response = await reader.read()
            writer.close()
            await server.before_stop()
            await server.close()
            await server.after_stop()
            print("RESPONSE " + json.dumps(response.split(b"\\r\\n\\r\\n", 1)[1].decode()))
            print("NATIVE " + json.dumps(type(server.server).__name__))
        asyncio.run(main())
    ''')
    assert json.loads(json.loads(lines["RESPONSE"]))["host"] == "unix.test"
    assert json.loads(lines["NATIVE"]) == "NativeServer"


def test_a_second_server_on_the_loop_uses_the_asyncio_server():
    # One native server per process: a second create_server on the loop it
    # serves (another app's server, say) uses the asyncio server, with a
    # warning, and both serve. Closing one leaves the other running.
    first, second = free_port(), free_port()
    lines, result = run(f'''
        app.config.NATIVE_SERVER = True
        async def main():
            a = await app.create_server(host="127.0.0.1", port={first}, return_asyncio_server=True)
            await a.startup()
            b = await app.create_server(host="127.0.0.1", port={second}, return_asyncio_server=True)
            request = b"GET /json HTTP/1.1\\r\\nHost: x\\r\\n\\r\\n"
            statuses = [(await exchange(port, request))[0] for port in ({first}, {second})]
            await a.close()
            statuses.append((await exchange({second}, request))[0])
            await b.close()
            await a.close()  # again: stopping is idempotent
            print("STATUS " + json.dumps(statuses))
        asyncio.run(main())
    ''')
    assert json.loads(lines["STATUS"]) == ["200"] * 3
    assert "this one uses the asyncio server" in result.stdout + result.stderr


def test_tcp_run_serves_on_sanics_socket():
    # The native host accepts on the socket Sanic bound (shard 0 hands
    # connections to the others); it stays open while serving, so
    # app.serve_location (read by sanic-ext's OpenAPI) works as with Sanic.
    port = free_port()
    lines, _ = run(f'''
        app.config.NATIVE_SERVER = True
        app.config.NATIVE_SAME_THREAD = False  # two shards, whatever the environment says

        @app.get("/shard")
        async def shard(request):
            return text(str(request._native_request.shard))

        @app.after_server_start
        async def requests(app):
            streams = [await asyncio.open_connection("127.0.0.1", {port}) for _ in range(4)]
            shards = []
            for reader, writer in streams:
                writer.write(b"GET /shard HTTP/1.1\\r\\nHost: x\\r\\n\\r\\n")
                head = await reader.readuntil(b"\\r\\n\\r\\n")
                length = int(head.lower().split(b"content-length: ")[1].split(b"\\r\\n")[0])
                shards.append(int(await reader.readexactly(length)))
            for _, writer in streams:
                writer.close()
            print("RESULTS " + json.dumps([app.serve_location, sorted(set(shards))]), flush=True)
            app.stop()

        app.run(host="127.0.0.1", port={port})
    ''')
    location, shards = json.loads(lines["RESULTS"])
    assert location == f"http://127.0.0.1:{port}" and shards == [0, 1]


def test_a_child_forked_after_the_native_host_started_uses_asyncio():
    # Sanic's "fork" start method from a process where the native runtime already ran
    # (a test client's server, say): the child has the runtime's memory but
    # not its threads, so its servers use the asyncio server.
    lines, _ = run('''
        app.config.NATIVE_SERVER = True
        request, response = app.test_client.get("/json")
        from oldman_server.native import engine
        from oldman_server.server.runners import _native_unavailable
        pid = os.fork()
        if pid == 0:
            print("CHILD " + json.dumps([engine.startable(), _native_unavailable(None)]), flush=True)
            os._exit(0)
        os.waitpid(pid, 0)
        print("PARENT " + json.dumps([response.status, engine.startable()]), flush=True)
    ''')
    startable, reason = json.loads(lines["CHILD"])
    assert not startable and "forked after it started" in reason
    assert json.loads(lines["PARENT"]) == [200, True]


def io_uring_available():
    """Whether this system lets a process set up io_uring (container seccomp
    profiles commonly refuse io_uring_setup)."""
    import ctypes
    libc = ctypes.CDLL(None, use_errno=True)
    params = (ctypes.c_byte * 120)()  # struct io_uring_params
    fd = libc.syscall(425, 1, params)  # io_uring_setup: 425 on x86_64 and aarch64
    if fd < 0:
        return False
    os.close(fd)
    return True


@pytest.mark.parametrize("setup, expected", [
    ("", 10000),                                         # Python's own: raised
    ("gc.set_threshold(5000, *gc.get_threshold()[1:])", 5000),  # tuned by the program: kept
    ("app.config.NATIVE_GC_THRESHOLD = None", None),     # turned off: Python's own
])
def test_the_native_host_raises_the_collectors_first_threshold(setup, expected):
    port = free_port()
    lines, _ = run(f'''
        import gc
        default = gc.get_threshold()
        app.config.NATIVE_SERVER = True
        {setup}

        @app.after_server_start
        async def thresholds(app):
            print("GC " + json.dumps([default, gc.get_threshold()]), flush=True)

        drive({port})
        app.run(host="127.0.0.1", port={port}, single_process=True)
    ''')
    default, serving = json.loads(lines["GC"])
    assert serving == [expected or default[0], *default[1:]]
    assert [status for status, _ in json.loads(lines["RESULTS"])] == ["200", "200"]


@pytest.mark.parametrize("backend", ["epoll", "io_uring"])
def test_reactor_backend_setting(backend):
    if backend == "io_uring" and not io_uring_available():
        pytest.skip("io_uring_setup is not permitted here")
    # NATIVE_REACTOR_BACKEND picks the shards' I/O (the io_uring build of the
    # runtime for io_uring); every shard runs it.
    port = free_port()
    lines, _ = run(f'''
        from oldman_server.native import host
        app.config.NATIVE_SERVER = True
        app.config.NATIVE_SAME_THREAD = False  # shard threads, which report their backend
        app.config.NATIVE_REACTOR_BACKEND = "{backend}"

        @app.after_server_start
        async def backends(app):
            print("BACKENDS " + json.dumps(sorted({{s["backend"] for s in host._engine.shards()}})), flush=True)

        drive({port})
        app.run(host="127.0.0.1", port={port})
    ''')
    assert json.loads(lines["BACKENDS"]) == [backend]
    assert [status for status, _ in json.loads(lines["RESULTS"])] == ["200", "200"]


@pytest.mark.parametrize("backend", ["epoll", "io_uring"])
def test_the_same_thread_mode_serves_on_either_backend(backend):
    # One process in the same-thread mode: no shard thread, on the backend
    # NATIVE_REACTOR_BACKEND names (io_uring where the system allows it).
    if backend == "io_uring" and not io_uring_available():
        pytest.skip("io_uring_setup is not permitted here")
    port = free_port()
    lines, result = run(f'''
        from oldman_server.native import host
        app.config.NATIVE_SERVER = True
        app.config.NATIVE_SAME_THREAD = True
        app.config.NATIVE_REACTOR_BACKEND = "{backend}"

        @app.after_server_start
        async def layout(app):
            engine = host._engine
            print("LAYOUT " + json.dumps([engine.shards(), engine.config.same_thread,
                                          engine._native.fallback()]), flush=True)

        drive({port})
        app.run(host="127.0.0.1", port={port})
    ''')
    assert json.loads(lines["LAYOUT"]) == [[], True, ""]
    assert [status for status, _ in json.loads(lines["RESULTS"])] == ["200", "200"]
    assert "io_uring is unavailable" not in result.stdout + result.stderr


def test_the_same_thread_mode_refuses_shard_cpus():
    result = subprocess.run([sys.executable, "-c", APP + textwrap.dedent(f'''
        app.config.NATIVE_SERVER = True
        app.config.NATIVE_SAME_THREAD = True
        app.config.NATIVE_SHARD_CPUS = "1"
        app.run(host="127.0.0.1", port={free_port()})
        print("RUN RETURNED", flush=True)
    ''')], capture_output=True, text=True, timeout=60, cwd=ROOT)
    assert result.returncode != 0 and "RUN RETURNED" not in result.stdout
    assert "the same-thread mode has none" in result.stderr


def test_io_uring_falls_back_to_epoll_without_its_build(tmp_path):
    # Where the io_uring build cannot run (here: it is missing next to the
    # extension; elsewhere seccomp or an old kernel refuses io_uring), the
    # runtime serves with epoll and says why.
    from oldman_server.native import _native
    extension = tmp_path / os.path.basename(_native.__file__)
    shutil.copy(_native.__file__, extension)
    code = textwrap.dedent(f'''
        import importlib.util, json, os, select
        spec = importlib.util.spec_from_file_location("_native", {str(extension)!r})
        native = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(native)
        runtime = native.Runtime([sorted(os.sched_getaffinity(0))[-1]], native.ServerConfig(), "io_uring")
        while runtime.stats()["state"] == 0:
            select.select([runtime.fd()], [], [], 1)
            runtime.clear_notification()
        print(json.dumps([runtime.fallback(), [s["backend"] for s in runtime.shards()]]))
        runtime.stop()
        runtime.join()
        runtime.close_fd()
    ''')
    done = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=30)
    assert done.returncode == 0, done.stderr
    fallback, backends = json.loads(done.stdout)
    assert "liboldman_native_uring.so" in fallback and backends == ["epoll"]


def test_unknown_reactor_backend_is_refused():
    from oldman_server.native import _native
    with pytest.raises(ValueError, match="epoll or io_uring"):
        _native.Runtime([sorted(os.sched_getaffinity(0))[-1]], _native.ServerConfig(), "linux-aio")


def test_failing_main_process_start_still_runs_main_process_stop():
    # As Sanic.serve: main_process_start runs before the socket is bound, and
    # main_process_stop runs even when it fails.
    with tempfile.TemporaryDirectory() as directory:
        path = os.path.join(directory, "app.sock")
        lines, _ = run(f'''
            app.config.NATIVE_SERVER = True
            app.config.NATIVE_SHARDS = 1

            @app.main_process_start
            def fail(app):
                raise RuntimeError("main start failed")

            try:
                app.run(unix={path!r})
            except RuntimeError as error:
                print("FAILED " + json.dumps(str(error)))
            print("EVENTS " + json.dumps(events))
            print("LEFT " + json.dumps(os.path.exists({path!r})))
        ''')
    assert json.loads(lines["FAILED"]) == "main start failed"
    assert json.loads(lines["EVENTS"]) == ["main_process_start", "main_process_stop"]
    assert json.loads(lines["LEFT"]) is False


@pytest.mark.parametrize("native", [True, False])
def test_sigterm_right_after_the_first_response_stops_gracefully(short_dir, native):
    # The native host answers while it starts, and uvloop drops a signal that
    # arrives while its loop is not running: the process was killed by the
    # SIGTERM (no stop listeners) or never stopped.
    path = str(short_dir / "app.sock")
    server = textwrap.dedent(f'''
        from oldman_server import OldmanServer
        from oldman_server.response import text
        app = OldmanServer("sigterm_race", configure_logging=False)
        app.config.NATIVE_SERVER = {native}
        app.config.NATIVE_SHARDS = 1

        @app.get("/")
        async def index(request):
            return text("ok")

        @app.after_server_stop
        def stopped(app):
            print("STOPPED", flush=True)

        app.run(unix={path!r}{"" if native else ", single_process=True"})
    ''')
    outcomes = []
    for _ in range(3):
        process = subprocess.Popen([sys.executable, "-c", server], cwd=ROOT,
                                   stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                import socket
                client = socket.socket(socket.AF_UNIX)
                client.settimeout(1)
                try:
                    client.connect(path)
                    client.sendall(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                    data = b""
                    while chunk := client.recv(65536):
                        data += chunk
                    if data.startswith(b"HTTP/1.1 200"):
                        break
                except OSError:
                    time.sleep(0.005)
                finally:
                    client.close()
            process.send_signal(signal.SIGTERM)
            try:
                code = process.wait(8)
                outcomes.append((code, "STOPPED" in process.stdout.read()))
            except subprocess.TimeoutExpired:
                outcomes.append("hung")
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
            process.stdout.close()
    assert outcomes == [(0, True)] * 3


def test_a_native_host_that_cannot_start_fails_the_run():
    # A single process: app.run() raises (it returned normally, exit status 0).
    result = subprocess.run([sys.executable, "-c", APP + textwrap.dedent(f'''
        app.config.NATIVE_SERVER = True
        app.config.NATIVE_SAME_THREAD = False  # the shard threads' CPUs
        app.config.NATIVE_SHARD_CPUS = "4000,4001"
        app.run(host="127.0.0.1", port={free_port()})
        print("RUN RETURNED", flush=True)
    ''')], capture_output=True, text=True, timeout=60, cwd=ROOT)
    assert result.returncode != 0
    assert "RUN RETURNED" not in result.stdout
    assert "shard_cpus must be distinct" in result.stderr


def test_create_server_with_a_host_name():
    # Without a socket, create_server() binds as Sanic does: a host name works.
    port = free_port()
    lines, _ = run(f'''
        app.config.NATIVE_SERVER = True
        app.config.NATIVE_SHARDS = 1

        async def main():
            server = await app.create_server(host="localhost", port={port},
                                             return_asyncio_server=True)
            await server.startup()
            await server.before_start()
            await server.after_start()
            status, body = await exchange({port}, b"GET /json HTTP/1.1\\r\\nHost: named\\r\\n\\r\\n")
            print("RESULT " + json.dumps([status, json.loads(body)["host"]]), flush=True)
            await server.before_stop()
            await server.close()
            await server.after_stop()

        import uvloop
        uvloop.run(main())
    ''')
    assert json.loads(lines["RESULT"]) == ["200", "named"]


def test_a_passed_socket_is_served_as_it_is():
    # Bound, not listening, with SO_REUSEPORT (as an application may prepare
    # it): the host listens on this socket, as Sanic does.
    lines, _ = run('''
        import socket
        sock = socket.socket()
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        app.config.NATIVE_SERVER = True
        app.config.NATIVE_SHARDS = 1

        @app.after_server_start
        async def check(app):
            print("LISTENING " + json.dumps(bool(
                sock.getsockopt(socket.SOL_SOCKET, socket.SO_ACCEPTCONN))), flush=True)
            print("STATUS " + json.dumps((await exchange(port, b"GET /json HTTP/1.1\\r\\nHost: x\\r\\n\\r\\n"))[0]),
                  flush=True)
            app.stop()

        app.run(sock=sock, single_process=True)
    ''')
    assert json.loads(lines["LISTENING"]) is True
    assert json.loads(lines["STATUS"]) == "200"


DRAIN_SERVER = textwrap.dedent('''
    import asyncio, sys
    from oldman_server import OldmanServer
    from oldman_server.response import file_stream, raw, text
    port, path = int(sys.argv[1]), sys.argv[2]
    app = OldmanServer("drain", configure_logging=False)
    app.config.NATIVE_SERVER = True
    app.config.NATIVE_SHARDS = 1
    app.config.GRACEFUL_SHUTDOWN_TIMEOUT = 8
    app.config.ACCESS_LOG = False

    @app.get("/fast")
    async def fast(request):
        return text("fast")

    @app.get("/slow")
    async def slow(request):
        await asyncio.sleep(2)
        return text("slow")

    @app.get("/file")
    async def file(request):
        await asyncio.sleep(1)
        return await file_stream(path)

    @app.get("/big")
    async def big(request):
        return raw(b"b" * (8 << 20))

    @app.after_server_start
    def ready(app):
        print("READY", flush=True)

    app.run(host="127.0.0.1", port=port, single_process=True)
''')


def test_a_graceful_stop_stops_accepting_and_finishes_what_is_in_progress(tmp_path):
    # As Sanic: on SIGTERM the server stops accepting and closes idle
    # connections, while requests in progress finish, a file stream and a
    # large response still being read by a slow client included. The native
    # host went on accepting and answered new requests with 503, failed
    # file_stream with 500, and cut off responses still being written.
    import socket

    content = os.urandom(300_000)
    path = tmp_path / "data.bin"
    path.write_bytes(content)
    port = free_port()
    server = subprocess.Popen([sys.executable, "-c", DRAIN_SERVER, str(port), str(path)],
                              cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    def connect():
        return socket.create_connection(("127.0.0.1", port), timeout=5)

    def get(sock, target):
        sock.sendall(f"GET {target} HTTP/1.1\r\nHost: x\r\n\r\n".encode())

    readers = {}

    def response(sock):
        """(status, body) of the next response on sock, ("closed", b"") at its end."""
        reader = readers.setdefault(sock, sock.makefile("rb"))
        status_line = reader.readline()
        if not status_line:
            return "closed", b""
        headers = {}
        while (line := reader.readline()) != b"\r\n":
            name, value = line.decode().rstrip("\r\n").split(": ", 1)
            headers[name.lower()] = value
        if "content-length" in headers:
            return int(status_line.split()[1]), reader.read(int(headers["content-length"]))
        body = b""  # chunked
        while size := int(reader.readline(), 16):
            body += reader.read(size)
            reader.readline()
        reader.readline()
        return int(status_line.split()[1]), body

    try:
        assert server.stdout.readline().strip() == "READY"
        idle = connect()
        get(idle, "/fast")
        assert response(idle)[0] == 200
        slow, streamed, big = connect(), connect(), connect()
        big.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65536)  # not read until the end
        slow.sendall(b"GET /slow HTTP/1.1\r\nHost: x\r\n\r\nGET /fast HTTP/1.1\r\nHost: x\r\n\r\n")
        get(streamed, "/file")
        get(big, "/big")
        time.sleep(0.3)
        server.send_signal(signal.SIGTERM)
        time.sleep(0.5)
        # The idle keep-alive connection is closed, a new one refused.
        assert response(idle) == ("closed", b"")
        try:
            fresh = connect()
            get(fresh, "/fast")
            refused = response(fresh)[0] == "closed"
        except (ConnectionRefusedError, ConnectionResetError):
            refused = True
        assert refused
        # A busy connection is served, a request pipelined behind included;
        # then it closes.
        assert response(slow) == (200, b"slow")
        assert response(slow) == (200, b"fast")
        assert response(slow) == ("closed", b"")
        assert response(streamed) == (200, content)
        status, body = response(big)
        assert status == 200 and len(body) == 8 << 20
        assert server.wait(10) == 0
    finally:
        if server.poll() is None:
            server.kill()
            server.wait()
        server.stdout.close()
        server.stderr.close()


@pytest.mark.parametrize("preloaded", [False, True])
def test_preload_runs_the_program_again_with_the_libraries(preloaded):
    # The C library (mapped in every process anyway): what is checked is the
    # program run again with LD_PRELOAD, not what the dynamic linker does.
    library = next(line.split()[-1] for line in Path("/proc/self/maps").read_text().splitlines()
                   if Path(line.split()[-1]).name.startswith(("libc.so", "libc-")))
    port = free_port()
    lines, result = run(f'''
        print("START " + json.dumps([os.environ.get("LD_PRELOAD"),
                                     os.environ.get("_OLDMAN_SERVER_PRELOADED")]), flush=True)
        app.config.PRELOAD = {library!r}
        app.config.NATIVE_SERVER = True
        app.config.NATIVE_SHARDS = 1
        drive({port})
        app.run(host="127.0.0.1", port={port})
        print("PRELOAD " + json.dumps([os.environ.get("LD_PRELOAD"),
                                       os.environ.get("_OLDMAN_SERVER_PRELOADED")]))
    ''', env={"LD_PRELOAD": library if preloaded else ""})
    assert json.loads(lines["RESULTS"])[1] == ["200", "hi"]
    # Run once more only when LD_PRELOAD lacked the library; the marker of
    # the second run is gone by the time the app serves.
    assert result.stdout.count("START ") == (1 if preloaded else 2)
    assert json.loads(lines["START"]) == [library, None if preloaded else "1"]
    assert json.loads(lines["PRELOAD"]) == [library, None]


def test_preload_refuses_a_missing_library(tmp_path):
    result = subprocess.run([sys.executable, "-c", textwrap.dedent(f'''
        from oldman_server import OldmanServer
        app = OldmanServer("preload_missing", configure_logging=False)
        app.config.PRELOAD = {str(tmp_path / "missing.so")!r}
        app.run(single_process=True)
    ''')], capture_output=True, text=True, timeout=60, cwd=ROOT,
        env={k: v for k, v in os.environ.items() if k != "LD_PRELOAD"})
    assert result.returncode != 0
    assert "PRELOAD: no such library" in result.stderr


def test_native_server_setting(monkeypatch, caplog):
    import oldman_server.native as native
    monkeypatch.setattr(native, "_noted", False)
    linux = native.NATIVE_PLATFORM
    # Unset (the default): on Linux when the extension is installed.
    monkeypatch.setattr(native, "_extension", True)
    assert native.native_server({"NATIVE_SERVER": None}) is linux
    assert native.native_server({}) is linux
    assert native.native_server({"NATIVE_SERVER": False}) is False
    assert native.native_server({"NATIVE_SERVER": True}) is linux
    assert native.native_server({"NATIVE_SERVER": 1}) is linux
    assert not caplog.records
    # Without the extension the default serves by asyncio, with one warning.
    monkeypatch.setattr(native, "_extension", False)
    assert native.native_server({"NATIVE_SERVER": None}) is False
    assert native.native_server({"NATIVE_SERVER": None}) is False
    if linux:
        assert len(caplog.records) == 1 and "not installed" in caplog.records[0].getMessage()
