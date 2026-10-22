"""Sanic's standard entry points serve through the native host (NATIVE_SERVER).

Each scenario runs in its own interpreter: a process starts Seastar once.
"""
from collections import Counter
import json
import os
from pathlib import Path
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
        app.run(host="127.0.0.1", port={port}, single_process=True, motd=False)
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
        app.run(host="127.0.0.1", port={port}, motd=False)
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
        app.prepare(host="127.0.0.1", port=0, workers=2, motd=False)
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

    async def ask(address=None):
        address = address or (("unix", UNIX) if UNIX else ("tcp", "127.0.0.1", int(os.environ["PORT"])))
        if address[0] == "unix":
            reader, writer = await asyncio.open_unix_connection(address[1])
        else:
            reader, writer = await asyncio.open_connection(address[1], address[2])
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
        results = await (own() if WORKER_LISTEN else held() if HOLD
                         else asyncio.gather(*(get() for _ in range(64))))
        print("RESULTS " + json.dumps(results), flush=True)
        if MARK and not os.path.exists(MARK):
            open(MARK, "w").close()
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
            app.run(unix=UNIX, workers=2, motd=False)
        else:
            app.run(host="127.0.0.1", port=int(os.environ["PORT"]), workers=2, motd=False)
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


def expected_groups():
    from oldman_server.config import Config
    from oldman_server.native.cpus import worker_groups
    return worker_groups(Config(), 2)


def check_worker_cpus(results):
    groups = expected_groups()
    for result in results:
        group = groups[int(result["worker"].split("-")[2]) % len(groups)]
        assert result["python"] == [group[0]]
        assert result["shards"] == [[cpu] for cpu in group[1:]]
        assert result["loop"] == "uvloop.Loop"


def test_workers_serve_natively_on_their_own_cpus(short_dir):
    # The main process reserves the address; each worker listens there with
    # its own SO_REUSEPORT socket and serves through its own native host,
    # Python and shards on the CPU group the main process planned for it.
    port = free_port()
    [results], listeners = run_workers(short_dir, {"PORT": str(port)})
    assert {result["worker"] for result in results} == {"Oldman-Server-0-0", "Oldman-Server-1-0"}
    check_worker_cpus(results)
    assert {result["location"] for result in results} == {f"http://127.0.0.1:{port}"}
    assert {result["server"] for result in results} == {f"127.0.0.1:{port}"}
    # Each worker listens with its own socket.
    assert len(set.union(*listeners.values())) == 2


def test_restarted_workers_serve_natively_again(short_dir):
    port = free_port()
    (first, second), _ = run_workers(short_dir, {"PORT": str(port), "RESTART_MARK": str(short_dir / "mark")})
    for results in (first, second):
        assert {result["worker"] for result in results} == {"Oldman-Server-0-0", "Oldman-Server-1-0"}
        check_worker_cpus(results)
    # New processes, on the same CPU groups.
    assert not {result["pid"] for result in first} & {result["pid"] for result in second}


def test_workers_share_a_unix_socket(short_dir):
    # Unix sockets have no SO_REUSEPORT: the workers accept on the main
    # process's socket (instead of each binding its own over the path, so that
    # the last one would get every connection), which it removes at exit.
    path = short_dir / "app.sock"
    [results], listeners = run_workers(short_dir, {"UNIX": str(path)})
    check_worker_cpus(results)
    assert {result["server"] for result in results} == {str(path)}
    assert listeners.keys() == {"Oldman-Server-0-0", "Oldman-Server-1-0"}
    assert len(set.union(*listeners.values())) == 1
    assert not path.exists()
    # The workers take turns accepting (oldman_server.native.accept); a
    # burst of 64 used to split anywhere from 64:0 to 32:32.
    assert min(Counter(result["worker"] for result in results).values()) >= 16


def test_workers_sharing_a_unix_socket_take_turns(short_dir):
    # Connections one after another, kept open: a worker accepts while no
    # other has fewer open connections. Without turns, the worker whose shard
    # is busy polling (not sleeping) takes nearly every connection.
    path = short_dir / "app.sock"
    [results], _ = run_workers(short_dir, {"UNIX": str(path), "HOLD": "16",
                                           "OLDMAN_SERVER_NATIVE_SHARDS": "1"})
    split = Counter(result["worker"] for result in results)
    assert split.keys() == {"Oldman-Server-0-0", "Oldman-Server-1-0"}
    assert max(split.values()) - min(split.values()) <= 2


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

    @app.after_server_start
    async def ready(app):
        open(f"ready-{os.getpid()}", "w").close()

    if __name__ == "__main__":
        app.run(unix=os.environ["UNIX"], workers=2, motd=False)
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

        app.run(host="127.0.0.1", port={free_port()}, single_process=True, motd=False)
    ''')
    assert json.loads(lines["RESPONSE"])["host"] == "own.test"
    assert lines["MODE"] == "0o600"
    assert not list(short_dir.glob("w*.sock"))


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
        drive_unix({str(path)!r})
        app.run(unix={str(path)!r}, motd=False)
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

        app.run(host="127.0.0.1", port={port}, motd=False)
    ''')
    location, shards = json.loads(lines["RESULTS"])
    assert location == f"http://127.0.0.1:{port}" and shards == [0, 1]


def test_a_child_forked_after_the_native_host_started_uses_asyncio():
    # Sanic's "fork" start method from a process where Seastar already ran
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


@pytest.mark.parametrize("backend", ["epoll", "linux-aio", "io_uring"])
def test_reactor_backend_setting(backend):
    if backend == "io_uring" and not io_uring_available():
        pytest.skip("io_uring_setup is not permitted here")
    # NATIVE_REACTOR_BACKEND picks Seastar's reactor backend; every shard runs it.
    port = free_port()
    lines, _ = run(f'''
        from oldman_server.native import host
        app.config.NATIVE_SERVER = True
        app.config.NATIVE_REACTOR_BACKEND = "{backend}"

        @app.after_server_start
        async def backends(app):
            print("BACKENDS " + json.dumps(sorted({{s["backend"] for s in host._engine.shards()}})), flush=True)

        drive({port})
        app.run(host="127.0.0.1", port={port}, motd=False)
    ''')
    assert json.loads(lines["BACKENDS"]) == [backend]
    assert [status for status, _ in json.loads(lines["RESULTS"])] == ["200", "200"]


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
                app.run(unix={path!r}, motd=False)
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

        app.run(unix={path!r}, motd=False{"" if native else ", single_process=True"})
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
        app.config.NATIVE_SHARD_CPUS = "4000,4001"
        app.run(host="127.0.0.1", port={free_port()}, motd=False)
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
    # it): the host listens on this socket, as Sanic does, rather than taking
    # it for its own address reservation.
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

        app.run(sock=sock, single_process=True, motd=False)
    ''')
    assert json.loads(lines["LISTENING"]) is True
    assert json.loads(lines["STATUS"]) == "200"
