"""Serves an app that sets NATIVE_SERVER, as an application developed on macOS or
Windows would (the asyncio server serves there, after a warning), in one
process and with the worker manager: it must answer and exit cleanly."""
import socket
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path

APP = textwrap.dedent('''
    import asyncio, sys, urllib.request
    from oldman_server import OldmanServer
    from oldman_server.response import json

    PORT, SINGLE = int(sys.argv[1]), sys.argv[2] == "single"
    app = OldmanServer("compat_smoke")
    app.config.NATIVE_SERVER = True

    @app.get("/")
    async def index(request):
        return json({"loop": type(asyncio.get_running_loop()).__module__})

    @app.after_server_start
    async def check(app):
        def get():
            return urllib.request.urlopen(f"http://127.0.0.1:{PORT}/", timeout=10).read()
        print("BODY " + (await asyncio.to_thread(get)).decode(), flush=True)
        if SINGLE:
            app.stop()
        else:
            app.m.terminate()

    if __name__ == "__main__":
        app.run(host="127.0.0.1", port=PORT, single_process=SINGLE, motd=False)
''')


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def main():
    failed = []
    with tempfile.TemporaryDirectory() as directory:
        script = Path(directory) / "compat_smoke_app.py"
        script.write_text(APP)
        for mode in ("single", "workers"):
            result = subprocess.run([sys.executable, str(script), str(free_port()), mode],
                                    capture_output=True, text=True, timeout=90, cwd=directory)
            output = result.stdout + result.stderr
            ok = (result.returncode == 0 and "BODY {\"loop\":" in output
                  and "the native host runs on Linux only" in output)
            print(f"{mode}: {'ok' if ok else 'FAILED'} (exit {result.returncode})")
            if not ok:
                print(output[-4000:])
                failed.append(mode)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
