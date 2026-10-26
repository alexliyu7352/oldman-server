"""bare.py PORT: the native host's bare function with the site's three routes
(as its bare WSGI entries: a hand-written check of the path), 4 same-thread
workers on this process's CPUs; the machinery of probes/wrk_comparison/serve.py."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from probes.wrk_comparison import serve

TEXT = (("content-type", "text/plain; charset=utf-8"),)
loop_handler = serve.bare_handler


def handler(request):
    target = request.target
    if request.method == "GET":
        if target == b"/":
            return 200, b"", ()
        if target.startswith(b"/user/"):
            return 200, target[6:], TEXT
        if target == b"/loop":
            return loop_handler(request)
    elif request.method == "POST" and target == b"/user":
        return 200, b"", ()
    return 404, b"", ()


serve.bare_handler = handler  # bare_worker looks it up when it opens the Engine

if __name__ == "__main__":
    serve.bare(int(sys.argv[1]), 4, True)
