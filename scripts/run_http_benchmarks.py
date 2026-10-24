"""Line and JSON exchange with a benchmark child process over its pipes."""
import json
import select


def read_line(proc, timeout=30):
    # Binary unbuffered pipes: select must observe every line, not Python's
    # text buffer. readline is safe after readiness; children flush records.
    if not select.select([proc.stdout], [], [], timeout)[0]:
        raise TimeoutError(f"child {proc.pid} did not report")
    line = proc.stdout.readline()
    if not line:
        raise RuntimeError(f"child {proc.pid} exited early: {proc.poll()}")
    return line.decode().strip()


def read_json(proc):
    while True:
        line = read_line(proc)
        if line.startswith("{"):
            return json.loads(line)


def send(proc, command):
    proc.stdin.write((command+"\n").encode())
    proc.stdin.flush()
