"""Own an explicitly targeted perf session with a startup acknowledgement."""
import json
import os
from pathlib import Path
import select
import shutil
import signal
import subprocess
import time

from probes.application_lifecycle.inputs import fingerprint


def perf_binary():
    # Ubuntu's PATH entry is a shell wrapper. Pin the kernel-matched ELF that
    # it launches, so hashes identify the actual recorder/symbolizer.
    path = Path('/usr/lib/linux-tools') / os.uname().release / 'perf'
    if not path.is_file():
        path = Path(shutil.which('perf') or '/nonexistent/perf')
    path = path.resolve()
    with path.open('rb') as stream:
        if stream.read(4) != b'\x7fELF':
            raise ValueError('perf must resolve to an ELF executable')
    return str(path)


def command(tid, out, control_fd, ack_fd, *, extra_tids=(), event="switches"):
    if event not in ("switches", "cpu"):
        raise ValueError("unknown capture event")
    tids = [tid, *extra_tids]
    if any(type(t) is not int or t <= 0 for t in tids) or len(set(tids)) != len(tids):
        raise ValueError("invalid capture threads")
    sample = (["-e", "context-switches", "-c", "1", "--switch-events"] if event == "switches"
              else ["-e", "cpu-clock:u", "-F", "199"])
    return [perf_binary(), "record", *sample, "--no-inherit", "-t", ",".join(map(str, tids)),
            "-g", "--call-graph", "dwarf,8192", "--clockid", "mono",
            "-m", "1024", "-D", "-1", "--control", f"fd:{control_fd},{ack_fd}",
            "-o", str(Path(out) / "perf.data")]


class Recorder:
    def __init__(self, tid, out, *, extra_tids=(), event="switches"):
        self.process = self.log = None
        self.fds = []
        self.out = Path(out)
        self.timeline = {"tid": tid, "created_ns": time.monotonic_ns()}
        if extra_tids or event != "switches":
            self.timeline.update(tids=[tid, *extra_tids], event=event)
        try:
            if (self.out / "perf.data").exists():
                raise FileExistsError(self.out / "perf.data")
            read_control, self.control = os.pipe()
            self.fds = [read_control, self.control]
            self.ack, write_ack = os.pipe()
            self.fds.extend((self.ack, write_ack))
            cmd = command(tid, out, read_control, write_ack, extra_tids=extra_tids, event=event)
            with (self.out / "perf-command.json").open("x") as stream:
                json.dump(cmd, stream, indent=2)
                stream.write("\n")
            self.log = (self.out / "perf.log").open("xb")
            self.process = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=self.log, stderr=self.log,
                pass_fds=(read_control, write_ack), start_new_session=True)
            for fd in (read_control, write_ack):
                os.close(fd)
                self.fds.remove(fd)
            self._control("enable")
            self.timeline["enabled_ack_ns"] = time.monotonic_ns()
            self.timeline['perf_executable'] = fingerprint(f'/proc/{self.process.pid}/exe')
            self.timeline['perf_pid'] = self.process.pid
        except BaseException:
            self.close()
            raise

    def _control(self, message):
        os.write(self.control, (message + "\n").encode())
        reply = os.read(self.ack, 4096) if select.select([self.ack], [], [], 5)[0] else None
        # Host perf 5.15 includes the C string terminator in its reply.
        if reply not in (b"ack\n", b"ack\n\0"):
            raise RuntimeError(f"perf did not acknowledge {message}: {reply!r}")

    def finish(self):
        # The caller has already waited for the target to exit. perf exits on
        # that final TID; sending a disable command here races with control EOF.
        self.timeline["finish_requested_ns"] = time.monotonic_ns()
        code = self.process.wait(10)
        self.timeline.update(returncode=code, finished_ns=time.monotonic_ns())
        with (self.out / "perf-control.json").open("x") as stream:
            json.dump(self.timeline, stream, indent=2)
            stream.write("\n")
        if code != 0:
            raise RuntimeError(f"perf failed with {code}; inspect perf.log")
        return self.timeline

    def close(self):
        if self.process is not None and self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(5)
        for fd in self.fds:
            os.close(fd)
        self.fds.clear()
        if self.log is not None:
            self.log.close()
