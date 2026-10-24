"""Three unmodified Sanic HTTP workers or one Sanic native service."""
import argparse
import cProfile
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import pstats
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


def native_config(port, cpus, listen_fd=-1, accept=None):
    from oldman_server.native.engine import Config, ServerConfig
    # As an application would set NATIVE_REACTOR_BACKEND (calibrate.py's
    # mode:workload:OLDMAN_SERVER_NATIVE_REACTOR_BACKEND=io_uring).
    backend = os.environ.get('OLDMAN_SERVER_NATIVE_REACTOR_BACKEND')
    return Config(shard_cpus=tuple(cpus[1:]), **({} if backend is None else {'reactor_backend': backend}),
                  server=ServerConfig(
        http_port=0 if listen_fd >= 0 else port, listen_fd=listen_fd,
        application_dispatch_all=True, max_requests=1024,
        max_bytes=64*1024*1024, max_body_bytes=65536, max_response_bytes=65536,
        decision_timeout_ms=5000, generate_date_header=True, coalesce_notifications=True,
        # Stock adds only X-Probe-Pid in its middleware; the framework host matches.
        probe_headers='pid',
        # Sanic's KEEP_ALIVE_TIMEOUT / REQUEST_TIMEOUT defaults, as stock enforces.
        keep_alive_timeout_ms=120000, request_timeout_ms=60000, **(accept or {})))


def application_contract(engine):
    from probes.sanic_boundary import application
    file = Path(application.__file__).resolve()
    return {'json_codec': 'oldman_server.response.json',
            'bytes_headers': getattr(engine, '_application_bytes_headers', False),
            'server_config': asdict(engine.config.server),
            'business': {'path': str(file), 'sha256': hashlib.sha256(file.read_bytes()).hexdigest()}}


def verify_stock():
    record = json.loads((ROOT/'upstream/sanic.json').read_text())
    root = ROOT/'build/sanic-upstream'/('sanic-'+record['commit'])
    for relative, expected in record['original_sha256'].items():
        if hashlib.sha256((root/relative).read_bytes()).hexdigest() != expected:
            raise ValueError(f'upstream source mismatch: {relative}')
    return root


class CounterWindow:
    """Only counters are differenced; retain metadata in the full snapshots."""
    def __init__(self, stats):
        self.stats = stats

    def begin(self):
        self.before = self.stats()

    def end(self):
        after = self.stats()
        return {'pid': os.getpid(), 'after': after,
                'delta': {k: v-self.before[k] for k,v in after.items() if isinstance(v, int)}}

    async def close(self):
        pass


def profile_records(stats):
    return [{'file': key[0], 'line': key[1], 'function': key[2],
             'primitive_calls': value[0], 'calls': value[1],
             'self_thread_cpu_seconds': value[2], 'cumulative_thread_cpu_seconds': value[3]}
            for key, value in sorted(stats.stats.items())]


class ProfileWindow(CounterWindow):
    """One worker, one thread CPU profile; never enabled on the default path."""
    def __init__(self, stats, out):
        super().__init__(stats)
        self.out = out
        self.profiler = None
        self.used = False

    def begin(self):
        if self.used:
            raise ValueError('profile window already used')
        super().begin()
        self.used = True
        self.profiler = cProfile.Profile(time.thread_time)
        self.cpu0 = time.thread_time_ns()
        self.profiler.enable()

    def end(self):
        if self.profiler is None:
            raise ValueError('no active profile window')
        profiler, self.profiler = self.profiler, None
        profiler.disable()
        cpu = time.thread_time_ns()-self.cpu0
        result = super().end()
        file = self.out/f'profile-{os.getpid()}.pstats'
        profiler.dump_stats(str(file))
        result.update(thread_cpu_ns=cpu, profile={
            'pid': os.getpid(), 'file': file.name, 'timer': 'thread_time',
            'sha256': hashlib.sha256(file.read_bytes()).hexdigest(),
            'calls': profile_records(pstats.Stats(profiler))})
        return result

    async def close(self):
        if self.profiler is not None:
            self.profiler.disable()
            self.profiler = None


class SampleWindow(CounterWindow):
    """Diagnosis only (PROBE_SAMPLE_OUT): SIGPROF stack samples of the main thread
    during the measured window, written as text; never used for gate windows."""
    def __init__(self, stats, out):
        super().__init__(stats)
        sys.path.insert(0, str(ROOT/'scripts'))
        from bench_inproc import Sampler
        self.sampler, self.out = Sampler(0.0005), Path(out)

    def begin(self):
        super().begin()
        self.cpu0 = time.thread_time_ns()
        self.sampler.start()

    def end(self):
        self.sampler.stop()
        result = super().end()
        requests = max(result['delta'].get('responses', 1), 1)
        cpu_us = (time.thread_time_ns() - self.cpu0) / 1000 / requests
        import contextlib, io
        text = io.StringIO()
        with contextlib.redirect_stdout(text):
            self.sampler.report(cpu_us, 60)
        self.out.write_text(f'main thread cpu_us/request={cpu_us:.3f}\n' + text.getvalue())
        return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=('stock', 'candidate'), required=True)
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--profile-dir', type=Path)
    parser.add_argument('--workers', type=int, default=1,
                        help='candidate: worker processes, each on an equal share of the CPUs')
    args = parser.parse_args()
    # Stock imports the verified upstream source as `sanic`; candidate uses
    # oldman_server. Dependencies and interpreter still come from the
    # single uv project environment.
    if args.mode == 'stock':
        sys.path.insert(0, str(verify_stock()))
    sys.path.insert(0, str(ROOT/'scripts'))
    import asyncio
    import socket
    import uvloop
    if args.mode == 'stock':
        import sanic as framework
    else:
        import oldman_server as framework
    import comparison_server as comparison
    from probes.application_lifecycle.inputs import runtime_state
    from probes.sanic_boundary.application import make_app
    if args.mode == 'candidate':
        from oldman_server.native.host import serve
    # Candidate worker processes: the socket they share and their accept turns.
    shared = {}

    class Service:
        def __init__(self, mode, cpus, requests, byte_limit):
            self.cpus = cpus
            self.counts = dict(started=0, finished=0, responses=0, errors=0, active=0)
            self.engine = self.host = self.server = None

        async def start(self, port, listener=None):
            app = self.app = make_app(stock=args.mode == 'stock')
            # Identical accounting middleware in both Python modes. Engine's
            # native resource counters remain separately checked on candidate.
            @app.on_request
            async def entered(request):
                self.counts['started'] += 1
                self.counts['active'] += 1
            @app.on_response
            async def finished(request, response):
                self.counts['finished'] += 1
                self.counts['active'] -= 1
                self.counts['responses'] += 1
                self.counts['errors'] += int(response.status != 200)
                if args.mode == 'stock':
                    response.headers['X-Probe-Pid'] = str(os.getpid())
            if args.mode == 'candidate':
                listen_fd = -1
                accept = None
                if args.workers > 1:
                    # The parent's listening socket, on which the worker
                    # processes take turns accepting, as Sanic's workers do
                    # with NATIVE_SERVER (oldman_server.native.accept).
                    import ctypes
                    from oldman_server.native.accept import SLOTS
                    listen_fd = shared['listener'].fileno()
                    accept = dict(listen_shared=True, accept_counts=ctypes.addressof(shared['table']),
                                  accept_slots=SLOTS, accept_slot=shared['groups'].index(self.cpus))
                config = native_config(port, self.cpus, listen_fd, accept)
                self.host = serve(app, config)
                self.engine = await self.host.__aenter__()
                os.sched_setaffinity(0, {self.cpus[0]})
            else:
                os.sched_setaffinity(0, {self.cpus[0]})
                app.config.REQUEST_MAX_SIZE = 65536
                app.config.KEEP_ALIVE_TIMEOUT = 120
                sock = socket.socket()
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                sock.bind(('127.0.0.1', port)); sock.listen(1024); sock.setblocking(False)
                self.server = await app.create_server(sock=sock, access_log=False)
                await self.server.startup()
                await self.server.before_start()
                await self.server.start_serving()
                await self.server.after_start()

        def stats(self):
            result = dict(self.counts)
            if self.engine:
                result.update({'engine_'+k:v for k,v in self.engine.server_stats().items()})
            return result

        def info(self):
            # The framework the service loaded (the sanic_* keys predate the rename).
            file = Path(framework.__file__).resolve()
            result = {'pid': os.getpid(), 'event_loop': str(type(asyncio.get_running_loop())),
                'runtime': runtime_state(), 'sanic_version': framework.__version__,
                'sanic_file': str(file), 'sanic_sha256': hashlib.sha256(file.read_bytes()).hexdigest(),
                'main_cpu': self.cpus[0], 'shards': self.engine.shards() if self.engine else []}
            if self.engine:
                from probes.sanic_boundary.server import module_evidence
                result['loaded'] = module_evidence()
                result['application_contract'] = application_contract(self.engine)
            return result

        async def close(self):
            if self.host:
                await self.host.__aexit__(None, None, None)
            elif self.engine:
                await self.engine.aclose()
            if self.server:
                await self.server.before_stop()
                await self.server.close()
                for connection in list(self.server.connections):
                    connection.close()
                await self.server.after_stop()

    def child(worker_args, cpu, requests, byte_limit, connection, inherited):
        for pipe in inherited:
            pipe.close()
        cpus = cpu if isinstance(cpu, list) else [cpu]
        uvloop.run(comparison.worker(worker_args, cpus, requests, byte_limit, connection))

    comparison.Snapshot = (lambda stats: ProfileWindow(stats, args.profile_dir)) if args.profile_dir else CounterWindow
    if os.environ.get('PROBE_SAMPLE_OUT') and not args.profile_dir:
        comparison.Snapshot = lambda stats: SampleWindow(stats, os.environ['PROBE_SAMPLE_OUT'])
    comparison.Service = Service
    comparison.child = child
    args.requests, args.bytes = 1024, 64*1024*1024
    cpus = sorted(os.sched_getaffinity(0))
    # Stock: one worker per CPU. Candidate: `workers` processes, each a Python
    # CPU and its shard CPUs (the layout Sanic's NATIVE_SERVER workers get).
    size = len(cpus) // args.workers
    if (len(cpus) not in (3, 4) or (args.workers > 1 and args.mode != 'candidate')
            or args.workers < 1 or size * args.workers != len(cpus) or size < 2):
        raise ValueError('three or four service CPUs, split evenly between candidate workers')
    if args.mode == 'stock':
        comparison.prefork(args, cpus)
    elif args.workers > 1:
        import multiprocessing
        from oldman_server.native.accept import counts
        shared['groups'] = [cpus[i:i + size] for i in range(0, len(cpus), size)]
        shared['listener'] = socket.socket()
        shared['listener'].setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        shared['listener'].bind(('127.0.0.1', args.port)); shared['listener'].listen(1024)
        shared['table'] = counts(multiprocessing.get_context('fork'))
        comparison.prefork(args, shared['groups'])
    else:
        uvloop.run(comparison.worker(args, cpus, args.requests, args.bytes))


if __name__ == '__main__':
    main()
