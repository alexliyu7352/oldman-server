"""How requests, connections and CPU split between native workers behind OpenResty.

Each cell starts the application afresh as a deployment does (`app.run(workers=N)`,
app.py) with OpenResty in front of it, drives OpenResty with http_load, and records
per worker: requests (X-Probe-Pid), upstream connections (ss, mid-window), Python
thread and other thread CPU. Placement of connections is random per start, so each
two-worker cell is repeated with fresh processes.

Run after `source scripts/env.sh`:
    python probes/nginx_proxy/run.py --out <directory> [--smoke]
"""
import argparse
import itertools
import json
import os
from pathlib import Path
import re
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT/'scripts'))
from probes.sanic_spike.run import free_port
from run_http_benchmarks import read_json, read_line, send
from run_comparison import stop_process
from probes.nginx_proxy.app import EXPECTED
from oldman_server.native.listen import address as listen_address

NGINX = Path('/usr/local/openresty/nginx/sbin/nginx')
# Physical cores 0-3 (one CCX) serve: one group of 1 Python thread + 1 shard per
# worker. OpenResty gets cores 4-5 with their SMT siblings, the client cores 6-7,
# this driver the siblings of the client's cores.
LAYOUT = {'server': (0, 1, 2, 3), 'nginx': (4, 5, 12, 13), 'client': (6, 7), 'control': (14, 15)}
TICK = os.sysconf('SC_CLK_TCK')


def nginx_conf(prefix, port, upstreams, mode, keepalive_requests):
    """mode "default": no proxy settings; OpenResty 1.31 then speaks HTTP/1.1
    to the upstream and keeps its connections (older nginx used HTTP/1.0 and a
    connection per request). "close": HTTP/1.0, a new upstream connection per
    request. "keepalive": the usual tuned pool. Several upstreams (one per
    worker) are balanced with least_conn."""
    masks = ' '.join(format(1 << cpu, '016b') for cpu in LAYOUT['nginx'])
    pool = 'least_conn;' if len(upstreams) > 1 else ''
    proxy = ''
    if mode == 'keepalive':
        pool += f' keepalive 32; keepalive_requests {keepalive_requests};'
        proxy = 'proxy_http_version 1.1; proxy_set_header Connection "";'
    elif mode == 'close':
        proxy = 'proxy_http_version 1.0; proxy_set_header Connection close;'
    servers = ' '.join(f'server {upstream};' for upstream in upstreams)
    temp = ' '.join(f'{name}_temp_path {prefix}/{name};' for name in
                    ('client_body', 'proxy', 'fastcgi', 'uwsgi', 'scgi'))
    return f'''
worker_processes {len(LAYOUT['nginx'])};
worker_cpu_affinity {masks};
daemon off;
pid {prefix}/nginx.pid;
error_log {prefix}/error.log warn;
events {{ worker_connections 8192; }}
http {{
    access_log off;
    {temp}
    upstream app {{ {servers} {pool} }}
    server {{
        listen 127.0.0.1:{port} reuseport backlog=4096;
        keepalive_requests 1000000;
        location / {{ proxy_pass http://app; {proxy} }}
    }}
}}
'''


def children(pid):
    return [int(p) for p in Path(f'/proc/{pid}/task/{pid}/children').read_text().split()]


def thread_ticks(pid):
    rows = {}
    for task in Path(f'/proc/{pid}/task').iterdir():
        try:
            fields = (task/'stat').read_text().rsplit(')', 1)[1].split()
            rows[int(task.name)] = int(fields[11]) + int(fields[12])
        except (FileNotFoundError, ProcessLookupError):
            pass
    return rows


def connections(address, pids):
    """Established upstream connections per worker, as ss attributes them."""
    if isinstance(address, int):
        lines = subprocess.run(['ss', '-Htnp', 'state', 'established', f'( sport = :{address} )'],
                               capture_output=True, text=True, check=True).stdout.splitlines()
    else:
        # The framework binds a temporary name (the path, a dot and a token) and renames
        # it, so accepted sockets show that name.
        names = (f' {address} ', f' {address}.')
        lines = [line for line in subprocess.run(['ss', '-Hxp'], capture_output=True, text=True,
                 check=True).stdout.splitlines()
                 if 'ESTAB' in line and any(name in line for name in names)]
    counts = dict.fromkeys(pids, 0)
    for line in lines:
        for pid in map(int, re.findall(r'pid=(\d+)', line)):
            if pid in counts:
                counts[pid] += 1
    return counts


def port_prefix(workers):
    """A prefix p such that ports p0 .. p<workers-1> are free."""
    while True:
        prefix = free_port() // 10
        ports = [prefix * 10 + n for n in range(workers)]
        if ports[-1] < 65536 and all(port_free(port) for port in ports):
            return prefix


def port_free(port):
    with socket.socket() as sock:
        try:
            sock.bind(('127.0.0.1', port))
        except OSError:
            return False
        return True


def wait_for(check, timeout, what):
    deadline = time.monotonic() + timeout
    while not check():
        if time.monotonic() > deadline:
            raise TimeoutError(what)
        time.sleep(0.05)


def cell(out, *, transport, mode, workers, workload, concurrency, rate, repeat, warmup, seconds,
         keepalive_requests):
    key = f'{transport}-{mode}-w{workers}-{workload}-c{concurrency}-r{rate:g}-{repeat}'
    path = out/f'{key}.json'
    if path.exists():
        return json.loads(path.read_text())
    work = Path(tempfile.mkdtemp(prefix='np'))  # short: unix socket paths are limited to 107 bytes
    processes, logs = [], []

    def start(command, role, cpus, env=None):
        log = (work/f'{role}.log').open('wb'); logs.append(log)
        child = subprocess.Popen(['taskset', '-c', ','.join(map(str, cpus))] + command,
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE if role == 'client' else log,
                                 stderr=log, bufsize=0, start_new_session=True, env=env)
        processes.append(child); return child

    row = dict(key=key, transport=transport, mode=mode, workers=workers, workload=workload,
               concurrency=concurrency, rate=rate, repeat=repeat, warmup=warmup, seconds=seconds,
               keepalive_requests=keepalive_requests, layout=LAYOUT)
    try:
        # "tcp"/"unix": OpenResty proxies to the run's one address.
        # "tcp-each"/"unix-each": each worker also listens on its own address
        # (NATIVE_WORKER_LISTEN), which OpenResty balances with least_conn.
        each = transport.endswith('-each')
        family = transport.split('-')[0]
        shared = free_port() if family == 'tcp' else str(work/'app.sock')
        env = os.environ | {'PROBE_OUT': str(work), 'OLDMAN_SERVER_NATIVE_SERVER': 'true',
                            'OLDMAN_SERVER_NATIVE_SHARDS': '1', 'UV_USE_IO_URING': '0'}
        if each:
            template = f'127.0.0.1:{port_prefix(workers)}{{n}}' if family == 'tcp' else str(work/'w{n}.sock')
            env['OLDMAN_SERVER_NATIVE_WORKER_LISTEN'] = template
            addresses = [listen_address(template, n)[-1] for n in range(workers)]
        else:
            addresses = [shared]
        app = start([sys.executable, str(Path(__file__).with_name('app.py')), '--workers', str(workers)]
                    + (['--port', str(shared)] if family == 'tcp' else ['--unix', shared]),
                    'app', LAYOUT['server'][:2*workers], env)
        wait_for(lambda: len(list(work.glob('worker-*.json'))) == workers or app.poll() is not None,
                 60, 'workers did not start')
        if app.poll() is not None:
            raise RuntimeError(f'application exited: {app.returncode}')
        pids = sorted(json.loads(p.read_text())['pid'] for p in work.glob('worker-*.json'))
        row['worker_cpus'] = {pid: {tid: sorted(os.sched_getaffinity(tid)) for tid in thread_ticks(pid)}
                              for pid in pids}

        port = free_port()
        upstreams = [f'127.0.0.1:{a}' if family == 'tcp' else f'unix:{a}' for a in addresses]
        (work/'nginx.conf').write_text(nginx_conf(work, port, upstreams, mode, keepalive_requests))
        nginx = start([str(NGINX), '-p', str(work), '-c', str(work/'nginx.conf'), '-e', str(work/'error.log')],
                      # Not the project's libraries (env.sh): its OpenSSL is not OpenResty's.
                      'nginx', LAYOUT['nginx'], {k: v for k, v in os.environ.items() if k != 'LD_LIBRARY_PATH'})
        wait_for(lambda: (work/'nginx.pid').exists() and len(children(int((work/'nginx.pid').read_text())))
                 == len(LAYOUT['nginx']) or nginx.poll() is not None, 10, 'nginx did not start')
        if nginx.poll() is not None:
            raise RuntimeError(f'nginx exited: {nginx.returncode}')
        nginx_pids = children(int((work/'nginx.pid').read_text()))
        with urllib.request.urlopen(f'http://127.0.0.1:{port}/{workload}', timeout=5) as response:
            if response.read() != EXPECTED or int(response.headers['X-Probe-Pid']) not in pids:
                raise ValueError('preflight response mismatch')

        (work/'expected.bin').write_bytes(EXPECTED)
        client = start([str(ROOT/'build/http_load'), '--smp', str(len(LAYOUT['client'])),
                        '--cpuset', ','.join(map(str, LAYOUT['client'])), '--port', str(port),
                        '--target-pids', ','.join(map(str, pids)), '--path', '/'+workload,
                        '--expected', str(work/'expected.bin'), '--concurrency', str(concurrency),
                        '--rate', str(rate), '--warmup', str(warmup), '--seconds', str(seconds)],
                       'client', LAYOUT['client'])
        while read_line(client, warmup+30) != 'MEASURE_READY':
            pass
        before = {pid: thread_ticks(pid) for pid in pids + nginx_pids}
        started = time.monotonic()
        send(client, 'start')
        time.sleep(seconds/2)
        row['connections'] = dict.fromkeys(pids, 0)
        for address in addresses:
            for pid, count in connections(address, pids).items():
                row['connections'][pid] += count
        while read_line(client, seconds+30) != 'MEASURE_DONE':
            pass
        elapsed = time.monotonic() - started
        after = {pid: thread_ticks(pid) for pid in pids + nginx_pids}
        row['client'] = result = read_json(client)

        def cores(pid, threads):
            return sum(after[pid][t] - before[pid].get(t, 0) for t in threads) / TICK / elapsed
        total = sum(result['server_pids'].values())
        row['per_worker'] = {pid: {
            'requests': result['server_pids'].get(str(pid), 0),
            'share': result['server_pids'].get(str(pid), 0) / total if total else 0,
            'connections': row['connections'][pid],
            'python_cores': cores(pid, [pid]),
            'other_cores': cores(pid, [t for t in after[pid] if t != pid])} for pid in pids}
        row['nginx_cores'] = sum(cores(pid, list(after[pid])) for pid in nginx_pids)
        shares = [w['share'] for w in row['per_worker'].values()]
        row['share_ratio'] = min(shares) / max(shares) if max(shares) else 0
        row['rps'], row['p50_us'], row['p99_us'] = result['rps'], result['p50_us'], result['p99_us']
        row['errors'] = result['errors']  # includes status_errors (non-200 replies)
        row['nginx_errors'] = (work/'error.log').read_text(errors='replace').splitlines()[-5:]
        if result['integrity_errors']:
            raise ValueError('integrity errors')
    except BaseException as error:
        row['error'] = repr(error)
        for log in work.glob('*.log'):
            row.setdefault('logs', {})[log.name] = log.read_text(errors='replace')[-4000:]
        raise
    finally:
        for process in reversed(processes):
            stop_process(process)
        for log in logs:
            log.close()
        path.write_text(json.dumps(row, indent=1, sort_keys=True))
        shutil.rmtree(work, ignore_errors=True)
    return row


def summarize(rows):
    """Median per configuration; the gate looks at the worst repeat."""
    groups = {}
    for row in rows:
        groups.setdefault((row['transport'], row['mode'], row['workload'], row['concurrency'], row['rate'],
                           row['workers']), []).append(row)
    lines = []
    for (transport, mode, workload, concurrency, rate, workers), cells in sorted(groups.items()):
        single = groups.get((transport, mode, workload, concurrency, rate, 1)) or groups.get(
            ('tcp', mode, workload, concurrency, rate, 1))
        rps = statistics.median(c['rps'] for c in cells)
        base = statistics.median(c['rps'] for c in single) if single else None
        lines.append({
            'transport': transport, 'mode': mode, 'workload': workload, 'concurrency': concurrency,
            'rate': rate, 'workers': workers, 'repeats': len(cells), 'rps': round(rps),
            'scaling': round(rps/base, 3) if base else None,
            'rps_min': round(min(c['rps'] for c in cells)),
            'share_ratio_min': round(min(c['share_ratio'] for c in cells), 3),
            'connections': [sorted(c['connections'].values()) for c in cells],
            'python_cores': [sorted(round(w['python_cores'], 2) for w in c['per_worker'].values()) for c in cells],
            'nginx_cores': round(max(c['nginx_cores'] for c in cells), 2),
            'p99_us': round(statistics.median(c['p99_us'] for c in cells)),
            'errors': sum(c['errors'] for c in cells)})
    return lines


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--smoke', action='store_true')
    parser.add_argument('--workload', default='work', choices=('small', 'work', 'sleep'))
    parser.add_argument('--transports', default='tcp,unix')
    parser.add_argument('--modes', default='close,keepalive')
    parser.add_argument('--concurrency', default='8,32,128,512')
    parser.add_argument('--rate', type=float, default=0, help='open-loop arrivals/s; 0 is closed-loop')
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--keepalive-requests', type=int, default=1000, help="nginx's upstream default")
    parser.add_argument('--skip-single', action='store_true', help='no one-worker baseline cells')
    args = parser.parse_args()
    if not os.sched_getaffinity(0) >= set(itertools.chain(*LAYOUT.values())):
        raise SystemExit('the layout needs CPUs 0-7 and 12-15')
    os.sched_setaffinity(0, LAYOUT['control'])
    args.out.mkdir(parents=True, exist_ok=True)
    transports, modes = args.transports.split(','), args.modes.split(',')
    levels = [int(c) for c in args.concurrency.split(',')]
    warmup, seconds, repeats = (1, 2, 1) if args.smoke else (2, 6, args.repeats)
    if args.smoke:
        levels = levels[:1]
    cells = [] if args.skip_single else [
        dict(transport='tcp', mode=m, workers=1, concurrency=c, repeat=0) for m in modes for c in levels]
    cells += [dict(transport=t, mode=m, workers=2, concurrency=c, repeat=r)
              for r in range(repeats) for t in transports for m in modes for c in levels]
    rows = []
    for spec in cells:
        row = cell(args.out, workload=args.workload, rate=args.rate, warmup=warmup, seconds=seconds,
                   keepalive_requests=args.keepalive_requests, **spec)
        rows.append(row)
        print(json.dumps({k: row[k] for k in ('key', 'rps', 'share_ratio', 'p99_us', 'nginx_cores', 'errors')}
                         | {'connections': sorted(row['connections'].values())}), flush=True)
    # Every cell in the directory: runs with other rates or transports add to it.
    rows = [json.loads(path.read_text()) for path in sorted(args.out.glob('*.json')) if path.name != 'summary.json']
    summary = summarize(rows)
    (args.out/'summary.json').write_text(json.dumps(summary, indent=1))
    for line in summary:
        print(json.dumps(line), flush=True)


if __name__ == '__main__':
    main()
