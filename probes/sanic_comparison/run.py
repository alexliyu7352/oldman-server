"""Same CPU budget for both services: three CPUs (one Python thread and two
shards against three stock workers) or four (candidate worker processes against
four stock workers); and a pure Seastar reference on three."""
import argparse
import hashlib
import itertools
import json
import math
import os
from pathlib import Path
import pstats
import socket
import subprocess
import sys
import time
import traceback

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT/'scripts'))
from probes.sanic_boundary import run as boundary
from probes.sanic_boundary.run import atomic_json, read_response
from probes.sanic_boundary.application import EXPECTED, INPUT, concurrency
from probes.sanic_comparison.server import verify_stock, profile_records
from probes.application_lifecycle.inputs import runtime_fingerprint, process_identity, check_identity
from probes.sanic_spike.run import free_port
from scripts.cpu_budget import _cpus, describe_cpu, cpu_snapshot, require_unthrottled
from run_http_benchmarks import read_json, read_line, send
from run_comparison import stop_process, process_sample

MODES = ('candidate', 'stock', 'native')


def fingerprints():
    source = boundary.fingerprints()
    stock = verify_stock()
    paths = [ROOT/'CMakeLists.txt', ROOT/'build/sanic_baseline_native',
             *sorted((ROOT/'probes/sanic_comparison').glob('*.py')),
             ROOT/'probes/sanic_comparison/native.cc', *sorted((stock/'sanic').rglob('*.py'))]
    for p in paths:
        source[str(p.relative_to(ROOT))] = hashlib.sha256(p.read_bytes()).hexdigest()
    # Resolve the standalone reference's shared libraries in this exact env.
    import re
    for filename in re.findall(r'(/[^\s]+)\s+\(', subprocess.check_output(
            ['ldd', str(ROOT/'build/sanic_baseline_native')], text=True)):
        p = Path(filename).resolve(); source[str(p)] = hashlib.sha256(p.read_bytes()).hexdigest()
    return source


def validate_response(status, headers, body, pids, workload):
    if (status != 200 or body != EXPECTED[workload] or
            headers.get('x-probe-pid') not in {str(pid) for pid in pids} or
            headers.get('content-type') != ('application/octet-stream' if workload == 'body' else 'application/json') or
            headers.get('content-encoding') or headers.get('connection', '').lower() == 'close'):
        raise ValueError('response semantics mismatch')
    if headers.get('transfer-encoding') == 'chunked' and 'content-length' not in headers:
        return 'chunked'
    if headers.get('content-length') == str(len(body)) and 'transfer-encoding' not in headers:
        return 'content-length'
    raise ValueError('invalid response framing')


def preflight(port, pids, workload):
    body = INPUT if workload == 'body' else b''
    request = (f"{'POST' if body else 'GET'} /{workload} HTTP/1.1\r\nHost: 127.0.0.1\r\n"
        f'Content-Type: application/octet-stream\r\nContent-Length: {len(body)}\r\nConnection: keep-alive\r\n\r\n').encode()+body
    responses = []
    with socket.create_connection(('127.0.0.1', port), timeout=5) as sock:
        with sock.makefile('rb') as stream:
            for _ in range(2):
                sock.sendall(request)
                status, headers, actual = read_response(stream)
                framing = validate_response(status, headers, actual, pids, workload)
                responses.append({'status': status, 'headers': headers, 'body_hex': actual.hex(), 'framing': framing})
    return responses


def validate_counts(row):
    client = row['client']; n = client['requests']; pids = row['ready']['pids']
    if row['returncodes'] != [0, 0] or n <= 0:
        raise ValueError('failed process or empty workload')
    for key in ('errors', 'status_errors', 'integrity_errors', 'dropped', 'new_connections'):
        if client[key]: raise ValueError(f'client {key}')
    for key in ('seconds', 'rps', 'p99_us'):
        if not math.isfinite(client[key]) or client[key] <= 0: raise ValueError('invalid timing')
    rate = row.get('rate', 0)
    if rate and client['scheduled'] != math.ceil(rate*row['seconds']-1e-8):
        raise ValueError('offered load mismatch')
    if (client['arrival_seconds'] != row['seconds'] or client['rate'] != rate or
            not row['seconds'] <= client['seconds'] <= row['seconds']+10 or
            client['scheduled'] != n or client['total_connections'] != row['concurrency'] or
            client['response_bytes'] != n*len(EXPECTED[row['workload']]) or
            set(client['server_pids']) != {str(pid) for pid in pids} or
            sum(client['server_pids'].values()) != n):
        raise ValueError('workload/PID/byte count mismatch')
    workers = row['server']['workers']
    if len(workers) != len(pids) or [w['pid'] for w in workers] != pids:
        raise ValueError('missing/mismatched worker stats')
    for w in workers:
        count = client['server_pids'][str(w['pid'])]
        if not count or any(w['delta'][key] != count for key in ('started', 'finished', 'responses')):
            raise ValueError('worker response counts differ')
    if not row['closed']['closed'] or len(row['closed']['workers']) != len(pids):
        raise ValueError('missing worker shutdown')
    for stats, closed in [(w['after'], False) for w in workers] + [(w, True) for w in row['closed']['workers']]:
        if stats['active'] or stats['errors']:
            raise ValueError('active work or errors remain')
        if row['mode'] == 'candidate':
            # At the end of the window the shards may still be reporting
            # connections the client is closing (queued application_closed
            # events); requests in flight show in the other counters.
            keys = boundary.ZERO if closed else [k for k in boundary.ZERO if k != 'queued_decisions']
            if any(stats['engine_'+k] for k in keys) or stats['engine_python_application_errors']:
                raise ValueError('native resources or Python errors remain')
    if row['mode'] == 'candidate':
        for w in workers:
            count = client['server_pids'][str(w['pid'])]
            if any(w['delta']['engine_'+k] != count for k in
                   ('responses', 'python_application_started', 'python_application_finished')):
                raise ValueError('native/application counts differ')
    if not math.isfinite(row['service_cpu_seconds']) or row['service_cpu_seconds'] <= 0:
        raise ValueError('invalid service CPU')


def validate_profiles(row, out):
    for worker in row['server']['workers']:
        profile = worker.get('profile')
        if row.get('diagnostic', 'none') == 'none':
            if profile is not None:
                raise ValueError('profile in uninstrumented window')
            continue
        if (not profile or profile.get('pid') != worker['pid'] or
                profile.get('timer') != 'thread_time' or worker.get('thread_cpu_ns', 0) <= 0 or
                profile.get('file') != f'profile-{worker["pid"]}.pstats'):
            raise ValueError('missing or invalid worker profile')
        file = out/profile['file']
        if not file.is_file() or hashlib.sha256(file.read_bytes()).hexdigest() != profile.get('sha256'):
            raise ValueError('profile file/hash mismatch')
        if not profile.get('calls') or profile['calls'] != profile_records(pstats.Stats(str(file))):
            raise ValueError('profile calls differ from raw file')
        for call in profile['calls']:
            if any(not math.isfinite(call[k]) or call[k] < 0 for k in
                   ('self_thread_cpu_seconds', 'cumulative_thread_cpu_seconds', 'calls', 'primitive_calls')):
                raise ValueError('invalid profile counters')


def verify_ready(ready, mode, server, layout, runtime, workers=1):
    pids = ready['pids']
    processes = len(layout['server']) if mode == 'stock' else workers
    if (not ready['ready'] or len(pids) != processes or
            set(ready['service_pids']) != set(pids) | {server.pid}):
        raise ValueError('incorrect service process set')
    if mode != 'stock' and workers == 1 and pids != [server.pid]:
        raise ValueError('unexpected service PID')
    if mode != 'native':
        # The framework in the service: the verified upstream source for
        # stock, the oldman_server package otherwise.
        package = (verify_stock()/'sanic' if mode == 'stock' else boundary.PACKAGE)/'__init__.py'
        for worker in ready['workers']:
            if (worker['runtime'] != runtime['state'] or 'uvloop.Loop' not in worker['event_loop'] or
                worker['sanic_version'] != ('25.12.1' if mode == 'stock' else boundary.package_version()) or
                worker['sanic_file'] != str(package) or
                worker['sanic_sha256'] != hashlib.sha256(package.read_bytes()).hexdigest()):
                raise ValueError('loaded Sanic/runtime mismatch')
            if mode == 'candidate':
                boundary.verify_modules(worker['loaded'])
    if [w['pid'] for w in ready['workers']] != pids:
        raise ValueError('worker metadata count/order mismatch')
    for index, worker in enumerate(ready['workers']):
        if mode == 'stock':
            if worker['main_cpu'] != layout['server'][index] or worker['shards']:
                raise ValueError('stock worker layout mismatch')
            boundary.check_threads(worker['pid'], [layout['server'][index]])
        else:
            # Candidate worker `index`: its share of the server CPUs, Python first.
            size = len(layout['server']) // workers
            group = layout['server'][index * size:(index + 1) * size]
            if mode == 'candidate' and (worker['main_cpu'] != group[0] or
                    os.sched_getaffinity(worker['pid']) != {group[0]}):
                raise ValueError('candidate main thread affinity mismatch')
            cpus = group[1:] if mode == 'candidate' else layout['server']
            if len(worker['shards']) != len(cpus): raise ValueError('shard count mismatch')
            for shard, cpu in zip(worker['shards'], cpus, strict=True):
                if os.sched_getaffinity(shard['tid']) != {cpu}: raise ValueError('shard affinity mismatch')


def cpu_delta(before, after):
    old = {r['pid']: r['cpu_ticks'] for r in before['processes']}
    if set(old) != {r['pid'] for r in after['processes']}: raise ValueError('changed service PID set')
    deltas = [r['cpu_ticks']-old[r['pid']] for r in after['processes']]
    if any(n < 0 for n in deltas): raise ValueError('CPU counter reset')
    return sum(deltas)/os.sysconf('SC_CLK_TCK')


def busy_ticks():
    """Per-CPU user, nice, system and steal ticks. Interrupt time is left out:
    it includes the window's own loopback traffic."""
    busy = {}
    for line in Path('/proc/stat').read_text().splitlines():
        name, *values = line.split()
        if name[:3] == 'cpu' and name[3:].isdigit():
            user, nice, system, _idle, _iowait, _irq, _softirq, steal = map(int, values[:8])
            busy[int(name[3:])] = user + nice + system + steal
    return busy


def foreign_cores(before, after, cpus, own_seconds, seconds):
    """Cores other processes used on cpus and their SMT siblings during a window
    (own_seconds: the CPU time of the processes measured there): the CPUs are
    pinned but not reserved, and the desktop's other work slows the window (a
    sibling shares its core) without showing in the service's own CPU."""
    cpus = set().union(*(_cpus(Path(f'/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list')
                               .read_text().strip()) for cpu in cpus))
    busy = sum(after[cpu] - before[cpu] for cpu in cpus) / os.sysconf('SC_CLK_TCK')
    return round(max(busy - own_seconds, 0) / seconds, 3)


def pressure_totals():
    """System-wide stall time (µs) from PSI: some task waiting for CPU, memory or IO.
    Other work on the desktop (a filesystem-wide search, say) slows windows
    without using their CPUs; IO pressure is how it shows."""
    return {resource: int(Path(f'/proc/pressure/{resource}').read_text().splitlines()[0].rsplit('total=', 1)[1])
            for resource in ('cpu', 'memory', 'io')}


def execute(out, mode, workload, *, layout, budget, runtime, warmup, seconds, rate=0,
            diagnostic='none', workers=1, scale=1):
    if diagnostic not in ('none', 'cprofile') or not math.isfinite(rate) or rate < 0:
        raise ValueError('invalid diagnostic/rate')
    if mode not in MODES:
        raise ValueError('unsupported service/workload')
    if workers != 1 and mode != 'candidate':
        raise ValueError('worker processes are a candidate setting')
    if diagnostic == 'cprofile' and (mode not in ('candidate', 'stock') or rate <= 0):
        raise ValueError('profiling requires a Python service and a positive offered rate')
    out.mkdir(parents=True, exist_ok=False)
    (out/'expected.bin').write_bytes(EXPECTED[workload]); (out/'input.bin').write_bytes(INPUT)
    port = free_port()
    # Both services of a comparison get the same client: `scale` times the
    # single-process connections (connection_scale).
    row = dict(mode=mode, workload=workload, warmup=warmup, seconds=seconds, rate=rate, diagnostic=diagnostic,
               concurrency=concurrency(workload) * scale, workers=workers,
               layout=layout, commands=[], validated=False)
    processes, logs = [], []
    def start(command, role):
        command = ['taskset', '-c', ','.join(map(str,layout[role]))]+command
        row['commands'].append(command)
        log = (out/f'{role}.log').open('wb'); logs.append(log)
        child = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log,
            bufsize=0, start_new_session=True, env=os.environ | {'UV_USE_IO_URING':'0'})
        processes.append(child); return child
    try:
        command = ([str(ROOT/'build/sanic_baseline_native'),str(port),','.join(map(str,layout['server']))]
            if mode == 'native' else [sys.executable,str(Path(__file__).with_name('server.py')),
            '--mode',mode,'--port',str(port),'--workers',str(workers)])
        if diagnostic == 'cprofile': command += ['--profile-dir', str(out.resolve())]
        server = start(command, 'server'); ready = row['ready'] = read_json(server)
        verify_ready(ready,mode,server,layout,runtime,workers)
        identities = [process_identity(pid, os.getpid() if pid == server.pid else server.pid,
            ROOT/'build/sanic_baseline_native' if mode == 'native' else Path(sys.executable))
            for pid in ready['service_pids']]
        row['identities'] = identities
        row['preflight'] = preflight(port,ready['pids'],workload)
        cpus = ','.join(map(str,layout['client']))
        client = start([str(ROOT/'build/http_load'),'--smp','3','--cpuset',cpus,'--thread-affinity','1',
            '--reactor-backend','epoll','--overprovisioned','--default-log-level','warn',
            '--enable-glibc-exception-scaling-workaround','0','--port',str(port),
            '--target-pids',','.join(map(str,ready['pids'])),'--path','/'+workload,
            '--method','POST' if workload == 'body' else 'GET','--input',str(out/'input.bin'),
            '--expected',str(out/'expected.bin'),'--concurrency',str(row['concurrency']),'--rate',str(rate),
            '--warmup',str(warmup),'--seconds',str(seconds)], 'client')
        while read_line(client) != 'MEASURE_READY': pass
        row['threads_before'] = {str(pid):boundary.check_threads(pid,layout['server']) for pid in ready['service_pids']}
        row['client_threads'] = boundary.check_threads(client.pid,layout['client'])
        busy_before, pressure_before, busy_started = busy_ticks(), pressure_totals(), time.monotonic()
        row['cpu_before'] = cpu_snapshot()
        if any(budget[k] != row['cpu_before'][k] for k in ('cpu','cpuset')): raise ValueError('budget changed')
        send(server,'begin'); assert read_json(server)['begun']
        row['before'] = process_sample(ready['service_pids'])
        send(client,'start')
        while read_line(client,seconds+20) != 'MEASURE_DONE': pass
        row['after'] = process_sample(ready['service_pids'])
        for identity in identities: check_identity(identity)
        row['threads_after'] = {str(pid):boundary.check_threads(pid,layout['server']) for pid in ready['service_pids']}
        busy_after, pressure_after, elapsed = busy_ticks(), pressure_totals(), time.monotonic() - busy_started
        send(server,'end'); row['server'] = read_json(server)
        row['client'] = read_json(client)
        row['foreign_cores'] = {
            'server': foreign_cores(busy_before, busy_after, layout['server'],
                                    cpu_delta(row['before'], row['after']), elapsed),
            'client': foreign_cores(busy_before, busy_after, layout['client'], row['client']['cpu_seconds'], elapsed)}
        # Share of the window in which some task on the machine stalled.
        row['pressure'] = {resource: round((pressure_after[resource] - pressure_before[resource]) / (elapsed * 1e6), 3)
                           for resource in pressure_after}
        for key in ('cpu_seconds', 'cpu_cores'):
            if not math.isfinite(row['client'][key]) or row['client'][key] <= 0:
                raise ValueError('invalid client CPU')
        row['cpu_after'] = cpu_snapshot()
        send(server,'quit'); row['closed'] = read_json(server)
        row['returncodes'] = [p.wait(15) for p in (server,client)]
        row['service_cpu_seconds'] = cpu_delta(row['before'],row['after'])
        row['service_cpu_us_per_request'] = row['service_cpu_seconds']*1e6/row['client']['requests']
        atomic_json(out/'row.json',row)
        require_unthrottled(row['cpu_before'],row['cpu_after']); validate_counts(row)
        validate_profiles(row, out)
        row['validated'] = True; atomic_json(out/'row.json',row)
        return row
    except BaseException:
        row['failure'] = traceback.format_exc(); atomic_json(out/'row.json',row); raise
    finally:
        for process in reversed(processes): stop_process(process)
        for log in logs: log.close()


def case_order(*, python_only=False, smoke=False):
    order = []
    for workload in EXPECTED:
        if not smoke:
            order.extend((f'{workload}-aa-{i}','candidate',workload) for i in range(2))
        if python_only:
            permutations = [('candidate', 'stock')] if smoke else [
                ('candidate', 'stock'), ('stock', 'candidate')] * 2
        else:
            permutations = [MODES] if smoke else itertools.permutations(MODES)
        for repeat,modes in enumerate(permutations):
            order.extend((f'{workload}-r{repeat}-{mode}',mode,workload) for mode in modes)
    return order


def connection_scale(server_cores):
    """The client's connections, in single-process units: twice them on four
    server cores (the multi-worker comparison), whatever the candidate's
    worker count, so that its layouts meet the same load."""
    return 2 if server_cores == 4 else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out',type=Path,required=True)
    parser.add_argument('--smoke',action='store_true')
    parser.add_argument('--python-only',action='store_true',
                        help='compare candidate and stock in four alternating pairs per workload')
    parser.add_argument('--server-cores',type=int,default=3,choices=(3,4),
                        help='server CPUs; stock runs one worker on each')
    parser.add_argument('--workers',type=int,default=1,
                        help='candidate worker processes, splitting the server CPUs evenly (one CPU '
                             'each: the same-thread mode)')
    args = parser.parse_args(); out = args.out.resolve(); out.mkdir(parents=True,exist_ok=False)
    if args.workers != 1 and not args.python_only:
        raise SystemExit('candidate workers compare against stock only (--python-only)')
    budget = describe_cpu(); layout,topology = boundary.layout_for(budget, args.server_cores)
    source, runtime = fingerprints(),runtime_fingerprint()
    order = case_order(python_only=args.python_only, smoke=args.smoke)
    manifest = {'source':source,'runtime':runtime,'cpu_budget':budget,'layout':layout,'topology':topology,
        'order':order,'smoke':args.smoke,'python_only':args.python_only,'created_ns':time.time_ns(),
        'server_cores':args.server_cores,'workers':args.workers,
        'git_head':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
        'os_release':Path('/etc/os-release').read_text(),'kernel':os.uname().release}
    atomic_json(out/'manifest.json',manifest)
    original = os.sched_getaffinity(0); os.sched_setaffinity(0,set(layout['controller']))
    try:
        for key,mode,workload in order:
            row = execute(out/key,mode,workload,layout=layout,budget=budget,runtime=runtime,
                warmup=.1 if args.smoke else 2,seconds=.25 if args.smoke else 5,
                workers=args.workers if mode == 'candidate' else 1,scale=connection_scale(args.server_cores))
            print(json.dumps({'key':key,'rps':row['client']['rps'],
                  'cpu_us':row['service_cpu_us_per_request'],'foreign_cores':row['foreign_cores'],
                  'pressure':row['pressure']}),flush=True)
        if source != fingerprints() or runtime != runtime_fingerprint(): raise ValueError('inputs changed')
        atomic_json(out/'complete.json',{'windows':len(order),'fingerprints_unchanged':True})
    finally:
        os.sched_setaffinity(0,original)


if __name__ == '__main__': main()
