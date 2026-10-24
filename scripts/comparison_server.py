"""Worker processes of the comparison server: each runs the service the
driver supplies and answers the controller's begin / end / quit commands.

The driver (probes/sanic_comparison/server.py) sets Service, the service a
worker runs, and Snapshot, the counter window it reports. Only the
controller pipe carries IPC in prefork mode; HTTP bodies never cross it.
"""
import asyncio
import json
import multiprocessing as mp
import os
import sys

Service = Snapshot = None


async def worker(args, cpus, requests, byte_limit, control=None, listener=None):
    service = Service(args.mode, cpus, requests, byte_limit)
    await service.start(args.port, listener)
    snapshot = Snapshot(service.stats)
    loop = asyncio.get_running_loop()
    transport = None
    if control is None:
        reader = asyncio.StreamReader()
        transport, _ = await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), sys.stdin)
        def emit(value): print(json.dumps(value), flush=True)
        async def command(): return (await reader.readline()).decode().strip()
        emit({"ready": True, "pids": [os.getpid()], "service_pids": [os.getpid()],
              "workers": [service.info()], "server_cpus": cpus, "mode": args.mode})
    else:
        queue = asyncio.Queue()
        def receive():
            try: queue.put_nowait(control.recv())
            except EOFError: queue.put_nowait("quit"); loop.remove_reader(control.fileno())
        loop.add_reader(control.fileno(), receive)
        emit = control.send
        command = queue.get
        emit(service.info())
    try:
        while cmd := await command():
            if cmd == "begin":
                snapshot.begin(); emit({"begun": True})
            elif cmd == "end":
                emit({"workers": [snapshot.end()]})
            elif cmd == "quit":
                break
            else:
                raise ValueError(cmd)
    finally:
        await snapshot.close()
        await service.close()
        emit({"closed": True, "workers": [service.stats()]})
        if transport: transport.close()
        if control:
            loop.remove_reader(control.fileno()); control.close()


def child(args, cpu, requests, byte_limit, connection, inherited):
    # Each child closes all parent pipe endpoints inherited from earlier forks.
    for pipe in inherited: pipe.close()
    asyncio.run(worker(args, [cpu], requests, byte_limit, connection))


def finish_workers(children, timeout=5):
    """Reap every owned worker; a close snapshot is not proof of a clean exit."""
    outcomes = []
    for process in children:
        process.join(timeout)
        forced = process.is_alive()
        if forced:
            process.terminate(); process.join(3)
        if process.is_alive():
            process.kill(); process.join()
        outcomes.append({"pid": process.pid, "exitcode": process.exitcode, "forced": forced})
    return outcomes


def prefork(args, cpus):
    """One child per entry of cpus: a CPU, or a list of CPUs for a child that
    serves through the native host (its Python CPU first)."""
    ctx = mp.get_context("fork")
    children, pipes = [], []
    try:
        for i, cpu in enumerate(cpus):
            parent, connection = ctx.Pipe()
            process = ctx.Process(target=child, args=(args, cpu,
                args.requests//len(cpus)+(i < args.requests % len(cpus)),
                args.bytes//len(cpus)+(i < args.bytes % len(cpus)), connection, [*pipes, parent]))
            process.start(); connection.close(); children.append(process); pipes.append(parent)
        os.sched_setaffinity(0, {cpus[0] if isinstance(cpus[0], int) else cpus[0][0]})
        infos = [p.recv() for p in pipes]
        print(json.dumps({"ready": True, "pids": [p.pid for p in children],
            "service_pids": [os.getpid(), *(p.pid for p in children)], "workers": infos,
            "server_cpus": cpus, "mode": args.mode}), flush=True)
        for line in sys.stdin:
            cmd = line.strip()
            for p in pipes: p.send(cmd)
            replies = [p.recv() for p in pipes]
            result = {"begun": True} if cmd == "begin" else {"workers": [w for r in replies for w in r["workers"]]}
            if cmd == "quit":
                result["worker_exits"] = finish_workers(children)
                result["closed"] = all(row["exitcode"] == 0 and not row["forced"]
                                       for row in result["worker_exits"])
            print(json.dumps(result), flush=True)
            if cmd == "quit":
                if not result["closed"]:
                    raise RuntimeError("benchmark worker did not exit cleanly")
                break
    finally:
        finish_workers(children, timeout=1)
        for p in pipes: p.close()

