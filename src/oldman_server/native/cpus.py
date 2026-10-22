"""CPUs for the native host's threads: each serving process runs Python on one
CPU and its Seastar shards on others.

One process takes NATIVE_SHARD_CPUS, or the last NATIVE_SHARDS CPUs of its
affinity, for the shards. With Sanic's worker processes the main process
splits its CPUs into one group per worker (NATIVE_WORKER_CPUS: Python CPU
first, then the shard CPUs) and passes the groups to the workers.

Plain Python: the worker manager plans CPUs without loading the extension.
"""
import os

# One Python thread keeps about two shards busy (1 + 2 CPUs per process).
DEFAULT_SHARDS = 2
_WORKER_PREFIX = "Oldman-Server-"


def _available():
    return sorted(os.sched_getaffinity(0))


def parse_cpus(value):
    """A CPU list setting: a sequence of CPUs, "1,2", or one CPU (the
    environment loader turns "3" into the int 3). None or "" when unset."""
    if value is None or value == "":
        return None
    if isinstance(value, int):
        return (value,)
    if isinstance(value, str):
        value = [part for part in value.split(",") if part.strip()]
    return tuple(int(cpu) for cpu in value)


def _parse_groups(value):
    """NATIVE_WORKER_CPUS as groups: a list of CPU lists, or "0,1,2;3,4,5"."""
    if isinstance(value, str):
        value = [part.split(",") for part in value.split(";") if part.strip()]
    groups = [tuple(int(cpu) for cpu in group) for group in value]
    if not groups or any(len(group) < 2 for group in groups):
        raise ValueError("NATIVE_WORKER_CPUS needs groups of a Python CPU and at least one shard CPU")
    return groups


def shard_count(config, cpus, processes=1):
    """NATIVE_SHARDS, or up to DEFAULT_SHARDS as the CPUs per process allow."""
    return int(config.get("NATIVE_SHARDS") or max(1, min(DEFAULT_SHARDS, cpus // processes - 1)))


def fast_workers(config):
    """Workers for fast=True: as many processes of 1 + NATIVE_SHARDS CPUs as fit."""
    cpus = len(_available())
    return max(1, cpus // (1 + shard_count(config, cpus)))


def worker_groups(config, workers):
    """CPU groups for `workers` server processes (each its Python CPU, then its
    shard CPUs): NATIVE_WORKER_CPUS, or consecutive groups of this process's
    CPUs. Workers beyond the groups share them (worker n uses group n % len)."""
    if parse_cpus(config.get("NATIVE_SHARD_CPUS")) is not None:
        raise ValueError("NATIVE_SHARD_CPUS names one process's shard CPUs; "
                         "with worker processes use NATIVE_SHARDS or NATIVE_WORKER_CPUS")
    if config.get("NATIVE_WORKER_CPUS"):
        return _parse_groups(config.NATIVE_WORKER_CPUS)
    cpus = _available()
    size = 1 + shard_count(config, len(cpus), workers)
    if len(cpus) < size:
        # Too few CPUs for one group: Python shares with the shards.
        return [(cpus[0], *cpus[-max(1, len(cpus) - 1):])]
    return [tuple(cpus[start:start + size]) for start in range(0, len(cpus) - size + 1, size)]


def worker_number():
    """This server worker process's number, or None outside one: the lowest
    number free among the server workers when the manager created it
    (OLDMAN_SERVER_WORKER_SLOT), so 0..n-1 for n workers through scaling; the
    n of its name Oldman-Server-n-m when started without one."""
    name = os.environ.get("OLDMAN_SERVER_WORKER_NAME", "")
    if not name.startswith(_WORKER_PREFIX):
        return None
    slot = os.environ.get("OLDMAN_SERVER_WORKER_SLOT")
    if slot is not None:
        return int(slot)
    return int(name[len(_WORKER_PREFIX):].split("-")[0])


def worker_group(config):
    """This server worker process's CPU group, or None outside one (or when the
    main process planned none)."""
    planned = config.get("NATIVE_WORKER_CPUS")
    number = worker_number()
    if not planned or number is None:
        return None
    groups = _parse_groups(planned)
    return groups[number % len(groups)]
