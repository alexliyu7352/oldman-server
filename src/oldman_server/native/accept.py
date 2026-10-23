"""Turns at accepting for worker processes that share one listening socket
(the main process's TCP or unix socket, or one passed in).

Every worker's shard 0 accepts on the same socket. Left alone, the worker whose
shard is busy (polling rather than sleeping) wins nearly every new connection.
So the main process gives the workers a table of open-connection counts in
shared memory, and a worker's shard 0 accepts only while no other worker has
fewer (native/server.cc, accept_shared).

Plain Python: the main process creates the table without the extension.
"""
import ctypes

from oldman_server.native.cpus import worker_number

# Worker numbers below this take turns; any others accept whenever they can.
SLOTS = 64
# A 64-byte line per worker: open connections, heartbeat, padding (AcceptSlot).
_WORDS = 8


def counts(context):
    """A zeroed table in shared memory for the worker processes `context`
    starts (they receive it as they start)."""
    return context.RawArray(ctypes.c_uint64, SLOTS * _WORDS)


def turn(table):
    """ServerConfig fields that make this worker process take turns on a shared
    socket, or {} outside a worker process."""
    number = worker_number()
    if table is None or number is None or number >= SLOTS:
        return {}
    return dict(accept_counts=ctypes.addressof(table), accept_slots=SLOTS, accept_slot=number)
