"""Worker-local counters; process/PSS aggregation lives in the controller."""
import asyncio
import os
import time

from benchmark_work import distribution_ns, threads


class Snapshot:
    def __init__(self, stats):
        self.stats = stats
        self.window = None
        self.lag = []
        self.beat = asyncio.create_task(self.heartbeat())

    async def heartbeat(self):
        while True:
            token = self.window
            due = time.monotonic_ns() + 1_000_000
            await asyncio.sleep(.001)
            if token is not None and self.window is token:
                self.lag.append(max(0, time.monotonic_ns()-due))

    def begin(self):
        self.lag.clear()
        self.window = {"stats": self.stats(), "threads": threads(),
                       "cpu": time.process_time(), "time": time.monotonic()}

    def end(self):
        finish, cpu = time.monotonic(), time.process_time()
        before, self.window = self.window, None
        if before is None:
            raise RuntimeError("end without begin")
        elapsed = finish - before["time"]
        after = self.stats()
        old = {r["tid"]: r for r in before["threads"]}
        rows = threads()
        for row in rows:
            row["cpu_seconds"] = (row["cpu_ticks"]-old.get(row["tid"], {}).get("cpu_ticks", 0))/os.sysconf("SC_CLK_TCK")
            row["cpu_cores"] = row["cpu_seconds"]/elapsed
        return {"pid": os.getpid(), "seconds": elapsed, "cpu_seconds": cpu-before["cpu"],
                "cpu_cores": (cpu-before["cpu"])/elapsed, "threads": rows, "after": after,
                "delta": {k: v-before["stats"][k] for k, v in after.items() if isinstance(v, int)},
                "loop_lag": distribution_ns(self.lag), "lag_ns": self.lag}

    async def close(self):
        self.beat.cancel()
        await asyncio.gather(self.beat, return_exceptions=True)
