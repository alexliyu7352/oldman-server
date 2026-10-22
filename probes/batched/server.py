"""Identical fixed-response policy; switch only decision-return batching."""
import argparse
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from probes.fixed.server import HybridService, comparison, uvloop
from oldman_server.native.engine import Config, Engine, ServerConfig


class BatchService(HybridService):
    def __init__(self, mode, cpus, requests, byte_limit):
        super().__init__(mode, cpus, requests, byte_limit)
        self.batch = mode == "seastar-batch"

    async def start(self, port, listener=None):
        def decide(req): return req.method == "GET" and req.route == "fixed"
        self.engine = await Engine.open(Config(shard_cpus=tuple(self.cpus[1:]),
            batch_decisions=self.batch,
            server=ServerConfig(http_port=port, max_requests=self.requests,
                max_bytes=self.byte_limit, decision_timeout_ms=5000)), decision_handler=decide)
        os.sched_setaffinity(0, {self.cpus[0]})

    def info(self):
        return super().info() | {"batch_decisions": self.batch}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["seastar", "seastar-batch"], required=True)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    args.requests, args.bytes = 1024, 64*1024*1024
    cpus = sorted(os.sched_getaffinity(0))
    comparison.SeastarService = BatchService
    uvloop.run(comparison.worker(args, cpus, args.requests, args.bytes))
