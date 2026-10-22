"""Python configures native operations; /policy requests do not call Python.

source scripts/env.sh
python examples/native_pipeline.py --cpus 0,1,2 --file /path/to/asset.bin
curl http://127.0.0.1:8080/policy/file -o downloaded.bin
"""
import argparse
import asyncio
import os

import uvloop
from probe_engine import Config, Engine, NativePolicy, ServerConfig


async def serve(args):
    engine = await Engine.open(Config(shard_cpus=tuple(map(int, args.cpus.split(","))),
        batch_decisions=True, server=ServerConfig(http_port=args.port, file_path=args.file)))
    try:
        version = await engine.install_policy(NativePolicy(allow_fixed=True, allow_file=True,
            max_payload_bytes=max(1024, os.path.getsize(args.file))))
        print(f"PID {os.getpid()}, policy version {version}, HTTP port {args.port}", flush=True)
        # Application code may await another install_policy() to change permissions.
        # The await acknowledges every shard; existing requests keep their version.
        await asyncio.Event().wait()
    finally:
        await engine.aclose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cpus", required=True)
    parser.add_argument("--file", required=True)
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    try:
        uvloop.run(serve(args))
    except KeyboardInterrupt:
        pass
