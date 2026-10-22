"""One ordinary GIL Python process. POST /python/compress?level=6.

source scripts/env.sh
python examples/async_business.py
"""
import asyncio
import os
import uvloop
from probe_engine import Config, Engine, HttpResponse, ServerConfig


async def business(request):
    try:
        level = int(request.parameter or "6")
    except ValueError:
        return HttpResponse(status=400)
    if not 0 <= level <= 9:
        return HttpResponse(status=400)
    # The request body stays in C++; the await resumes this Python coroutine.
    compressed = await request.compress(level=level)
    bound = request.body_size + (request.body_size >> 12) + (request.body_size >> 14) + 13
    if compressed.size > bound:
        return HttpResponse(status=422)
    return HttpResponse(body=compressed, tag=str(compressed.size))


async def main():
    cpus = tuple(sorted(os.sched_getaffinity(0))[:3])
    engine = await Engine.open(Config(shard_cpus=cpus[:1], worker_cpus=cpus,
        share_worker_cpus=True, batch_decisions=True, server=ServerConfig(http_port=8080,
            max_requests=128, max_bytes=256*1024*1024,
            decision_timeout_ms=5000, coalesce_notifications=True)), http_handler=business)
    os.sched_setaffinity(0, {cpus[0]})
    print(f"PID {os.getpid()}, CPUs {cpus}; POST http://127.0.0.1:8080/python/compress?level=6", flush=True)
    try:
        await asyncio.Event().wait()
    finally:
        await engine.aclose()


if __name__ == "__main__":
    try:
        uvloop.run(main())
    except KeyboardInterrupt:
        pass
