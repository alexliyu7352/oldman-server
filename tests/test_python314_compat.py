"""3.14 migration gate. The native cases share the process's persistent
runtime (a process starts one runtime), as the framework host does.

Strict xfails expose two upstream uvloop API gaps. The framework's real call
paths below must pass on both loops; xfails do not certify full asyncio parity.
"""
import asyncio
import contextvars
import json

import pytest
import uvloop

from oldman_server.native import engine as api
from test_http import config, request, wait_stat


def run_on(loop_name, scenario):
    factory = asyncio.new_event_loop if loop_name == "asyncio" else uvloop.new_event_loop
    with asyncio.Runner(loop_factory=factory) as runner:
        return runner.run(scenario())


@pytest.mark.parametrize("loop_name", ["asyncio", pytest.param("uvloop", marks=pytest.mark.xfail(
    strict=True, raises=TypeError,
    reason="uvloop 0.22.1 create_task rejects eager_start and arbitrary factory kwargs"))])
def test_task_factory_receives_python314_keywords(loop_name):
    async def scenario():
        loop = asyncio.get_running_loop()
        marker = contextvars.ContextVar("task-context", default="outside")
        context = contextvars.copy_context()
        context.run(marker.set, "inside")
        observed = []

        def factory(loop, coroutine, **kwargs):
            observed.append(dict(kwargs))
            kwargs.pop("custom_marker")
            return asyncio.eager_task_factory(loop, coroutine, **kwargs)

        async def child():
            await asyncio.sleep(0)
            return marker.get()

        loop.set_task_factory(factory)
        coroutine = child()
        try:
            try:
                task = loop.create_task(coroutine, name="named-child", context=context,
                                        eager_start=False, custom_marker="forwarded")
            except BaseException:
                coroutine.close()
                raise
            assert await task == "inside"
            assert task.get_name() == "named-child"
            assert observed == [{"name": "named-child", "context": context,
                                 "eager_start": False, "custom_marker": "forwarded"}]
            assert marker.get() == "outside"
        finally:
            loop.set_task_factory(None)

    run_on(loop_name, scenario)


@pytest.mark.parametrize("loop_name", ["asyncio", pytest.param("uvloop", marks=pytest.mark.xfail(
    strict=True, raises=AssertionError,
    reason="uvloop 0.22.1 sets Task.name after the factory instead of forwarding name"))])
def test_task_factory_receives_name(loop_name):
    async def scenario():
        loop = asyncio.get_running_loop()
        observed = []

        def factory(loop, coroutine, **kwargs):
            observed.append(kwargs.get("name"))
            return asyncio.Task(coroutine, loop=loop, **kwargs)

        loop.set_task_factory(factory)
        try:
            task = loop.create_task(asyncio.sleep(0), name="visible-to-factory")
            await task
            assert task.get_name() == "visible-to-factory"
            assert observed == ["visible-to-factory"]
        finally:
            loop.set_task_factory(None)

    run_on(loop_name, scenario)


@pytest.mark.parametrize("loop_name", ["asyncio", "uvloop"])
def test_real_handler_factory_context_and_nested_timeout(loop_name):
    async def scenario():
        loop = asyncio.get_running_loop()
        marker = contextvars.ContextVar("handler-context", default="outside")
        token = marker.set("registered")
        application_tasks = []

        def factory(loop, coroutine, **kwargs):
            task = asyncio.Task(coroutine, loop=loop, **kwargs)
            if coroutine.cr_code.co_name == "_serve_application":
                task.set_name("application-task")
                application_tasks.append(task)
            return task

        async def handler(req):
            task = asyncio.current_task()
            assert task in application_tasks
            assert task.get_name() == "application-task"
            assert marker.get() == "registered"
            context = contextvars.copy_context()
            context.run(marker.set, req.query_string)

            async def child():
                await asyncio.sleep(0)
                return marker.get(), asyncio.current_task().get_name()

            nested = loop.create_task(child(), name="business-child", context=context)
            value, name = await nested
            # The framework already has an outer decision timeout. An inner
            # timeout must uncancel the same real Task and permit a valid reply.
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(.01):
                    await asyncio.Event().wait()
            assert asyncio.current_task() is task and task.cancelling() == 0
            assert marker.get() == "registered"
            return 200, json.dumps({"value": value, "name": name}).encode(), ()

        cfg = config(application_dispatch_all=True, decision_timeout_ms=2000)
        engine = await api.Engine.open(cfg, application_handler=handler, persistent=True)
        loop.set_task_factory(factory)
        try:
            for value in ("alpha", "beta"):
                status, _, body = await request(cfg.server.http_port, f"/nested?{value}")
                assert status == 200
                assert json.loads(body) == {"value": value, "name": "business-child"}
            await wait_stat(engine, "python_application_active", lambda n: n == 0)
            assert len(application_tasks) == 2
            assert all(task.done() and not task.cancelled() for task in application_tasks)
            assert engine.server_stats()["python_application_errors"] == 0
        finally:
            loop.set_task_factory(None)
            await engine.aclose()
            marker.reset(token)
        stats = engine.server_stats()
        assert stats["active_requests"] == stats["reserved_bytes"] == 0

    run_on(loop_name, scenario)
