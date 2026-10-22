import asyncio

from asyncio.tasks import Task
from unittest.mock import Mock, call

import pytest

from oldman_server.app import OldmanServer
from oldman_server.application.state import ApplicationServerInfo, ServerStage
from oldman_server.response import empty


try:
    from unittest.mock import AsyncMock
except ImportError:
    from tests.framework.asyncmock import AsyncMock  # type: ignore

pytestmark = pytest.mark.asyncio


async def dummy(n=0):
    for _ in range(n):
        await asyncio.sleep(1)
    return True


@pytest.fixture(autouse=True)
def mark_app_running(app: OldmanServer):
    app.state.server_info.append(
        ApplicationServerInfo(
            stage=ServerStage.SERVING, settings={}, server=AsyncMock()
        )
    )


async def test_add_task_returns_task(app: OldmanServer):
    task = app.add_task(dummy())

    assert isinstance(task, Task)
    assert len(app._task_registry) == 0


async def test_add_task_with_name(app: OldmanServer):
    task = app.add_task(dummy(), name="dummy")

    assert isinstance(task, Task)
    assert len(app._task_registry) == 1
    assert task is app.get_task("dummy")

    for task in app.tasks:
        assert task in app._task_registry.values()


async def test_cancel_task(app: OldmanServer):
    task = app.add_task(dummy(3), name="dummy")

    assert task
    assert not task.done()
    assert not task.cancelled()

    await asyncio.sleep(0.1)

    assert not task.done()
    assert not task.cancelled()

    await app.cancel_task("dummy")

    assert task.cancelled()


async def test_purge_tasks(app: OldmanServer):
    app.add_task(dummy(3), name="dummy")

    await app.cancel_task("dummy")

    assert len(app._task_registry) == 1

    app.purge_tasks()

    assert len(app._task_registry) == 0


async def test_purge_tasks_with_create_task(app: OldmanServer):
    app.add_task(asyncio.create_task(dummy(3)), name="dummy")

    await app.cancel_task("dummy")

    assert len(app._task_registry) == 1

    app.purge_tasks()

    assert len(app._task_registry) == 0


def test_shutdown_tasks_on_app_stop():
    class TestOldmanServer(OldmanServer):
        shutdown_tasks = Mock()

    app = TestOldmanServer("Test")

    @app.route("/")
    async def handler(_):
        return empty()

    app.test_client.get("/")

    app.shutdown_tasks.call_args == [
        call(timeout=0),
        call(15.0),
    ]


async def test_task_result_is_preserved(app: OldmanServer):
    async def return_value(x):
        await asyncio.sleep(0)
        return x

    task = app.add_task(return_value(42), name="return_42")

    result = await task

    assert result == 42
    assert task.result() == 42


async def test_task_result_with_callable(app: OldmanServer):
    async def coro_with_app(app):
        await asyncio.sleep(0)
        return app.name

    task = app.add_task(coro_with_app, name="return_app_name")

    result = await task

    assert result == app.name
    assert task.result() == app.name
