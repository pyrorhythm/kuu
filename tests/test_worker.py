from __future__ import annotations

import threading
from typing import Any

import anyio
import pytest

from kuu.app import Kuu
from kuu.brokers.memory import MemoryBroker
from kuu.config import Settings
from kuu.exceptions import RetryErr
from kuu.middleware import RetryMiddleware
from kuu.worker import Worker

pytestmark = pytest.mark.anyio


async def _run_worker_until(app: Kuu, predicate, *, timeout: float = 3.0) -> None:
    config = Settings(app="test:app", task_modules=["test.mod"], queues=["default"], concurrency=4)
    worker = Worker(config, app=app)

    async def _supervise(scope: anyio.CancelScope):
        while not predicate():
            await anyio.sleep(0.02)
        scope.cancel()

    with anyio.fail_after(timeout):
        async with anyio.create_task_group() as tg:
            tg.start_soon(_supervise, tg.cancel_scope)
            tg.start_soon(worker.run)


async def test_blocking_task_runs_off_event_loop_thread():
    app = Kuu(broker=MemoryBroker())
    main_thread = threading.get_ident()
    captured: dict[str, Any] = {}

    @app.task(blocking=True)
    def cpu_bound(x: int) -> int:
        captured["thread"] = threading.get_ident()
        captured["result"] = x * x
        return x * x

    await cpu_bound.q(7)
    await _run_worker_until(app, lambda: "thread" in captured)

    assert captured["result"] == 49
    assert captured["thread"] != main_thread, (
        "blocking task must not run on the event loop thread"
    )


async def test_async_task_runs_on_event_loop_thread():
    app = Kuu(broker=MemoryBroker())
    main_thread = threading.get_ident()
    captured: dict[str, Any] = {}

    @app.task
    async def echo(s: str) -> str:
        captured["thread"] = threading.get_ident()
        captured["result"] = s
        return s

    await echo.q("hi")
    await _run_worker_until(app, lambda: "thread" in captured)

    assert captured["result"] == "hi"
    assert captured["thread"] == main_thread


async def test_retry_err_causes_re_delivery_with_attempt_incremented():
    app = Kuu(broker=MemoryBroker(), middleware=[RetryMiddleware(base=0.01, cap=0.05)])
    attempts: list[int] = []

    @app.task(max_attempts=3)
    async def flaky(x: int) -> int:
        attempts.append(x)
        if len(attempts) < 3:
            raise RetryErr(delay=0.02, reason="not yet")
        return x

    await flaky.q(42)
    await _run_worker_until(app, lambda: len(attempts) >= 3, timeout=5.0)

    assert attempts == [42, 42, 42]


async def test_unknown_task_is_requeued_for_a_worker_that_knows_it():
    broker = MemoryBroker()
    app_unaware = Kuu(broker=broker)
    app_aware = Kuu(broker=broker)
    processed: list[int] = []

    @app_aware.task
    async def only_here(x: int) -> int:
        processed.append(x)
        return x

    succeeded_attempts: list[int] = []

    @app_aware.events.task_succeeded.connect
    async def _on_success(msg, elapsed):
        succeeded_attempts.append(msg.attempt)

    await only_here.q(7)

    config = Settings(
        app="test:app",
        queues=["default"],
        concurrency=4,
        unknown_task_delay=0.05,
    )

    async def _supervise(scope: anyio.CancelScope):
        while not processed:
            await anyio.sleep(0.02)
        scope.cancel()

    # unaware worker starts first so it grabs the message before the aware one
    with anyio.fail_after(5.0):
        async with anyio.create_task_group() as tg:
            tg.start_soon(_supervise, tg.cancel_scope)
            tg.start_soon(Worker(config, app=app_unaware).run)
            await anyio.sleep(0.1)
            tg.start_soon(Worker(config, app=app_aware).run)

    assert processed == [7]
    assert succeeded_attempts == [0], "requeue must not burn an attempt"


async def test_exhausted_attempts_dead_letter_the_message():
    broker = MemoryBroker()
    app = Kuu(broker=broker)
    runs: list[int] = []
    dead_seen = anyio.Event()

    @app.task(max_attempts=2)
    async def always_fails(x: int) -> None:
        runs.append(x)
        raise ValueError("boom")

    dead_snapshot: list = []

    @app.events.task_dead.connect
    async def _on_dead(msg):
        # snapshot here: worker shutdown closes the broker, wiping `_dead`
        dead_snapshot.extend(broker.dead("default"))
        dead_seen.set()

    await always_fails.q(1)
    await _run_worker_until(app, dead_seen.is_set, timeout=5.0)

    assert runs == [1, 1]
    assert len(dead_snapshot) == 1
    assert dead_snapshot[0].task == always_fails.task_name


async def test_worker_survives_finalize_failure_and_releases_slot():
    class FlakyAckBroker(MemoryBroker):
        def __init__(self) -> None:
            super().__init__()
            self.fail_once = True

        async def ack(self, delivery) -> None:
            if self.fail_once:
                self.fail_once = False
                raise ConnectionError("ack dropped")
            await super().ack(delivery)

    app = Kuu(broker=FlakyAckBroker())
    seen: list[int] = []

    @app.task
    async def echo(x: int) -> int:
        seen.append(x)
        return x

    await echo.q(1)
    await echo.q(2)
    config = Settings(app="test:app", queues=["default"], concurrency=1, prefetch=1)

    async def _supervise(scope: anyio.CancelScope):
        while len(seen) < 2:
            await anyio.sleep(0.02)
        scope.cancel()

    with anyio.fail_after(5.0):
        async with anyio.create_task_group() as tg:
            tg.start_soon(_supervise, tg.cancel_scope)
            tg.start_soon(Worker(config, app=app).run)

    assert seen == [1, 2]


async def test_worker_survives_transient_consume_failure():
    class FlakyBroker(MemoryBroker):
        def __init__(self) -> None:
            super().__init__()
            self.failures = 2

        async def consume(self, queues, prefetch):
            if self.failures:
                self.failures -= 1
                raise ConnectionError("transient broker outage")
            async for delivery in super().consume(queues, prefetch):
                yield delivery

    app = Kuu(broker=FlakyBroker())
    processed: list[str] = []

    @app.task
    async def echo(s: str) -> str:
        processed.append(s)
        return s

    await echo.q("hi")
    await _run_worker_until(app, lambda: bool(processed), timeout=10.0)

    assert processed == ["hi"]


async def test_blocking_flag_rejects_async_function():
    app = Kuu(broker=MemoryBroker())

    with pytest.raises(TypeError, match="blocking=True"):

        @app.task(blocking=True)
        async def bad() -> None:
            return None
