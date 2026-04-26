from __future__ import annotations

import threading
from typing import Any

import anyio
import pytest

from kuu.app import Kuu
from kuu.brokers.memory import MemoryBroker
from kuu.config import Kuunfig
from kuu.exceptions import RetryErr
from kuu.middleware import RetryMiddleware
from kuu.worker import Worker


async def _run_worker_until(app: Kuu, predicate, *, timeout: float = 3.0) -> None:
	config = Kuunfig.model_construct(queues=["default"], concurrency=4)
	worker = Worker(config, app=app)

	async def _supervise(scope: anyio.CancelScope):
		while not predicate():
			await anyio.sleep(0.02)
		scope.cancel()

	with anyio.fail_after(timeout):
		async with anyio.create_task_group() as tg:
			tg.start_soon(_supervise, tg.cancel_scope)
			tg.start_soon(worker.run)


@pytest.mark.anyio
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
	assert captured["thread"] != main_thread, "blocking task must not run on the event loop thread"


@pytest.mark.anyio
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


@pytest.mark.anyio
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


@pytest.mark.anyio
async def test_blocking_flag_rejects_async_function():
	app = Kuu(broker=MemoryBroker())

	with pytest.raises(TypeError, match="blocking=True"):

		@app.task(blocking=True)
		async def bad() -> None:
			return None
