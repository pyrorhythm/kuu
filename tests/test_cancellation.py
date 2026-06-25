from __future__ import annotations

from datetime import timedelta

import anyio
import pytest

from kuu._util import utcnow
from kuu.app import Kuu
from kuu.brokers.memory import MemoryBroker
from kuu.config import Settings
from kuu.result import Result
from kuu.serializers import JSONSerializer
from kuu.worker import Worker

pytestmark = pytest.mark.anyio


class DictResults:
	"""Minimal in-memory ResultBackend for asserting stored status."""

	def __init__(self) -> None:
		self.serializer = JSONSerializer()
		self.marshal_types = False
		self.ttl = None
		self.replay = False
		self.store_errors = True
		self.store: dict[str, Result] = {}

	def encode(self, value):
		return (self.serializer.marshal(value) if value is not None else b""), None

	def decode(self, result: Result):
		return None if not result.value else self.serializer.unmarshal(result.value, None)

	async def connect(self) -> None: ...
	async def close(self) -> None: ...

	async def get(self, key: str, **kwargs) -> Result | None:
		return self.store.get(key)

	async def set(self, key: str, result: Result, ttl=None) -> None:
		self.store[key] = result

	async def set_not_exists(self, key: str, result: Result, ttl=None) -> bool:
		if key in self.store:
			return False
		self.store[key] = result
		return True


async def test_cancel_running_task_drops_and_does_not_requeue():
	broker = MemoryBroker()
	results = DictResults()
	app = Kuu(broker=broker, results=results)

	started = anyio.Event()
	reached_end = False
	cancelled_ids: list = []

	@app.task
	async def long_task() -> None:
		nonlocal reached_end
		started.set()
		await anyio.sleep(30)
		reached_end = True  # must never run

	@app.events.task_cancelled.connect
	async def _on_cancel(msg) -> None:
		cancelled_ids.append(msg.id)

	handle = await long_task.q()
	config = Settings(app="test:app", queues=["default"], concurrency=4)

	async def _driver(scope: anyio.CancelScope) -> None:
		await started.wait()
		await handle.cancel()
		while not cancelled_ids:
			await anyio.sleep(0.02)
		scope.cancel()

	with anyio.fail_after(5.0):
		async with anyio.create_task_group() as tg:
			tg.start_soon(Worker(config, app=app).run)
			tg.start_soon(_driver, tg.cancel_scope)

	assert cancelled_ids == [handle.id]
	assert reached_end is False, "cancelled task body kept running past the await"
	assert broker.pending_count == 0, "revoked task must be dropped, not requeued"
	assert broker.dead_count == 0
	assert results.store[handle.key].status == "cancelled"


async def test_cancel_pending_task_never_runs():
	broker = MemoryBroker(pump_interval=0.05)
	app = Kuu(broker=broker)

	ran = False
	cancelled_ids: list = []

	@app.task
	async def deferred() -> None:
		nonlocal ran
		ran = True

	@app.events.task_cancelled.connect
	async def _on_cancel(msg) -> None:
		cancelled_ids.append(msg.id)

	# schedule far enough out that the worker (and its revocation watcher) is up
	# and the revoke lands before the message is ever delivered
	handle = await app.enqueue_by_name(
		deferred.task_name, not_before=utcnow() + timedelta(seconds=0.5)
	)
	config = Settings(app="test:app", queues=["default"], concurrency=4)

	async def _driver(scope: anyio.CancelScope) -> None:
		await anyio.sleep(0.2)  # let the revocation watcher subscribe
		await handle.cancel()
		while not cancelled_ids:
			await anyio.sleep(0.02)
		scope.cancel()

	with anyio.fail_after(5.0):
		async with anyio.create_task_group() as tg:
			tg.start_soon(Worker(config, app=app).run)
			tg.start_soon(_driver, tg.cancel_scope)

	assert cancelled_ids == [handle.id]
	assert ran is False, "pending task was revoked but still executed"
	assert broker.pending_count == 0


async def test_revoke_is_noop_for_unknown_id():
	broker = MemoryBroker()
	app = Kuu(broker=broker)

	done = anyio.Event()

	@app.task
	async def echo(x: int) -> int:
		done.set()
		return x

	await echo.q(1)
	config = Settings(app="test:app", queues=["default"], concurrency=4)

	async def _driver(scope: anyio.CancelScope) -> None:
		await app.cancel("00000000-0000-0000-0000-000000000000")  # nobody holds this
		await done.wait()
		scope.cancel()

	with anyio.fail_after(5.0):
		async with anyio.create_task_group() as tg:
			tg.start_soon(Worker(config, app=app).run)
			tg.start_soon(_driver, tg.cancel_scope)

	assert done.is_set()
