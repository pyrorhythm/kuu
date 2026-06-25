from __future__ import annotations

import anyio
import pytest

from kuu.app import Kuu
from kuu.config import Settings
from kuu.worker import Worker

pytestmark = pytest.mark.anyio


def _config(app: Kuu, *, concurrency: int = 4, prefetch: int = 2) -> Settings:
	return Settings(
		app="test:app",
		task_modules=["test"],
		queues=[app.default_queue],
		concurrency=concurrency,
		prefetch=prefetch,
		shutdown_timeout=2.0,
	)


async def _run_with_driver(app: Kuu, config: Settings, driver, *, timeout: float = 30.0) -> None:
	"""Run the worker alongside `driver(scope)`; driver cancels scope to stop."""
	with anyio.fail_after(timeout):
		async with anyio.create_task_group() as tg:
			tg.start_soon(Worker(config, app=app).run)
			tg.start_soon(driver, tg.cancel_scope)


async def test_e2e_cancel_running_task_drops_it(make_app):
	app = await make_app()
	started = anyio.Event()
	reached_end = False
	cancelled: list = []

	@app.task
	async def long_task() -> None:
		nonlocal reached_end
		started.set()
		await anyio.sleep(30)
		reached_end = True  # must never run

	@app.events.task_cancelled.connect
	async def _on_cancel(msg) -> None:
		cancelled.append(msg.id)

	handle = await long_task.q()

	async def _driver(scope: anyio.CancelScope) -> None:
		await started.wait()
		# broadcast revoke is best-effort: re-issue until the worker's watcher has
		# bound its channel (rabbitmq's fanout setup is slower than redis/nats).
		with anyio.move_on_after(15):
			while not cancelled:
				await handle.cancel()
				await anyio.sleep(0.3)
		scope.cancel()

	await _run_with_driver(app, _config(app), _driver)

	assert cancelled == [handle.id]
	assert reached_end is False, "cancelled task body kept running past the await"


async def test_e2e_cancel_pending_task_never_runs(make_app):
	# concurrency=1: a blocker holds the only slot so `target` stays pending in
	# the queue. We revoke `target` while it waits, then release the blocker and
	# assert the worker drops `target` without ever running its body.
	app = await make_app()
	blocker_started = anyio.Event()
	release = anyio.Event()
	target_ran = False
	cancelled: list = []

	@app.task
	async def blocker() -> None:
		blocker_started.set()
		await release.wait()

	@app.task
	async def target() -> None:
		nonlocal target_ran
		target_ran = True

	@app.events.task_cancelled.connect
	async def _on_cancel(msg) -> None:
		cancelled.append(msg.id)

	await blocker.q()
	target_handle = await target.q()

	async def _driver(scope: anyio.CancelScope) -> None:
		await blocker_started.wait()  # worker is up; revocation watcher subscribing
		# re-broadcast for a bounded window so the revoke lands in the worker's
		# local set before the blocker frees the slot and `target` is pulled.
		with anyio.move_on_after(3):
			while True:
				await target_handle.cancel()
				await anyio.sleep(0.3)
		release.set()
		with anyio.move_on_after(15):
			while target_handle.id not in cancelled:
				await anyio.sleep(0.05)
		scope.cancel()

	await _run_with_driver(app, _config(app, concurrency=1, prefetch=1), _driver)

	assert target_handle.id in cancelled
	assert target_ran is False, "pending task was revoked but still executed"


async def test_e2e_cancel_stores_cancelled_status(make_app_with_results):
	app = await make_app_with_results()
	started = anyio.Event()

	@app.task
	async def long_task() -> None:
		started.set()
		await anyio.sleep(30)

	handle = await long_task.q()
	stored: list = []

	async def _driver(scope: anyio.CancelScope) -> None:
		await started.wait()
		with anyio.move_on_after(15):
			while True:
				await handle.cancel()
				r = await app.results.get(handle.key, listen_timeout=0)
				if r is not None and r.status == "cancelled":
					stored.append(r)
					break
				await anyio.sleep(0.2)
		scope.cancel()

	await _run_with_driver(app, _config(app), _driver)

	assert stored and stored[0].status == "cancelled"
