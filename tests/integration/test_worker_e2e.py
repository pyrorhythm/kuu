from __future__ import annotations

from typing import Any

import anyio
import pytest
from pydantic import BaseModel

from kuu.app import Kuu
from kuu.config import Kuunfig
from kuu.exceptions import TaskError
from kuu.results.redis import RedisResults
from kuu.worker import Worker


def _config(app: Kuu, concurrency: int = 4) -> Kuunfig:
	return Kuunfig.model_construct(queues=[app.default_queue], concurrency=concurrency)


async def _run_worker_until(app: Kuu, predicate, *, timeout: float = 10.0) -> None:
	worker = Worker(_config(app), app=app)

	async def _supervise(scope: anyio.CancelScope):
		while not predicate():
			await anyio.sleep(0.05)
		scope.cancel()

	with anyio.fail_after(timeout):
		async with anyio.create_task_group() as tg:
			tg.start_soon(_supervise, tg.cancel_scope)
			tg.start_soon(worker.run)


@pytest.mark.anyio
async def test_success_result_is_stored_and_retrievable(make_app, redis_flushed: str):
	app: Kuu = await make_app(
		stream_prefix="e1:s:", zset_prefix="e1:z:", group="e1", results_prefix="e1:r:"
	)

	@app.task
	async def add(a: int, b: int) -> int:
		return a + b

	calls: list[tuple[int, int]] = []

	@app.task
	async def record(x: int) -> int:
		calls.append((x, x))
		return x * 2

	handle = await record.q(7)
	await _run_worker_until(app, lambda: len(calls) >= 1)

	verifier = RedisResults(url=redis_flushed, prefix="e1:r:")
	await verifier.connect()
	try:
		stored = await verifier.get(handle.key)
		assert stored is not None
		assert stored.status == "ok"
		assert verifier.decode(stored) == 14
	finally:
		await verifier.close()


@pytest.mark.anyio
async def test_replay_skips_re_execution_for_same_idempotency_key(make_app):
	app: Kuu = await make_app(
		stream_prefix="e2:s:", zset_prefix="e2:z:", group="e2", results_prefix="e2:r:"
	)

	calls: list[int] = []

	@app.task
	async def once(x: int) -> int:
		calls.append(x)
		return x

	from kuu.message import Payload

	h1 = await app.enqueue_by_name(
		once.task_name,
		Payload(args=(1,)),
		headers={"idempotency_key": "shared-key"},
	)
	await _run_worker_until(app, lambda: len(calls) >= 1)
	assert h1.key == "shared-key"

	h2 = await app.enqueue_by_name(
		once.task_name,
		Payload(args=(2,)),
		headers={"idempotency_key": "shared-key"},
	)
	worker = Worker(_config(app, concurrency=2), app=app)

	async def _stop_after(scope: anyio.CancelScope, secs: float):
		await anyio.sleep(secs)
		scope.cancel()

	with anyio.fail_after(8.0):
		async with anyio.create_task_group() as tg:
			tg.start_soon(_stop_after, tg.cancel_scope, 1.5)
			tg.start_soon(worker.run)

	assert calls == [1], "task body must not re-execute when result is replayed"
	assert h2.key == "shared-key"


@pytest.mark.anyio
async def test_final_attempt_failure_stores_error(make_app, redis_flushed: str):
	app: Kuu = await make_app(
		stream_prefix="e3:s:", zset_prefix="e3:z:", group="e3", results_prefix="e3:r:"
	)

	attempts: list[int] = []

	@app.task(max_attempts=1)
	async def boom() -> None:
		attempts.append(1)
		raise RuntimeError("kaboom")

	handle = await boom.q()
	await _run_worker_until(app, lambda: len(attempts) >= 1)
	await anyio.sleep(0.1)

	verifier = RedisResults(url=redis_flushed, prefix="e3:r:")
	await verifier.connect()
	try:
		stored = await verifier.get(handle.key)
		assert stored is not None
		assert stored.status == "error"
		assert "RuntimeError" in (stored.error or "")
		assert "kaboom" in (stored.error or "")
	finally:
		await verifier.close()

	# TaskHandle.result should raise.
	await app.results.connect()
	try:
		with pytest.raises(TaskError):
			await handle.result(timeout=1.0)
	finally:
		await app.results.close()


@pytest.mark.anyio
async def test_store_errors_disabled_does_not_persist_failure(make_app, redis_flushed: str):
	app: Kuu = await make_app(
		stream_prefix="e4:s:",
		zset_prefix="e4:z:",
		group="e4",
		results_prefix="e4:r:",
		result_store_errors=False,
	)

	attempts: list[int] = []

	@app.task(max_attempts=1)
	async def bad() -> None:
		attempts.append(1)
		raise ValueError("nope")

	handle = await bad.q()
	await _run_worker_until(app, lambda: len(attempts) >= 1)
	await anyio.sleep(0.1)

	verifier = RedisResults(url=redis_flushed, prefix="e4:r:")
	await verifier.connect()
	try:
		assert await verifier.get(handle.key) is None
	finally:
		await verifier.close()


@pytest.mark.anyio
async def test_replay_disabled_reruns_task_on_redelivery(make_app):
	app: Kuu = await make_app(
		stream_prefix="e5:s:",
		zset_prefix="e5:z:",
		group="e5",
		results_prefix="e5:r:",
		result_replay=False,
	)

	from kuu.message import Payload

	calls: list[int] = []

	@app.task
	async def t(x: int) -> int:
		calls.append(x)
		return x

	await app.enqueue_by_name(t.task_name, Payload(args=(1,)), headers={"idempotency_key": "k"})
	await _run_worker_until(app, lambda: len(calls) >= 1)

	await app.enqueue_by_name(t.task_name, Payload(args=(2,)), headers={"idempotency_key": "k"})
	await _run_worker_until(app, lambda: len(calls) >= 2)

	assert calls == [1, 2]


class Outcome(BaseModel):
	value: int
	label: str


@pytest.mark.anyio
async def test_marshal_types_round_trips_pydantic_model(make_app, redis_flushed: str):
	app: Kuu = await make_app(
		stream_prefix="e6:s:", zset_prefix="e6:z:", group="e6", results_prefix="e6:r:"
	)

	@app.task
	async def make_outcome(v: int) -> Outcome:
		return Outcome(value=v, label="ok")

	handle = await make_outcome.q(42)
	done = anyio.Event()

	# Wait for the result to land in redis.
	verifier = RedisResults(url=redis_flushed, prefix="e6:r:")
	await verifier.connect()
	try:

		async def _poll() -> None:
			while True:
				r = await verifier.get(handle.key)
				if r is not None and r.status == "ok":
					done.set()
					return
				await anyio.sleep(0.05)

		worker = Worker(_config(app, concurrency=2), app=app)

		async def _supervise(scope: anyio.CancelScope):
			await done.wait()
			scope.cancel()

		with anyio.fail_after(10.0):
			async with anyio.create_task_group() as tg:
				tg.start_soon(_supervise, tg.cancel_scope)
				tg.start_soon(_poll)
				tg.start_soon(worker.run)

		stored = await verifier.get(handle.key)
		assert stored is not None
		assert stored.type and stored.type.endswith("Outcome")
		decoded = verifier.decode(stored)
		assert isinstance(decoded, Outcome)
		assert decoded == Outcome(value=42, label="ok")
	finally:
		await verifier.close()


@pytest.mark.anyio
async def test_retry_then_success_stores_only_final_value(make_app, redis_flushed: str):
	from kuu.exceptions import RetryErr
	from kuu.middleware import RetryMiddleware

	app: Kuu = await make_app(
		stream_prefix="e7:s:",
		zset_prefix="e7:z:",
		group="e7",
		results_prefix="e7:r:",
		middleware=[RetryMiddleware(base=0.05, cap=0.1)],
	)

	attempts: list[int] = []

	@app.task(max_attempts=3)
	async def flaky(x: int) -> int:
		attempts.append(x)
		if len(attempts) < 2:
			raise RetryErr(delay=0.05, reason="flap")
		return x * 10

	handle = await flaky.q(3)
	await _run_worker_until(app, lambda: len(attempts) >= 2, timeout=15.0)
	await anyio.sleep(0.1)

	verifier = RedisResults(url=redis_flushed, prefix="e7:r:")
	await verifier.connect()
	try:
		stored = await verifier.get(handle.key)
		assert stored is not None
		assert stored.status == "ok"
		assert verifier.decode(stored) == 30
	finally:
		await verifier.close()


@pytest.mark.anyio
async def test_non_final_failure_does_not_store_error(make_app, redis_flushed: str):
	app: Kuu = await make_app(
		stream_prefix="e8:s:", zset_prefix="e8:z:", group="e8", results_prefix="e8:r:"
	)

	attempts: list[int] = []

	@app.task(max_attempts=3)
	async def sometimes(x: int) -> int:
		attempts.append(x)
		if len(attempts) < 2:
			raise RuntimeError("transient")
		return x

	handle = await sometimes.q(5)
	verifier = RedisResults(url=redis_flushed, prefix="e8:r:")
	await verifier.connect()
	try:
		await _run_worker_until(app, lambda: len(attempts) >= 1, timeout=10.0)
		stored_after_first = await verifier.get(handle.key)
		assert stored_after_first is None or stored_after_first.status != "error"

		await _run_worker_until(app, lambda: len(attempts) >= 2, timeout=15.0)
		await anyio.sleep(0.1)
		final = await verifier.get(handle.key)
		assert final is not None and final.status == "ok"
		assert verifier.decode(final) == 5
	finally:
		await verifier.close()


@pytest.mark.anyio
async def test_unknown_task_failure_stores_error_on_final_attempt(make_app, redis_flushed: str):
	app: Kuu = await make_app(
		stream_prefix="e9:s:", zset_prefix="e9:z:", group="e9", results_prefix="e9:r:"
	)

	from kuu.message import Payload

	handle = await app.enqueue_by_name(
		"missing.task",
		Payload(args=(1,)),
		max_attempts=1,
	)

	verifier = RedisResults(url=redis_flushed, prefix="e9:r:")
	await verifier.connect()

	saw_error: dict[str, Any] = {}

	async def _wait():
		while True:
			r = await verifier.get(handle.key)
			if r is not None and r.status == "error":
				saw_error["r"] = r
				return
			await anyio.sleep(0.05)

	try:
		worker = Worker(_config(app, concurrency=2), app=app)

		async def _supervise(scope: anyio.CancelScope):
			while "r" not in saw_error:
				await anyio.sleep(0.05)
			scope.cancel()

		with anyio.fail_after(10.0):
			async with anyio.create_task_group() as tg:
				tg.start_soon(_supervise, tg.cancel_scope)
				tg.start_soon(_wait)
				tg.start_soon(worker.run)

		assert "UnknownTask" in (saw_error["r"].error or "")
	finally:
		await verifier.close()
