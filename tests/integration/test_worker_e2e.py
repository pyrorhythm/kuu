from __future__ import annotations

from typing import Any

import anyio
import pytest
from pydantic import BaseModel

from kuu.app import Kuu
from kuu.config import Settings
from kuu.exceptions import TaskError
from kuu.results.redis import RedisResults
from kuu.worker import Worker


class InnerModel(BaseModel):
	city: str
	zip_code: str


class OuterModel(BaseModel):
	name: str
	inner: InnerModel


class Address(BaseModel):
	street: str
	city: str


class Notification(BaseModel):
	ntype: str
	message: str
	address: Address | None = None


class OrderLine(BaseModel):
	sku: str
	qty: int
	price: float = 0.0


class Order(BaseModel):
	lines: list[OrderLine]
	customer: str


def _config(app: Kuu, concurrency: int = 4) -> Settings:
	return Settings.model_construct(queues=[app.default_queue], concurrency=concurrency)


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


# ── Pydantic model coercion e2e ────────────────────────────────────────────


@pytest.mark.anyio
async def test_pydantic_model_kwarg_round_trips_through_redis(make_app, redis_flushed: str):
	"""Dict that arrives over the wire should be coerced back to a BaseModel."""
	app: Kuu = await make_app(
		stream_prefix="e10:s:", zset_prefix="e10:z:", group="e10", results_prefix="e10:r:"
	)

	received: list[Notification] = []

	@app.task
	async def send_notification(notification: Notification) -> str:
		received.append(notification)
		return notification.ntype

	await send_notification.q(notification=Notification(ntype="email", message="hello"))
	await _run_worker_until(app, lambda: len(received) >= 1)

	assert len(received) == 1
	assert isinstance(received[0], Notification)
	assert received[0].ntype == "email"
	assert received[0].message == "hello"


@pytest.mark.anyio
async def test_nested_pydantic_model_kwarg_round_trips(make_app, redis_flushed: str):
	"""Deeply-nested BaseModel kwargs must be reconstructed with inner models intact."""
	app: Kuu = await make_app(
		stream_prefix="e11:s:", zset_prefix="e11:z:", group="e11", results_prefix="e11:r:"
	)

	received: list[Notification] = []

	@app.task
	async def send_notification(notification: Notification) -> str:
		received.append(notification)
		return notification.ntype

	await send_notification.q(
		notification=Notification(
			ntype="sms", message="urgent", address=Address(street="123 Main", city="NYC")
		)
	)
	await _run_worker_until(app, lambda: len(received) >= 1)

	assert isinstance(received[0], Notification)
	assert isinstance(received[0].address, Address)
	assert received[0].address.city == "NYC"
	assert received[0].address.street == "123 Main"


@pytest.mark.anyio
async def test_pydantic_model_positional_arg_round_trips(make_app, redis_flushed: str):
	"""Positional BaseModel args also need coercion."""
	app: Kuu = await make_app(
		stream_prefix="e12:s:", zset_prefix="e12:z:", group="e12", results_prefix="e12:r:"
	)

	received: list[OuterModel] = []

	@app.task
	async def process(model: OuterModel) -> str:
		received.append(model)
		return model.name

	await process.q(OuterModel(name="Alice", inner=InnerModel(city="LA", zip_code="90001")))
	await _run_worker_until(app, lambda: len(received) >= 1)

	assert isinstance(received[0], OuterModel)
	assert isinstance(received[0].inner, InnerModel)
	assert received[0].inner.zip_code == "90001"


@pytest.mark.anyio
async def test_mixed_args_and_models_round_trip(make_app, redis_flushed: str):
	"""Mix of primitive positional args and a model kwarg."""
	app: Kuu = await make_app(
		stream_prefix="e13:s:", zset_prefix="e13:z:", group="e13", results_prefix="e13:r:"
	)

	received: list[tuple] = []

	@app.task
	async def place_order(user_id: int, order: Order) -> str:
		received.append((user_id, order))
		return f"{user_id}:{order.customer}"

	await place_order.q(
		user_id=99,
		order=Order(
			customer="Bob",
			lines=[OrderLine(sku="A1", qty=2, price=9.99), OrderLine(sku="B2", qty=1)],
		),
	)
	await _run_worker_until(app, lambda: len(received) >= 1)

	uid, order = received[0]
	assert uid == 99
	assert isinstance(order, Order)
	assert isinstance(order.lines[0], OrderLine)
	assert order.lines[0].sku == "A1"
	assert order.lines[0].price == 9.99
	assert order.lines[1].qty == 1
	# default field
	assert order.lines[1].price == 0.0


@pytest.mark.anyio
async def test_optional_none_field_in_model_round_trips(make_app, redis_flushed: str):
	"""Optional fields that are None when enqueued should remain None after coercion."""
	app: Kuu = await make_app(
		stream_prefix="e14:s:", zset_prefix="e14:z:", group="e14", results_prefix="e14:r:"
	)

	received: list[Notification] = []

	@app.task
	async def send_notification(notification: Notification) -> str:
		received.append(notification)
		return notification.ntype

	await send_notification.q(notification=Notification(ntype="push", message="ping"))
	await _run_worker_until(app, lambda: len(received) >= 1)

	assert received[0].address is None


@pytest.mark.anyio
async def test_blocking_task_with_pydantic_model_coerces(make_app, redis_flushed: str):
	"""Blocking (sync) tasks must also get model coercion."""
	app: Kuu = await make_app(
		stream_prefix="e15:s:", zset_prefix="e15:z:", group="e15", results_prefix="e15:r:"
	)

	import threading

	received: list[tuple[Notification, int]] = []
	main_thread = threading.get_ident()

	@app.task(blocking=True)
	def process(notification: Notification) -> str:
		received.append((notification, threading.get_ident()))
		return notification.ntype

	await process.q(
		notification=Notification(
			ntype="webhook", message="deploy", address=Address(street="1st Ave", city="SF")
		)
	)
	await _run_worker_until(app, lambda: len(received) >= 1)

	notif, tid = received[0]
	assert isinstance(notif, Notification)
	assert isinstance(notif.address, Address)
	assert notif.address.city == "SF"
	assert tid != main_thread


@pytest.mark.anyio
async def test_multiple_model_kwargs_all_coerced(make_app, redis_flushed: str):
	"""Two BaseModel kwargs in the same call must both be reconstructed."""
	app: Kuu = await make_app(
		stream_prefix="e16:s:", zset_prefix="e16:z:", group="e16", results_prefix="e16:r:"
	)

	received: list[tuple[Address, InnerModel]] = []

	@app.task
	async def merge(addr: Address, inner: InnerModel) -> str:
		received.append((addr, inner))
		return f"{addr.city}-{inner.zip_code}"

	await merge.q(
		addr=Address(street="2nd St", city="Chicago"),
		inner=InnerModel(city="Chicago", zip_code="60601"),
	)
	await _run_worker_until(app, lambda: len(received) >= 1)

	a, i = received[0]
	assert isinstance(a, Address)
	assert isinstance(i, InnerModel)
	assert a.street == "2nd St"
	assert i.zip_code == "60601"


@pytest.mark.anyio
async def test_pydantic_kwarg_with_primitives_coexists(make_app, redis_flushed: str):
	"""Primitive kwargs pass through untouched alongside model coercion."""
	app: Kuu = await make_app(
		stream_prefix="e17:s:", zset_prefix="e17:z:", group="e17", results_prefix="e17:r:"
	)

	received: list[tuple] = []

	@app.task
	async def send(channel: str, notification: Notification, priority: int) -> str:
		received.append((channel, notification, priority))
		return f"{channel}/{notification.ntype}/{priority}"

	await send.q(
		channel="slack",
		notification=Notification(ntype="info", message="deploy done"),
		priority=3,
	)
	await _run_worker_until(app, lambda: len(received) >= 1)

	ch, notif, prio = received[0]
	assert ch == "slack"
	assert prio == 3
	assert isinstance(notif, Notification)
	assert notif.ntype == "info"
