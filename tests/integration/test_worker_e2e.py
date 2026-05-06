from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import anyio
import pytest
from msgspec import Struct

from kuu.app import Kuu
from kuu.config import Settings
from kuu.exceptions import TaskError
from kuu.handle import TaskHandle
from kuu.message import Message, Payload
from kuu.worker import Worker

pytestmark = pytest.mark.anyio

# === shared models


class InnerModel(Struct):
	city: str
	zip_code: str


class OuterModel(Struct):
	name: str
	inner: InnerModel


class Address(Struct):
	street: str
	city: str


class Notification(Struct):
	ntype: str
	message: str
	address: Address | None = None


class OrderLine(Struct):
	sku: str
	qty: int
	price: float = 0.0


class Order(Struct):
	lines: list[OrderLine]
	customer: str


class Outcome(Struct):
	value: int
	label: str


# === helpers


def _config(app: Kuu, concurrency: int = 4) -> Settings:
	return Settings(
		app="test:app", task_modules=["test"], queues=[app.default_queue], concurrency=concurrency
	)


async def _run_worker_until(app: Kuu, predicate, *, timeout: float = 15.0) -> None:
	"""Run worker until predicate() is True, then cancel."""
	worker = Worker(_config(app), app=app)

	async def _supervise(scope: anyio.CancelScope):
		while not predicate():
			await anyio.sleep(0.05)
		scope.cancel()

	with anyio.fail_after(timeout):
		async with anyio.create_task_group() as tg:
			tg.start_soon(_supervise, tg.cancel_scope)
			tg.start_soon(worker.run)


async def _run_worker_briefly(app: Kuu, *, seconds: float = 3.0) -> None:
	config = Settings(
		app="test:app",
		task_modules=["test"],
		queues=[app.default_queue],
		concurrency=4,
		prefetch=2,
		shutdown_timeout=2.0,
	)
	worker = Worker(config, app=app)
	with anyio.move_on_after(seconds):
		await worker.run()


async def _await_result(app: Kuu, handle: TaskHandle, *, timeout: float = 10.0):
	result: list = []
	exc: list[BaseException] = []

	async def _poll(scope: anyio.CancelScope):
		try:
			result.append(await handle.result(timeout=timeout))
		except Exception as e:
			exc.append(e)
		scope.cancel()

	worker = Worker(_config(app, concurrency=2), app=app)
	with anyio.fail_after(timeout + 2):
		async with anyio.create_task_group() as tg:
			tg.start_soon(_poll, tg.cancel_scope)
			tg.start_soon(worker.run)

	if exc:
		raise exc[0]
	return result[0]


async def _with_worker(app: Kuu, fn, *, timeout: float = 10.0):
	"""Run an async fn alongside the worker; cancel worker when fn returns."""
	result: list = []

	async def _fn_wrapper(scope: anyio.CancelScope):
		result.append(await fn())
		scope.cancel()

	worker = Worker(_config(app, concurrency=2), app=app)
	with anyio.fail_after(timeout):
		async with anyio.create_task_group() as tg:
			tg.start_soon(_fn_wrapper, tg.cancel_scope)
			tg.start_soon(worker.run)

	return result[0] if result else None


# === basic delivery tests (no results needed)


async def test_e2e_enqueue_consume_ack(make_app):
	app = await make_app()
	delivered: list[str] = []

	@app.task
	async def hello(name: str) -> str:
		delivered.append(name)
		return f"hi {name}"

	await hello.q("world")
	await _run_worker_until(app, lambda: len(delivered) >= 1, timeout=15.0)

	assert delivered == ["world"]


async def test_e2e_scheduled_task_delivered_after_delay(make_app):
	app = await make_app()
	delivered_at: list[float] = []

	@app.task
	async def delayed() -> None:
		delivered_at.append(anyio.current_time())

	sent_at = anyio.current_time()
	when = datetime.now(timezone.utc) + timedelta(milliseconds=300)
	msg = Message(task=delayed.task_name, queue="default", payload=Payload(), not_before=when)
	await app.broker.schedule(msg, when)

	await _run_worker_until(app, lambda: len(delivered_at) >= 1, timeout=15.0)

	assert len(delivered_at) == 1
	elapsed = delivered_at[0] - sent_at
	assert elapsed >= 0.25, f"delivered too early: {elapsed:.3f}s"


async def test_e2e_nack_with_delay_retries(make_app):
	app = await make_app()
	attempts: list[int] = []

	@app.task(max_attempts=3)
	async def flaky() -> str:
		if len(attempts) == 0:
			attempts.append(1)
			from kuu.exceptions import RetryErr

			raise RetryErr(delay=0.2, reason="first attempt fails")
		attempts.append(2)
		return "ok"

	await flaky.q()
	await _run_worker_until(app, lambda: len(attempts) >= 2, timeout=15.0)

	assert attempts == [1, 2]


async def test_e2e_multiple_queues(make_app):
	app = await make_app()
	results_a: list[str] = []
	results_b: list[str] = []

	@app.task(queue="q1")
	async def task_a(x: str) -> None:
		results_a.append(x)

	@app.task(queue="q2")
	async def task_b(x: str) -> None:
		results_b.append(x)

	await task_a.q("a1")
	await task_b.q("b1")
	await task_a.q("a2")

	config = Settings(
		app="test:app",
		task_modules=["test"],
		queues=["q1", "q2"],
		concurrency=4,
		prefetch=2,
		shutdown_timeout=2.0,
	)
	worker = Worker(config, app=app)
	with anyio.move_on_after(5.0):
		await worker.run()

	assert sorted(results_a) == ["a1", "a2"]
	assert results_b == ["b1"]


async def test_e2e_blocking_task(make_app):
	import threading

	app = await make_app()
	main_thread = threading.get_ident()
	captured: dict = {}

	@app.task(blocking=True)
	def cpu_work(n: int) -> int:
		captured["thread"] = threading.get_ident()
		captured["result"] = n * n
		return n * n

	await cpu_work.q(7)
	await _run_worker_until(app, lambda: "result" in captured, timeout=15.0)

	assert captured["result"] == 49
	assert captured["thread"] != main_thread


async def test_e2e_max_attempts_exhausted(make_app):
	app = await make_app()
	count = 0

	@app.task(max_attempts=2)
	async def always_fail() -> None:
		nonlocal count
		count += 1
		raise RuntimeError("permanent failure")

	await always_fail.q()
	await _run_worker_briefly(app, seconds=6.0)

	assert count == 2


async def test_e2e_headers_preserved(make_app):
	app = await make_app()
	delivered_headers: list[dict] = []

	@app.task
	async def noop() -> None:
		pass

	msg = Message(
		task="noop",
		queue="default",
		payload=Payload(),
		headers={"trace-id": "abc-123", "tenant": "acme"},
	)
	await app.broker.enqueue(msg)

	config = Settings(
		app="test:app",
		task_modules=["test"],
		queues=["default"],
		concurrency=1,
		prefetch=1,
		shutdown_timeout=2.0,
	)
	worker = Worker(config, app=app)

	original_handle = worker._handle

	async def _spy(delivery):
		delivered_headers.append(dict(delivery.message.headers))
		await original_handle(delivery)

	worker._handle = _spy  # type: ignore[assignment]

	with anyio.move_on_after(5.0):
		await worker.run()

	assert len(delivered_headers) >= 1
	assert delivered_headers[0]["trace-id"] == "abc-123"
	assert delivered_headers[0]["tenant"] == "acme"


async def test_e2e_mixed_immediate_and_scheduled(make_app):
	app = await make_app()
	results: list[tuple[str, float]] = []

	@app.task
	async def record(tag: str) -> None:
		results.append((tag, anyio.current_time()))

	await record.q("immediate")

	when = datetime.now(timezone.utc) + timedelta(milliseconds=400)
	msg = Message(
		task=record.task_name,
		queue="default",
		payload=Payload(args=("scheduled",)),
		not_before=when,
	)
	await app.broker.schedule(msg, when)

	await _run_worker_until(app, lambda: len(results) >= 2, timeout=15.0)

	tags = [t for t, _ in results]
	assert "immediate" in tags
	assert "scheduled" in tags

	sched_time = next(t for tag, t in results if tag == "scheduled")
	immed_time = next(t for tag, t in results if tag == "immediate")
	assert sched_time >= immed_time


async def test_e2e_result_retrieval(make_app_with_results):
	app = await make_app_with_results()

	@app.task
	async def add(a: int, b: int) -> int:
		return a + b

	handle = await add.q(3, 4)
	value = await _await_result(app, handle)
	assert value == 7


async def test_e2e_replay_cached_result(make_app_with_results):
	app = await make_app_with_results(result_replay=True)
	run_count = 0

	@app.task
	async def counted(x: int) -> int:
		nonlocal run_count
		run_count += 1
		return x * 10

	enqueue = app._enqueue_task(counted, headers={"idempotency_key": "count-5"})
	h1 = await enqueue(5)
	v1 = await _await_result(app, h1)
	assert v1 == 50
	assert run_count == 1

	h2 = await enqueue(5)
	v2 = await _await_result(app, h2)
	assert v2 == 50
	assert run_count == 1, f"expected replay (run_count=1), got {run_count}"


async def test_success_result_is_stored_and_retrievable(make_app_with_results):
	app = await make_app_with_results()
	calls: list[tuple[int, int]] = []

	@app.task
	async def record(x: int) -> int:
		calls.append((x, x))
		return x * 2

	handle = await record.q(7)
	value = await _await_result(app, handle)
	assert value == 14


async def test_replay_skips_re_execution_for_same_idempotency_key(
	make_app_with_results,
):
	app = await make_app_with_results(result_replay=True)
	calls: list[int] = []

	@app.task
	async def once(x: int) -> int:
		calls.append(x)
		return x

	h1 = await app.enqueue_by_name(
		once.task_name,
		Payload(args=(1,)),
		headers={"idempotency_key": "shared-key"},
	)
	v1 = await _await_result(app, h1)
	assert v1 == 1
	assert calls == [1]
	assert h1.key == "shared-key"

	h2 = await app.enqueue_by_name(
		once.task_name,
		Payload(args=(2,)),
		headers={"idempotency_key": "shared-key"},
	)
	v2 = await _await_result(app, h2)
	assert v2 == 1
	assert calls == [1], "task body must not re-execute when result is replayed"


async def test_final_attempt_failure_stores_error(make_app_with_results):
	app = await make_app_with_results()
	attempts: list[int] = []

	@app.task(max_attempts=1)
	async def boom() -> None:
		attempts.append(1)
		raise RuntimeError("kaboom")

	handle = await boom.q()

	with pytest.raises(TaskError):
		await _await_result(app, handle)

	assert len(attempts) >= 1


async def test_store_errors_disabled_does_not_persist_failure(make_app_with_results):
	app = await make_app_with_results(result_store_errors=False)
	attempts: list[int] = []

	@app.task(max_attempts=1)
	async def bad() -> None:
		attempts.append(1)
		raise ValueError("nope")

	handle = await bad.q()
	stored: list = []

	async def _check():
		while len(attempts) < 1:
			await anyio.sleep(0.05)
		await anyio.sleep(0.2)
		# When store_errors=False, no result is stored, so use a timeout
		stored.append(await app.results.get(handle.key, listen_timeout=1.0))

	await _with_worker(app, _check)
	assert stored[0] is None


async def test_replay_disabled_reruns_task_on_redelivery(make_app_with_results):
	app = await make_app_with_results(result_replay=False)
	calls: list[int] = []

	@app.task
	async def t(x: int) -> int:
		calls.append(x)
		return x

	await app.enqueue_by_name(t.task_name, Payload(args=(1,)), headers={"idempotency_key": "k"})
	await _run_worker_until(app, lambda: len(calls) >= 1)

	await app.enqueue_by_name(t.task_name, Payload(args=(2,)), headers={"idempotency_key": "k"})
	await _run_worker_until(app, lambda: len(calls) >= 2, timeout=20.0)

	assert calls == [1, 2]


async def test_marshal_types_round_trips_pydantic_model(make_app_with_results):
	app = await make_app_with_results()

	@app.task
	async def make_outcome(v: int) -> Outcome:
		return Outcome(value=v, label="ok")

	handle = await make_outcome.q(42)
	result = await _await_result(app, handle)

	assert isinstance(result, Outcome)
	assert result == Outcome(value=42, label="ok")


async def test_retry_then_success_stores_only_final_value(make_app_with_results):
	from kuu.exceptions import RetryErr
	from kuu.middleware import RetryMiddleware

	app = await make_app_with_results(
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
	value = await _await_result(app, handle, timeout=15.0)

	assert value == 30
	assert len(attempts) >= 2


async def test_non_final_failure_does_not_store_error(make_app_with_results):
	app = await make_app_with_results()
	attempts: list[int] = []

	@app.task(max_attempts=3)
	async def sometimes(x: int) -> int:
		attempts.append(x)
		if len(attempts) < 2:
			raise RuntimeError("transient")
		return x

	handle = await sometimes.q(5)

	intermediate: list = []

	async def _check_intermediate():
		while len(attempts) < 1:
			await anyio.sleep(0.05)
		await anyio.sleep(0.1)
		intermediate.append(await app.results.get(handle.key))

	await _with_worker(app, _check_intermediate, timeout=12.0)
	assert intermediate[0] is None or intermediate[0].status != "error"

	value = await _await_result(app, handle, timeout=15.0)
	assert value == 5


async def test_unknown_task_failure_stores_error_on_final_attempt(
	make_app_with_results,
):
	app = await make_app_with_results()

	handle = await app.enqueue_by_name(
		"missing.task",
		Payload(args=(1,)),
		max_attempts=1,
	)

	saw_error: dict[str, Any] = {}

	async def _wait():
		while True:
			r = await app.results.get(handle.key)
			if r is not None and r.status == "error":
				saw_error["r"] = r
				return
			await anyio.sleep(0.05)

	await _with_worker(app, _wait)
	assert "UnknownTask" in (saw_error["r"].error or "")


async def test_pydantic_model_kwarg_round_trips(make_app_with_results):
	app = await make_app_with_results()
	received: list[Notification] = []

	@app.task
	async def send_notification(notification: Notification) -> str:
		received.append(notification)
		return notification.ntype

	await send_notification.q(notification=Notification(ntype="email", message="hello"))
	await _run_worker_until(app, lambda: len(received) >= 1)

	assert isinstance(received[0], Notification)
	assert received[0].ntype == "email"


async def test_nested_pydantic_model_kwarg_round_trips(make_app_with_results):
	app = await make_app_with_results()
	received: list[Notification] = []

	@app.task
	async def send_notification(notification: Notification) -> str:
		received.append(notification)
		return notification.ntype

	await send_notification.q(
		notification=Notification(
			ntype="sms",
			message="urgent",
			address=Address(street="123 Main", city="NYC"),
		)
	)
	await _run_worker_until(app, lambda: len(received) >= 1)

	assert isinstance(received[0].address, Address)
	assert received[0].address.city == "NYC"


async def test_pydantic_model_positional_arg_round_trips(make_app_with_results):
	app = await make_app_with_results()
	received: list[OuterModel] = []

	@app.task
	async def process(model: OuterModel) -> str:
		received.append(model)
		return model.name

	await process.q(OuterModel(name="Alice", inner=InnerModel(city="LA", zip_code="90001")))
	await _run_worker_until(app, lambda: len(received) >= 1)

	assert isinstance(received[0].inner, InnerModel)
	assert received[0].inner.zip_code == "90001"


async def test_mixed_args_and_models_round_trip(make_app_with_results):
	app = await make_app_with_results()
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
	assert isinstance(order.lines[0], OrderLine)
	assert order.lines[0].price == 9.99


async def test_optional_none_field_in_model_round_trips(make_app_with_results):
	app = await make_app_with_results()
	received: list[Notification] = []

	@app.task
	async def send_notification(notification: Notification) -> str:
		received.append(notification)
		return notification.ntype

	await send_notification.q(notification=Notification(ntype="push", message="ping"))
	await _run_worker_until(app, lambda: len(received) >= 1)

	assert received[0].address is None


async def test_blocking_task_with_pydantic_model_coerces(make_app_with_results):
	import threading

	app = await make_app_with_results()
	received: list[tuple[Notification, int]] = []
	main_thread = threading.get_ident()

	@app.task(blocking=True)
	def process(notification: Notification) -> str:
		received.append((notification, threading.get_ident()))
		return notification.ntype

	await process.q(
		notification=Notification(
			ntype="webhook",
			message="deploy",
			address=Address(street="1st Ave", city="SF"),
		)
	)
	await _run_worker_until(app, lambda: len(received) >= 1)

	notif, tid = received[0]
	assert isinstance(notif.address, Address)
	assert notif.address.city == "SF"
	assert tid != main_thread


async def test_multiple_model_kwargs_all_coerced(make_app_with_results):
	app = await make_app_with_results()
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
	assert i.zip_code == "60601"


async def test_list_of_models_round_trips(make_app_with_results):
	app = await make_app_with_results()
	received: list[list[OrderLine]] = []

	@app.task
	async def process_order(lines: list[OrderLine]) -> str:
		received.append(lines)
		return str(len(lines))

	await process_order.q(
		lines=[OrderLine(sku="A1", qty=2, price=9.99), OrderLine(sku="B2", qty=1)]
	)
	await _run_worker_until(app, lambda: len(received) >= 1)

	lines = received[0]
	assert len(lines) == 2
	assert isinstance(lines[0], OrderLine)
	assert lines[0].price == 9.99


async def test_dict_of_models_round_trips(make_app_with_results):
	app = await make_app_with_results()
	received: list[dict[str, InnerModel]] = []

	@app.task
	async def merge_inner(lookup: dict[str, InnerModel]) -> int:
		received.append(lookup)
		return len(lookup)

	await merge_inner.q(
		lookup={
			"nyc": InnerModel(city="NYC", zip_code="10001"),
			"la": InnerModel(city="LA", zip_code="90001"),
		}
	)
	await _run_worker_until(app, lambda: len(received) >= 1)

	lookup = received[0]
	assert isinstance(lookup["nyc"], InnerModel)
	assert lookup["la"].city == "LA"


async def test_optional_model_none_round_trips(make_app_with_results):
	app = await make_app_with_results()
	received: list[InnerModel | None] = []

	@app.task
	async def maybe_inner(loc: InnerModel | None) -> str:
		received.append(loc)
		return "none" if loc is None else loc.city

	await maybe_inner.q(loc=None)
	await _run_worker_until(app, lambda: len(received) >= 1)

	assert received[0] is None


async def test_optional_model_present_round_trips(make_app_with_results):
	app = await make_app_with_results()
	received: list[InnerModel | None] = []

	@app.task
	async def maybe_inner(loc: InnerModel | None) -> str:
		received.append(loc)
		return "none" if loc is None else loc.city

	await maybe_inner.q(loc=InnerModel(city="SF", zip_code="94102"))
	await _run_worker_until(app, lambda: len(received) >= 1)

	assert isinstance(received[0], InnerModel)
	assert received[0].city == "SF"


async def test_list_and_dict_models_together(make_app_with_results):
	app = await make_app_with_results()
	received: list[tuple[list[OrderLine], dict[str, OrderLine]]] = []

	@app.task
	async def bulk(lines: list[OrderLine], by_sku: dict[str, OrderLine]) -> int:
		received.append((lines, by_sku))
		return len(lines) + len(by_sku)

	await bulk.q(
		lines=[OrderLine(sku="A", qty=1, price=1.0), OrderLine(sku="B", qty=2)],
		by_sku={"C": OrderLine(sku="C", qty=3, price=5.0)},
	)
	await _run_worker_until(app, lambda: len(received) >= 1)

	lines, by_sku = received[0]
	assert lines[0].price == 1.0
	assert by_sku["C"].qty == 3
