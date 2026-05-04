from __future__ import annotations

from datetime import datetime, timedelta, timezone

import anyio
import pytest

from kuu.app import Kuu
from kuu.brokers.redis import RedisBroker
from kuu.config import Settings
from kuu.message import Message, Payload
from kuu.results.redis import RedisResults
from kuu.worker import Worker


async def _run_worker_briefly(app: Kuu, *, seconds: float = 3.0, queues=None):
	config = Settings.model_construct(
		queues=queues or ["default"],
		concurrency=4,
		prefetch=2,
		shutdown_timeout=2.0,
	)
	worker = Worker(config, app=app)

	with anyio.move_on_after(seconds):
		await worker.run()

	return worker


@pytest.mark.anyio
async def test_e2e_enqueue_consume_ack(redis_flushed: str):
	broker = RedisBroker(
		url=redis_flushed,
		group="e2e_ack_g",
		stream_prefix="e2e_ack:s:",
		zset_prefix="e2e_ack:z:",
		block_ms=200,
	)
	results = RedisResults(url=redis_flushed, prefix="e2e_ack:r:", ttl=60)
	app = Kuu(broker=broker, default_queue="default", results=results)

	delivered: list[str] = []

	@app.task
	async def hello(name: str) -> str:
		delivered.append(name)
		return f"hi {name}"

	await app.broker.connect()
	await app.results.connect()
	await app.broker.declare("default")

	await hello.q("world")
	await _run_worker_briefly(app, seconds=2.0)

	assert delivered == ["world"]

	await app.broker.close()
	await app.results.close()


@pytest.mark.anyio
async def test_e2e_scheduled_task_delivered_after_delay(redis_flushed: str):
	broker = RedisBroker(
		url=redis_flushed,
		group="e2e_sched_g",
		stream_prefix="e2e_sched:s:",
		zset_prefix="e2e_sched:z:",
		block_ms=200,
	)
	app = Kuu(broker=broker, default_queue="default")

	delivered_at: list[float] = []

	@app.task
	async def delayed() -> None:
		delivered_at.append(anyio.current_time())

	await app.broker.connect()
	await app.broker.declare("default")

	sent_at = anyio.current_time()
	when = datetime.now(timezone.utc) + timedelta(milliseconds=300)

	# Schedule directly via broker — use task FQN
	msg = Message(task=delayed.task_name, queue="default", payload=Payload(), not_before=when)
	await app.broker.schedule(msg, when)

	await _run_worker_briefly(app, seconds=3.0)

	assert len(delivered_at) == 1
	elapsed = delivered_at[0] - sent_at
	assert elapsed >= 0.25, f"delivered too early: {elapsed:.3f}s"

	await app.broker.close()


# ── nack with delay ────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_e2e_nack_with_delay_retries(redis_flushed: str):
	"""Task that fails once should be retried after delay, then succeed."""
	broker = RedisBroker(
		url=redis_flushed,
		group="e2e_nack_g",
		stream_prefix="e2e_nack:s:",
		zset_prefix="e2e_nack:z:",
		block_ms=200,
	)
	app = Kuu(broker=broker, default_queue="default")

	attempts: list[int] = []

	@app.task(max_attempts=3)
	async def flaky() -> str:
		if len(attempts) == 0:
			attempts.append(1)
			from kuu.exceptions import RetryErr

			raise RetryErr(delay=0.2, reason="first attempt fails")
		attempts.append(2)
		return "ok"

	await app.broker.connect()
	await app.broker.declare("default")

	await flaky.q()
	await _run_worker_briefly(app, seconds=4.0)

	assert len(attempts) == 2
	assert attempts == [1, 2]

	await app.broker.close()


# ── multiple tasks on different queues ─────────────────────────────────


@pytest.mark.anyio
async def test_e2e_multiple_queues(redis_flushed: str):
	"""Tasks on different queues are all consumed."""
	broker = RedisBroker(
		url=redis_flushed,
		group="e2e_multi_g",
		stream_prefix="e2e_multi:s:",
		zset_prefix="e2e_multi:z:",
		block_ms=200,
	)
	app = Kuu(broker=broker, default_queue="q1")

	results_a: list[str] = []
	results_b: list[str] = []

	@app.task(queue="q1")
	async def task_a(x: str) -> None:
		results_a.append(x)

	@app.task(queue="q2")
	async def task_b(x: str) -> None:
		results_b.append(x)

	await app.broker.connect()
	await app.broker.declare("q1")
	await app.broker.declare("q2")

	await task_a.q("a1")
	await task_b.q("b1")
	await task_a.q("a2")

	config = Settings.model_construct(
		queues=["q1", "q2"],
		concurrency=4,
		prefetch=2,
		shutdown_timeout=2.0,
	)
	worker = Worker(config, app=app)
	with anyio.move_on_after(3.0):
		await worker.run()

	assert sorted(results_a) == ["a1", "a2"]
	assert results_b == ["b1"]

	await app.broker.close()


# ── blocking task ──────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_e2e_blocking_task(redis_flushed: str):
	"""Blocking (sync) tasks run off the event loop thread."""
	import threading

	broker = RedisBroker(
		url=redis_flushed,
		group="e2e_block_g",
		stream_prefix="e2e_block:s:",
		zset_prefix="e2e_block:z:",
		block_ms=200,
	)
	app = Kuu(broker=broker, default_queue="default")

	main_thread = threading.get_ident()
	captured: dict = {}

	@app.task(blocking=True)
	def cpu_work(n: int) -> int:
		captured["thread"] = threading.get_ident()
		captured["result"] = n * n
		return n * n

	await app.broker.connect()
	await app.broker.declare("default")

	await cpu_work.q(7)
	await _run_worker_briefly(app, seconds=2.0)

	assert captured["result"] == 49
	assert captured["thread"] != main_thread

	await app.broker.close()


# ── max_attempts exhaustion → dead letter ──────────────────────────────


@pytest.mark.anyio
async def test_e2e_max_attempts_exhausted(redis_flushed: str):
	"""Task that always fails gets nack'd without requeue after max_attempts."""
	broker = RedisBroker(
		url=redis_flushed,
		group="e2e_dead_g",
		stream_prefix="e2e_dead:s:",
		zset_prefix="e2e_dead:z:",
		block_ms=200,
	)
	app = Kuu(broker=broker, default_queue="default")

	count = 0

	@app.task(max_attempts=2)
	async def always_fail() -> None:
		nonlocal count
		count += 1
		raise RuntimeError("permanent failure")

	await app.broker.connect()
	await app.broker.declare("default")

	await always_fail.q()
	await _run_worker_briefly(app, seconds=4.0)

	assert count == 2

	await app.broker.close()


# ── headers preserved through pipeline ─────────────────────────────────


@pytest.mark.anyio
async def test_e2e_headers_preserved(redis_flushed: str):
	"""Custom headers survive enqueue → consume → delivery."""
	broker = RedisBroker(
		url=redis_flushed,
		group="e2e_headers_g",
		stream_prefix="e2e_headers:s:",
		zset_prefix="e2e_headers:z:",
		block_ms=200,
	)
	app = Kuu(broker=broker, default_queue="default")

	delivered_headers: list[dict] = []

	@app.task
	async def noop() -> None:
		pass

	await app.broker.connect()
	await app.broker.declare("default")

	# Enqueue manually with headers
	msg = Message(
		task="noop",
		queue="default",
		payload=Payload(),
		headers={"trace-sched_id": "abc-123", "tenant": "acme"},
	)
	await app.broker.enqueue(msg)

	config = Settings.model_construct(
		queues=["default"],
		concurrency=1,
		prefetch=1,
		shutdown_timeout=2.0,
	)
	worker = Worker(config, app=app)

	# Intercept via monkeypatching _handle to capture headers
	original_handle = worker._handle

	async def _spy(delivery):
		delivered_headers.append(dict(delivery.message.headers))
		# Only capture first, then cancel
		await original_handle(delivery)

	worker._handle = _spy  # type: ignore[assignment]

	with anyio.move_on_after(3.0):
		await worker.run()

	assert len(delivered_headers) >= 1
	assert delivered_headers[0]["trace-sched_id"] == "abc-123"
	assert delivered_headers[0]["tenant"] == "acme"

	await app.broker.close()


# ── concurrent scheduled + immediate ───────────────────────────────────


@pytest.mark.anyio
async def test_e2e_mixed_immediate_and_scheduled(redis_flushed: str):
	"""Immediate and scheduled messages are both delivered correctly."""
	broker = RedisBroker(
		url=redis_flushed,
		group="e2e_mixed_g",
		stream_prefix="e2e_mixed:s:",
		zset_prefix="e2e_mixed:z:",
		block_ms=200,
	)
	app = Kuu(broker=broker, default_queue="default")

	results: list[tuple[str, float]] = []

	@app.task
	async def record(tag: str) -> None:
		results.append((tag, anyio.current_time()))

	await app.broker.connect()
	await app.broker.declare("default")

	# Immediate
	await record.q("immediate")

	# Scheduled 400ms out via broker directly
	when = datetime.now(timezone.utc) + timedelta(milliseconds=400)
	msg = Message(
		task=record.task_name,
		queue="default",
		payload=Payload(args=("scheduled",)),
		not_before=when,
	)
	await app.broker.schedule(msg, when)

	await _run_worker_briefly(app, seconds=4.0)

	tags = [t for t, _ in results]
	assert "immediate" in tags
	assert "scheduled" in tags

	# Scheduled should come after immediate
	sched_time = next(t for tag, t in results if tag == "scheduled")
	immed_time = next(t for tag, t in results if tag == "immediate")
	assert sched_time >= immed_time

	await app.broker.close()


# ── result retrieval via handle ────────────────────────────────────────


@pytest.mark.anyio
async def test_e2e_result_retrieval(redis_flushed: str):
	"""Task result can be retrieved via TaskHandle after worker processes it."""
	broker = RedisBroker(
		url=redis_flushed,
		group="e2e_result_g",
		stream_prefix="e2e_result:s:",
		zset_prefix="e2e_result:z:",
		block_ms=200,
	)
	results_backend = RedisResults(
		url=redis_flushed,
		prefix="e2e_result:r:",
		ttl=60,
		replay=True,
	)
	app = Kuu(broker=broker, default_queue="default", results=results_backend)

	@app.task
	async def add(a: int, b: int) -> int:
		return a + b

	await app.broker.connect()
	await app.results.connect()
	await app.broker.declare("default")

	handle = await add.q(3, 4)

	# Run worker to process
	await _run_worker_briefly(app, seconds=3.0)

	# Retrieve result — returns the decoded value directly
	value = await handle.result(timeout=5.0)
	assert value == 7

	await app.broker.close()
	await app.results.close()


# ── replay: same idempotency_key returns cached result ─────────────────


@pytest.mark.anyio
async def test_e2e_replay_cached_result(redis_flushed: str):
	"""Second enqueue with same idempotency_key returns cached result without re-running."""
	broker = RedisBroker(
		url=redis_flushed,
		group="e2e_replay_g",
		stream_prefix="e2e_replay:s:",
		zset_prefix="e2e_replay:z:",
		block_ms=200,
	)
	results_backend = RedisResults(
		url=redis_flushed,
		prefix="e2e_replay:r:",
		ttl=60,
		replay=True,
	)
	app = Kuu(broker=broker, default_queue="default", results=results_backend)

	run_count = 0

	@app.task
	async def counted(x: int) -> int:
		nonlocal run_count
		run_count += 1
		return x * 10

	await app.broker.connect()
	await app.results.connect()
	await app.broker.declare("default")

	# First run — use idempotency_key so replay can match
	enqueue = app._enqueue_task(counted, headers={"idempotency_key": "count-5"})
	h1 = await enqueue(5)
	await _run_worker_briefly(app, seconds=2.0)
	v1 = await h1.result(timeout=5.0)
	assert v1 == 50
	assert run_count == 1

	# Second run with same idempotency_key → should replay from cache
	h2 = await enqueue(5)
	await _run_worker_briefly(app, seconds=2.0)
	v2 = await h2.result(timeout=5.0)
	assert v2 == 50
	assert run_count == 1, f"expected replay (run_count=1), got run_count={run_count}"

	await app.broker.close()
	await app.results.close()
