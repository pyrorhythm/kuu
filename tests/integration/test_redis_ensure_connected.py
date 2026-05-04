from __future__ import annotations

from datetime import datetime, timedelta, timezone

import anyio
import pytest

from kuu.brokers.base import Delivery
from kuu.brokers.redis import RedisBroker
from kuu.exceptions import NotConnected
from kuu.message import Message, Payload


def _msg(queue: str = "q", **kw) -> Message:
	return Message(task="t", queue=queue, payload=Payload(), **kw)


async def test_connect_is_idempotent(redis_flushed: str):
	broker = RedisBroker(
		url=redis_flushed, group="g_idem", stream_prefix="idem:s:", zset_prefix="idem:z:"
	)
	await broker.connect()
	sha1 = broker._move_sha
	assert sha1 is not None

	await broker.connect()
	assert broker._move_sha == sha1

	await broker.close()


async def test_connect_sets_move_sha(redis_flushed: str):
	broker = RedisBroker(
		url=redis_flushed, group="g_sha", stream_prefix="sha:s:", zset_prefix="sha:z:"
	)
	assert broker._move_sha is None

	await broker.connect()
	assert broker._move_sha is not None
	assert len(broker._move_sha) == 40  # SHA1 hex

	await broker.close()


@pytest.mark.anyio
async def test_close_clears_move_sha(redis_flushed: str):
	"""close() must reset _move_sha to None."""
	broker = RedisBroker(
		url=redis_flushed, group="g_cls", stream_prefix="cls:s:", zset_prefix="cls:z:"
	)
	await broker.connect()
	assert broker._move_sha is not None

	await broker.close()
	assert broker._move_sha is None
	assert broker._redis.r is None


@pytest.mark.anyio
async def test_reconnect_after_close_restores_move_sha(redis_flushed: str):
	"""After close(), a fresh connect() must re-load the script."""
	broker = RedisBroker(
		url=redis_flushed, group="g_reco", stream_prefix="reco:s:", zset_prefix="reco:z:"
	)
	await broker.connect()
	sha1 = broker._move_sha

	await broker.close()
	assert broker._move_sha is None

	await broker.connect()
	assert broker._move_sha is not None
	assert broker._move_sha == sha1

	await broker.close()


# ── partial connect: script_load fails ─────────────────────────────────


@pytest.mark.anyio
async def test_partial_connect_script_load_fails(redis_flushed: str):
	"""If script_load raises, _redis.r is set but _move_sha stays None.

	This is the root cause of the NotConnected traceback: subsequent
	connect() calls early-return because _redis.r is not None, so
	_move_sha never gets populated.
	"""
	broker = RedisBroker(
		url=redis_flushed, group="g_part", stream_prefix="part:s:", zset_prefix="part:z:"
	)

	# Make first script_load fail by patching script_load on the transport
	original_script_load = None
	call_count = 0

	orig_connect = broker._redis.connect

	async def _patched_connect():
		await orig_connect()
		# After transport connect, monkeypatch script_load to fail once
		nonlocal original_script_load, call_count
		if call_count == 0:
			original_script_load = broker.r.script_load
			broker.r.script_load = _fail_once  # type: ignore[assignment]
		call_count += 1

	async def _fail_once(*a, **kw):
		broker.r.script_load = original_script_load  # type: ignore[assignment]
		raise RuntimeError("simulated script_load failure")

	broker._redis.connect = _patched_connect  # type: ignore[assignment]

	with pytest.raises(RuntimeError, match="simulated"):
		await broker.connect()

	# After partial failure: _redis.r is set but _move_sha is None
	assert broker._redis.r is not None
	assert broker._move_sha is None

	await broker.close()


@pytest.mark.anyio
async def test_connect_reloads_script_when_sha_missing(redis_flushed: str):
	"""connect() must detect _move_sha is None and re-load even if _redis.r is set.

	This is the FIX for the bug. connect() now checks both _redis.r and _move_sha.
	"""
	broker = RedisBroker(
		url=redis_flushed, group="g_force", stream_prefix="force:s:", zset_prefix="force:z:"
	)

	# Simulate partial connect: transport connected but script not loaded
	await broker._redis.connect()
	assert broker._redis.r is not None
	assert broker._move_sha is None

	# connect() should re-load the script
	await broker.connect()
	assert broker._move_sha is not None
	assert len(broker._move_sha) == 40

	await broker.close()


@pytest.mark.anyio
async def test_ensure_connected_recovers_from_partial_connect(redis_flushed: str):
	"""After a partial connect, _ensure_connected on a method should fix it."""
	broker = RedisBroker(
		url=redis_flushed, group="g_retry", stream_prefix="retry:s:", zset_prefix="retry:z:"
	)

	# Simulate partial connect: _redis.r set, _move_sha not
	await broker._redis.connect()
	assert broker._redis.r is not None
	assert broker._move_sha is None

	# Any @_ensure_connected method should trigger connect() which now fixes it
	await broker.declare("q")
	assert broker._move_sha is not None

	await broker.close()


# ── _pump_scheduled edge cases ─────────────────────────────────────────


@pytest.mark.anyio
async def test_pump_scheduled_raises_when_sha_missing(redis_flushed: str):
	"""_pump_scheduled must raise NotConnected if _move_sha is None.

	We simulate this by creating a broker with a connected transport but
	no _move_sha, and patching connect() to be a no-op so _ensure_connected
	doesn't fix it.
	"""
	broker = RedisBroker(
		url=redis_flushed, group="g_pump_no", stream_prefix="pump_no:s:", zset_prefix="pump_no:z:"
	)
	await broker.connect()
	assert broker._move_sha is not None

	# Force _move_sha to None without calling close()
	broker._move_sha = None

	# Patch connect to be a no-op so _ensure_connected doesn't fix it
	async def _noop():
		pass

	broker.connect = _noop  # type: ignore[assignment]

	with pytest.raises(NotConnected):
		await broker._pump_scheduled([])


@pytest.mark.anyio
async def test_pump_scheduled_does_not_raise_when_sha_set(redis_flushed: str):
	"""_pump_scheduled should proceed if _move_sha is populated."""
	broker = RedisBroker(
		url=redis_flushed, group="g_pump", stream_prefix="pump:s:", zset_prefix="pump:z:"
	)
	await broker.connect()
	assert broker._move_sha is not None

	with anyio.move_on_after(0.3):
		await broker._pump_scheduled(["q"])

	await broker.close()


# ── ensure_connected decorator behavior ────────────────────────────────


@pytest.mark.anyio
async def test_declare_ensures_connection(redis_flushed: str):
	"""declare() with @_ensure_connected must connect before running."""
	broker = RedisBroker(
		url=redis_flushed, group="g_decl", stream_prefix="decl:s:", zset_prefix="decl:z:"
	)
	assert broker._redis.r is None

	await broker.declare("q")
	assert broker._redis.r is not None
	assert broker._move_sha is not None

	await broker.close()


@pytest.mark.anyio
async def test_enqueue_ensures_connection(redis_flushed: str):
	"""enqueue() with @_ensure_connected must connect before running."""
	broker = RedisBroker(
		url=redis_flushed, group="g_enq", stream_prefix="enq:s:", zset_prefix="enq:z:"
	)
	assert broker._redis.r is None

	msg = _msg()
	await broker.enqueue(msg)
	assert broker._redis.r is not None

	await broker.close()


@pytest.mark.anyio
async def test_schedule_ensures_connection(redis_flushed: str):
	"""schedule() with @_ensure_connected must connect before running."""
	broker = RedisBroker(
		url=redis_flushed, group="g_sched", stream_prefix="sched:s:", zset_prefix="sched:z:"
	)
	assert broker._redis.r is None

	when = datetime.now(timezone.utc) + timedelta(seconds=60)
	await broker.schedule(_msg(), when)
	assert broker._redis.r is not None

	await broker.close()


@pytest.mark.anyio
async def test_ack_ensures_connection(redis_flushed: str):
	"""ack() with @_ensure_connected reconnects if broker was closed."""
	broker = RedisBroker(
		url=redis_flushed, group="g_ack", stream_prefix="ack:s:", zset_prefix="ack:z:"
	)
	await broker.connect()
	await broker.declare("q")
	await broker.enqueue(_msg())

	# Read delivery directly via XREADGROUP (bypass consume() generator)
	resp = await broker.r.xreadgroup(
		broker.group, broker.consumer, {broker._stream("q"): ">"}, count=1, block=1000
	)
	assert resp is not None
	stream_name, entries = resp[0]
	sid, data = entries[0]
	from kuu.brokers.redis import RedisReceipt

	delivery = Delivery(
		message=broker.serializer.unmarshal(data[b"m"], into=Message),
		receipt=RedisReceipt(queue="q", stream_id=sid),
		queue="q",
	)

	# Close transport directly (not via close() which tries to cleanup async gen)
	await broker._redis.close()
	broker._move_sha = None
	assert broker._redis.r is None

	# ack — @_ensure_connected should reconnect
	await broker.ack(delivery)
	assert broker._redis.r is not None
	assert broker._move_sha is not None

	await broker._redis.close()


@pytest.mark.anyio
async def test_nack_ensures_connection(redis_flushed: str):
	"""nack() with @_ensure_connected reconnects if broker was closed."""
	broker = RedisBroker(
		url=redis_flushed, group="g_nack", stream_prefix="nack:s:", zset_prefix="nack:z:"
	)
	await broker.connect()
	await broker.declare("q")
	await broker.enqueue(_msg())

	resp = await broker.r.xreadgroup(
		broker.group, broker.consumer, {broker._stream("q"): ">"}, count=1, block=1000
	)
	assert resp is not None
	stream_name, entries = resp[0]
	sid, data = entries[0]
	from kuu.brokers.redis import RedisReceipt

	delivery = Delivery(
		message=broker.serializer.unmarshal(data[b"m"], into=Message),
		receipt=RedisReceipt(queue="q", stream_id=sid),
		queue="q",
	)

	await broker._redis.close()
	broker._move_sha = None
	assert broker._redis.r is None

	await broker.nack(delivery, requeue=False)
	assert broker._redis.r is not None
	assert broker._move_sha is not None

	await broker._redis.close()


# ── _pump_scheduled integration ────────────────────────────────────────


@pytest.mark.anyio
async def test_pump_scheduled_moves_due_messages(redis_flushed: str):
	"""_pump_scheduled must move scheduled messages to the stream when due."""
	broker = RedisBroker(
		url=redis_flushed,
		group="g_pump_ok",
		stream_prefix="pump_ok:s:",
		zset_prefix="pump_ok:z:",
		block_ms=200,
	)
	await broker.connect()
	try:
		await broker.declare("q")

		# Schedule a message due immediately (add directly to zset)
		msg = _msg()
		past = datetime.now(timezone.utc) - timedelta(seconds=1)
		await broker.r.zadd(broker._zset("q"), {broker.serializer.marshal(msg): past.timestamp()})

		# Pump should move it to the stream
		with anyio.move_on_after(0.8):
			await broker._pump_scheduled(["q"])

		count = await broker.r.xlen(broker._stream("q"))
		assert count == 1, f"expected 1 message in stream, got {count}"
	finally:
		await broker.close()


@pytest.mark.anyio
async def test_pump_scheduled_does_not_move_future_messages(redis_flushed: str):
	"""Messages scheduled for the future must NOT be pumped yet."""
	broker = RedisBroker(
		url=redis_flushed,
		group="g_pump_fut",
		stream_prefix="pump_fut:s:",
		zset_prefix="pump_fut:z:",
		block_ms=200,
	)
	await broker.connect()
	try:
		await broker.declare("q")

		msg = _msg()
		future = datetime.now(timezone.utc) + timedelta(hours=1)
		await broker.r.zadd(broker._zset("q"), {broker.serializer.marshal(msg): future.timestamp()})

		with anyio.move_on_after(0.8):
			await broker._pump_scheduled(["q"])

		count = await broker.r.xlen(broker._stream("q"))
		assert count == 0

		zcount = await broker.r.zcard(broker._zset("q"))
		assert zcount == 1
	finally:
		await broker.close()


@pytest.mark.anyio
async def test_pump_scheduled_multiple_queues(redis_flushed: str):
	"""_pump_scheduled must pump across multiple queues concurrently."""
	broker = RedisBroker(
		url=redis_flushed,
		group="g_pump_multi",
		stream_prefix="pump_multi:s:",
		zset_prefix="pump_multi:z:",
		block_ms=200,
	)
	await broker.connect()
	try:
		past = datetime.now(timezone.utc) - timedelta(seconds=1)
		for q in ["a", "b", "c"]:
			await broker.declare(q)
			msg = _msg(queue=q)
			await broker.r.zadd(broker._zset(q), {broker.serializer.marshal(msg): past.timestamp()})

		with anyio.move_on_after(0.8):
			await broker._pump_scheduled(["a", "b", "c"])

		for q in ["a", "b", "c"]:
			count = await broker.r.xlen(broker._stream(q))
			assert count == 1, f"queue {q}: expected 1 message in stream, got {count}"
	finally:
		await broker.close()


# ── consume + _pump_scheduled interaction ──────────────────────────────


@pytest.mark.anyio
async def test_consume_delivers_scheduled_message_via_pump(redis_flushed: str):
	"""End-to-end: schedule a message, consume() must deliver it after pump moves it."""
	broker = RedisBroker(
		url=redis_flushed,
		group="g_e2e_sched",
		stream_prefix="e2e_sched:s:",
		zset_prefix="e2e_sched:z:",
		block_ms=200,
	)
	await broker.connect()
	try:
		await broker.declare("q")

		msg = _msg()
		when = datetime.now(timezone.utc) + timedelta(milliseconds=200)
		await broker.r.zadd(broker._zset("q"), {broker.serializer.marshal(msg): when.timestamp()})

		deliveries = []

		async def _consumer(scope: anyio.CancelScope):
			async for delivery in broker.consume(["q"], prefetch=1):
				deliveries.append(delivery)
				await broker.ack(delivery)
				scope.cancel()
				return

		with anyio.fail_after(10.0):
			async with anyio.create_task_group() as tg:
				tg.start_soon(_consumer, tg.cancel_scope)

		assert len(deliveries) == 1
		assert deliveries[0].message.id == msg.id
	finally:
		await broker.close()


@pytest.mark.anyio
async def test_consume_handles_empty_queues_without_error(redis_flushed: str):
	"""consume() with no messages should not crash."""
	broker = RedisBroker(
		url=redis_flushed,
		group="g_empty",
		stream_prefix="empty:s:",
		zset_prefix="empty:z:",
		block_ms=100,
	)
	await broker.connect()
	try:
		await broker.declare("q")

		count = 0
		with anyio.move_on_after(0.5):
			async for delivery in broker.consume(["q"], prefetch=1):
				count += 1
				await broker.ack(delivery)

		assert count == 0
	finally:
		await broker.close()
