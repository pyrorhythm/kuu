from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone

import anyio
import pytest

from kuu.brokers.base import Delivery
from kuu.brokers.redis import RedisBroker, RedisReceipt
from kuu.exceptions import NotConnected
from kuu.message import Message, Payload
from kuu.transports.redis import RedisTransport

pytestmark = pytest.mark.anyio


def _msg(queue: str = "q", **kw) -> Message:
	return Message(task="t", queue=queue, payload=Payload(), **kw)


def _broker(transport: RedisTransport, uid: str, **kw) -> RedisBroker:
	return RedisBroker(
		transport=transport,
		group=f"g_{uid}",
		consumer="c1",
		stream_prefix=f"s:{uid}:",
		zset_prefix=f"z:{uid}:",
		**kw,
	)


async def test_connect_is_idempotent(redis_transport: RedisTransport):
	broker = _broker(redis_transport, "idem")
	await broker.connect()
	sha1 = broker._move_sha
	assert sha1 is not None

	await broker.connect()
	assert broker._move_sha == sha1

	await broker.close()


async def test_connect_sets_move_sha(redis_transport: RedisTransport):
	broker = _broker(redis_transport, "sha")
	assert broker._move_sha is None

	await broker.connect()
	assert broker._move_sha is not None
	assert len(broker._move_sha) == 40

	await broker.close()


async def test_close_clears_move_sha(redis_transport: RedisTransport):
	broker = _broker(redis_transport, "cls")
	await broker.connect()
	assert broker._move_sha is not None

	await broker.close()
	assert broker._move_sha is None


async def test_reconnect_after_close_restores_move_sha(redis_transport: RedisTransport):
	broker = _broker(redis_transport, "reco")
	await broker.connect()
	sha1 = broker._move_sha

	await broker.close()
	assert broker._move_sha is None

	await broker.connect()
	assert broker._move_sha is not None
	assert broker._move_sha == sha1

	await broker.close()


async def test_partial_connect_script_load_fails(redis_transport: RedisTransport):
	"""If script_load raises, _redis.r is set but _move_sha stays None."""
	broker = _broker(redis_transport, "part")

	original_script_load = None
	call_count = 0
	orig_connect = broker._redis.connect

	async def _patched_connect():
		await orig_connect()
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

	assert broker._redis.r is not None
	assert broker._move_sha is None

	await broker.close()


async def test_connect_reloads_script_when_sha_missing(redis_transport: RedisTransport):
	broker = _broker(redis_transport, "force")

	await broker._redis.connect()
	assert broker._redis.r is not None
	assert broker._move_sha is None

	await broker.connect()
	assert broker._move_sha is not None
	assert len(broker._move_sha) == 40

	await broker.close()


async def test_ensure_connected_recovers_from_partial_connect(
	redis_transport: RedisTransport,
):
	broker = _broker(redis_transport, "retry")

	await broker._redis.connect()
	assert broker._redis.r is not None
	assert broker._move_sha is None

	await broker.declare("q")
	assert broker._move_sha is not None

	await broker.close()


async def test_pump_scheduled_raises_when_sha_missing(redis_transport: RedisTransport):
	broker = _broker(redis_transport, "pump_no")
	await broker.connect()
	assert broker._move_sha is not None

	broker._move_sha = None

	async def _noop():
		pass

	broker.connect = _noop  # type: ignore[assignment]

	with pytest.raises(NotConnected):
		await broker._pump_scheduled([])


async def test_pump_scheduled_does_not_raise_when_sha_set(
	redis_transport: RedisTransport,
):
	broker = _broker(redis_transport, "pump")
	await broker.connect()
	assert broker._move_sha is not None

	with anyio.move_on_after(0.3):
		await broker._pump_scheduled(["q"])

	await broker.close()


async def test_declare_ensures_connection(redis_transport: RedisTransport):
	broker = _broker(redis_transport, "decl")

	await broker.declare("q")
	assert broker._redis.r is not None
	assert broker._move_sha is not None

	await broker.close()


async def test_enqueue_ensures_connection(redis_transport: RedisTransport):
	broker = _broker(redis_transport, "enq")

	msg = _msg()
	await broker.enqueue(msg)
	assert broker._redis.r is not None

	await broker.close()


async def test_schedule_ensures_connection(redis_transport: RedisTransport):
	broker = _broker(redis_transport, "sched")

	when = datetime.now(timezone.utc) + timedelta(seconds=60)
	await broker.schedule(_msg(), when)
	assert broker._redis.r is not None

	await broker.close()


async def test_ack_ensures_connection(redis_transport: RedisTransport):
	broker = _broker(redis_transport, "ack")
	await broker.connect()
	await broker.declare("q")
	await broker.enqueue(_msg())

	resp = await broker.r.xreadgroup(
		broker.group, broker.consumer, {broker._stream("q"): ">"}, count=1, block=1000
	)
	assert resp is not None
	stream_name, entries = resp[0]
	sid, data = entries[0]

	delivery = Delivery(
		message=broker.serializer.unmarshal(data[b"m"], into=Message),
		receipt=RedisReceipt(queue="q", stream_id=sid),
		queue="q",
	)

	await broker._redis.close()
	broker._move_sha = None
	assert broker._redis.r is None

	await broker.ack(delivery)
	assert broker._redis.r is not None
	assert broker._move_sha is not None

	await broker._redis.close()


async def test_nack_ensures_connection(redis_transport: RedisTransport):
	broker = _broker(redis_transport, "nack")
	await broker.connect()
	await broker.declare("q")
	await broker.enqueue(_msg())

	resp = await broker.r.xreadgroup(
		broker.group, broker.consumer, {broker._stream("q"): ">"}, count=1, block=1000
	)
	assert resp is not None
	stream_name, entries = resp[0]
	sid, data = entries[0]

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


async def test_pump_scheduled_moves_due_messages(redis_transport: RedisTransport):
	broker = _broker(redis_transport, "pump_ok")
	await broker.connect()
	try:
		uid = secrets.token_hex(10)
		q = f"kuu_q_{uid}"
		await broker.declare(q)

		msg = _msg()
		past = datetime.now(timezone.utc) - timedelta(seconds=1)
		await broker.r.zadd(broker._zset(q), {broker.serializer.marshal(msg): past.timestamp()})

		with anyio.move_on_after(0.8):
			await broker._pump_scheduled([q])

		count = await broker.r.xlen(broker._stream(q))
		assert count == 1, f"expected 1 message in stream, got {count}"
	finally:
		await broker.close()
