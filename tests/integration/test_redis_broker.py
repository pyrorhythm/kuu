from __future__ import annotations

from datetime import datetime, timedelta, timezone

import anyio
import pytest

from kuu.brokers.redis import RedisBroker
from kuu.message import Message, Payload


def _msg(queue: str = "q", **kw) -> Message:
	return Message(task="t", queue=queue, payload=Payload(), **kw)


async def _consume_until(broker: RedisBroker, queue: str, on_delivery, *, count, timeout=10.0):
	results: list = []

	async def _worker(scope: anyio.CancelScope):
		async for delivery in broker.consume([queue], prefetch=8):
			idx = len(results)
			results.append(delivery)
			await on_delivery(idx, delivery, broker)
			if len(results) >= count:
				scope.cancel()
				return

	with anyio.fail_after(timeout):
		async with anyio.create_task_group() as tg:
			tg.start_soon(_worker, tg.cancel_scope)

	return results


@pytest.mark.anyio
async def test_enqueue_then_consume_round_trips_message(redis_flushed: str):
	broker = RedisBroker(
		url=redis_flushed, group="g1", stream_prefix="t1:s:", zset_prefix="t1:z:", block_ms=200
	)
	await broker.connect()
	try:
		await broker.declare("q")
		original = _msg()
		await broker.enqueue(original)

		async def ack(_i, d, b):
			await b.ack(d)

		[delivery] = await _consume_until(broker, "q", ack, count=1)
		assert delivery.message.id == original.id
		assert delivery.message.task == "t"
	finally:
		await broker.close()


@pytest.mark.anyio
async def test_scheduled_message_is_held_until_due(redis_flushed: str):
	broker = RedisBroker(
		url=redis_flushed, group="g2", stream_prefix="t2:s:", zset_prefix="t2:z:", block_ms=200
	)
	await broker.connect()
	try:
		await broker.declare("q")
		when = datetime.now(timezone.utc) + timedelta(milliseconds=400)
		await broker.schedule(_msg(), when)

		delivered_at: list[datetime] = []

		async def record(_i, d, b):
			delivered_at.append(datetime.now(timezone.utc))
			await b.ack(d)

		await _consume_until(broker, "q", record, count=1, timeout=10.0)
		assert delivered_at[0] >= when - timedelta(milliseconds=100)
	finally:
		await broker.close()


@pytest.mark.anyio
async def test_nack_with_delay_requeues_with_incremented_attempt(redis_flushed: str):
	broker = RedisBroker(
		url=redis_flushed, group="g3", stream_prefix="t3:s:", zset_prefix="t3:z:", block_ms=200
	)
	await broker.connect()
	try:
		await broker.declare("q")
		await broker.enqueue(_msg())

		async def handler(idx, delivery, b):
			if idx == 0:
				await b.nack(delivery, requeue=True, delay=0.2)
			else:
				await b.ack(delivery)

		results = await _consume_until(broker, "q", handler, count=2, timeout=10.0)
		assert results[0].message.attempt == 0
		assert results[1].message.attempt == 1
		assert results[0].message.id == results[1].message.id
	finally:
		await broker.close()


@pytest.mark.anyio
async def test_nack_without_requeue_drops_message(redis_flushed: str):
	broker = RedisBroker(
		url=redis_flushed, group="g4", stream_prefix="t4:s:", zset_prefix="t4:z:", block_ms=200
	)
	await broker.connect()
	try:
		await broker.declare("q")
		await broker.enqueue(_msg())

		seen = 0

		async def drop(_i, d, b):
			nonlocal seen
			seen += 1
			await b.nack(d, requeue=False)

		async def _runner(scope: anyio.CancelScope):
			async for delivery in broker.consume(["q"], prefetch=4):
				await drop(seen, delivery, broker)
				if seen >= 1:
					await anyio.sleep(0.6)
					scope.cancel()
					return

		with anyio.fail_after(5.0):
			async with anyio.create_task_group() as tg:
				tg.start_soon(_runner, tg.cancel_scope)

		assert seen == 1
	finally:
		await broker.close()
