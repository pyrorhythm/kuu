from __future__ import annotations

from datetime import datetime, timedelta, timezone

import anyio
import pytest

from kuu.brokers.memory import MemoryBroker
from kuu.message import Message, Payload


def _msg(**kw) -> Message:
	return Message(task="t", queue="q", payload=Payload(), **kw)


async def _consume_until(
	broker: MemoryBroker,
	queue: str,
	on_delivery,
	*,
	count: int,
	timeout: float = 2.0,
) -> list:
	results: list = []

	async def _worker(scope: anyio.CancelScope):
		async for delivery in broker.consume([queue], prefetch=1):
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
async def test_enqueue_then_consume_round_trips_message():
	broker = MemoryBroker()
	await broker.connect()
	original = _msg()
	await broker.enqueue(original)

	async def ack(_idx, d, b):
		await b.ack(d)

	[delivery] = await _consume_until(broker, "q", ack, count=1)
	assert delivery.message.id == original.id
	assert delivery.message.task == "t"


@pytest.mark.anyio
async def test_scheduled_message_is_held_until_due():
	broker = MemoryBroker(pump_interval=0.01)
	await broker.connect()
	when = datetime.now(timezone.utc) + timedelta(milliseconds=200)
	await broker.schedule(_msg(), when)

	delivered_at: list[datetime] = []

	async def record(_idx, d, b):
		delivered_at.append(datetime.now(timezone.utc))
		await b.ack(d)

	await _consume_until(broker, "q", record, count=1, timeout=2.0)
	assert delivered_at[0] >= when - timedelta(milliseconds=50)


@pytest.mark.anyio
async def test_nack_with_delay_requeues_with_incremented_attempt():
	broker = MemoryBroker(pump_interval=0.01)
	await broker.connect()
	await broker.enqueue(_msg())

	async def handler(idx, delivery, b):
		if idx == 0:
			await b.nack(delivery, requeue=True, delay=0.1)
		else:
			await b.ack(delivery)

	results = await _consume_until(broker, "q", handler, count=2, timeout=2.0)

	assert results[0].message.attempt == 0
	assert results[1].message.attempt == 1
	assert results[0].message.id == results[1].message.id
