from __future__ import annotations

from datetime import datetime, timedelta

import anyio
import pytest

from kuu._util import utcnow
from kuu.brokers.base import Broker, Delivery
from kuu.message import Message, Payload

pytestmark = pytest.mark.anyio


def _msg(queue: str = "q", **kw) -> Message:
	return Message(task="t", queue=queue, payload=Payload(), **kw)


async def _consume_until(
	broker: Broker,
	queue: str,
	on_delivery,
	*,
	count: int,
	timeout: float = 10.0,
) -> list[Delivery]:
	results: list[Delivery] = []

	async def _worker(scope: anyio.CancelScope):
		async for delivery in broker.consume([queue], prefetch=8):
			idx = len(results)
			results.append(delivery)
			await on_delivery(idx, delivery, broker)
			if len(results) >= count:
				scope.cancel()

	with anyio.fail_after(timeout):
		async with anyio.create_task_group() as tg:
			tg.start_soon(_worker, tg.cancel_scope)

	return results


async def test_enqueue_then_consume_round_trips_message(any_broker: Broker):
	await any_broker.declare("q")
	original = _msg()
	await any_broker.enqueue(original)

	async def ack(_i, d, b):
		await b.ack(d)

	[delivery] = await _consume_until(any_broker, "q", ack, count=1)
	assert delivery.message.id == original.id
	assert delivery.message.task == "t"


async def test_scheduled_message_is_held_until_due(any_broker: Broker):
	await any_broker.declare("q")
	when = utcnow() + timedelta(milliseconds=400)
	await any_broker.schedule(_msg(), when)

	delivered_at: list[datetime] = []

	async def record(_i, d, b):
		delivered_at.append(utcnow())
		await b.ack(d)

	await _consume_until(any_broker, "q", record, count=1, timeout=10.0)
	assert delivered_at[0] >= when - timedelta(milliseconds=150)


async def test_nack_with_delay_requeues_same_message(any_broker: Broker):
	await any_broker.declare("q")
	await any_broker.enqueue(_msg())

	async def handler(idx, delivery, b):
		if idx == 0:
			await b.nack(delivery, requeue=True, delay=0.2)
		else:
			await b.ack(delivery)

	results = await _consume_until(any_broker, "q", handler, count=2, timeout=10.0)
	assert results[0].message.id == results[1].message.id


async def test_nack_without_requeue_drops_message(any_broker: Broker):
	await any_broker.declare("q")
	await any_broker.enqueue(_msg())

	seen = 0

	async def drop(_i, d, b):
		nonlocal seen
		seen += 1
		await b.nack(d, requeue=False)

	async def _runner(scope: anyio.CancelScope):
		async for delivery in any_broker.consume(["q"], prefetch=4):
			await drop(seen, delivery, any_broker)
			if seen >= 1:
				await anyio.sleep(0.6)
				scope.cancel()

	with anyio.fail_after(5.0):
		async with anyio.create_task_group() as tg:
			tg.start_soon(_runner, tg.cancel_scope)

	assert seen == 1


async def test_consume_empty_queues_does_not_error(any_broker: Broker):
	await any_broker.declare("q")

	count = 0
	with anyio.move_on_after(0.5):
		async for delivery in any_broker.consume(["q"], prefetch=1):
			count += 1
			await any_broker.ack(delivery)

	assert count == 0
