from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any, NamedTuple

import aio_pika
import anyio
from msgspec import structs

from kuu._types import _ensure_connected
from kuu._util import utcnow
from kuu.exceptions import InvalidReceiptType
from kuu.message import Message
from kuu.serializers import JSONSerializer, Serializer
from kuu.transports.rabbitmq import RabbitMQTransport

from .base import Broker, Delivery

log = logging.getLogger("kuu.brokers.rabbitmq")


class RabbitMQReceipt(NamedTuple):
	queue: str
	message: Any


class RabbitMQBroker(Broker[RabbitMQReceipt]):
	"""RabbitMQ broker backed by :mod:`aio_pika`.

	Delayed retries are implemented by republishing from the worker process after
	an async sleep; use Redis if durable delayed scheduling matters.
	"""

	def __init__(
		self,
		url: str = "amqp://guest:guest@localhost/",
		*,
		transport: RabbitMQTransport | None = None,
		queue_prefix: str = "",
		durable: bool = True,
		serializer: Serializer = JSONSerializer(),
	) -> None:
		self._transport = transport or RabbitMQTransport(url)
		self.queue_prefix = queue_prefix
		self.durable = durable
		self.serializer = serializer
		self._declared: set[str] = set()
		self._scheduled: set[asyncio.Task[None]] = set()

	@property
	def transport(self) -> RabbitMQTransport:
		return self._transport

	def _queue(self, queue: str) -> str:
		return self._transport.queue_key(self.queue_prefix, queue)

	async def connect(self) -> None:
		await self._transport.connect()

	async def close(self) -> None:
		scheduled = list(self._scheduled)
		for task in scheduled:
			task.cancel()
		if scheduled:
			await asyncio.gather(*scheduled, return_exceptions=True)
		self._scheduled.clear()
		await self._transport.close()
		self._declared.clear()

	@_ensure_connected
	async def declare(self, queue: str) -> None:
		if queue in self._declared:
			return
		await self._transport.channel.declare_queue(self._queue(queue), durable=self.durable)
		self._declared.add(queue)

	@_ensure_connected
	async def enqueue(self, msg: Message) -> None:
		await self.declare(msg.queue)
		delivery_mode = (
			aio_pika.DeliveryMode.PERSISTENT
			if self.durable
			else aio_pika.DeliveryMode.NOT_PERSISTENT
		)
		await self._transport.channel.default_exchange.publish(
			aio_pika.Message(body=self.serializer.marshal(msg), delivery_mode=delivery_mode),
			routing_key=self._queue(msg.queue),
		)

	async def schedule(self, msg: Message, not_before: datetime) -> None:
		delay = max(0.0, (not_before - utcnow()).total_seconds())

		async def _later() -> None:
			try:
				await anyio.sleep(delay)
				await self.enqueue(msg)
			except asyncio.CancelledError:
				raise
			except Exception as e:
				log.exception("event=broker.schedule_failed queue=%s err=%r", msg.queue, e)

		task = asyncio.create_task(_later())
		self._scheduled.add(task)
		task.add_done_callback(self._scheduled.discard)

	async def consume(
		self, queues: list[str], prefetch: int
	) -> AsyncIterator[Delivery[RabbitMQReceipt]]:
		await self.connect()
		for queue in queues:
			await self.declare(queue)

		channel = await self._transport.connection.channel()
		await channel.set_qos(prefetch_count=max(1, prefetch))
		send, recv = anyio.create_memory_object_stream[Delivery[RabbitMQReceipt]](
			max(1, prefetch * max(1, len(queues)))
		)

		async def _forward(queue_name: str) -> None:
			queue = await channel.declare_queue(self._queue(queue_name), durable=self.durable)
			async with queue.iterator() as iterator:
				async for raw in iterator:
					msg = self.serializer.unmarshal(raw.body, into=Message)
					await send.send(
						Delivery(
							message=msg,
							receipt=RabbitMQReceipt(queue=queue_name, message=raw),
							queue=queue_name,
						)
					)

		tasks = [asyncio.create_task(_forward(queue)) for queue in queues]
		try:
			async with recv, send:
				async for delivery in recv:
					yield delivery
		finally:
			for task in tasks:
				task.cancel()
			await asyncio.gather(*tasks, return_exceptions=True)
			await channel.close()

	async def ack(self, delivery: Delivery) -> None:
		match delivery.receipt:
			case RabbitMQReceipt(message=raw):
				await raw.ack()
			case _:
				raise InvalidReceiptType(type(delivery.receipt))

	async def nack(
		self,
		delivery: Delivery,
		requeue: bool = True,
		delay: float | None = None,
	) -> None:
		match delivery.receipt:
			case RabbitMQReceipt(message=raw):
				pass
			case _:
				raise InvalidReceiptType(type(delivery.receipt))

		if not requeue:
			await raw.reject(requeue=False)
			return

		msg = structs.replace(delivery.message, attempt=delivery.message.attempt + 1)
		if delay and delay > 0:
			when = datetime.fromtimestamp(utcnow().timestamp() + delay, tz=timezone.utc)
			await self.schedule(msg, when)
		else:
			await self.enqueue(msg)
		await raw.ack()

	@_ensure_connected
	async def queue_depth(self, queue: str) -> int | None:
		try:
			q = await self._transport.channel.declare_queue(
				self._queue(queue), durable=self.durable
			)
			return int(q.declaration_result.message_count)
		except Exception as e:
			log.debug("event=broker.queue_depth_failed queue=%s err=%r", queue, e)
			return None
