from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from ..message import Message


@dataclass
class Delivery[Receipt]:
	message: Message
	receipt: Receipt
	queue: str


class Broker(Protocol):
	async def connect(self) -> None:
		"""Initialize connection to broker"""

	async def close(self) -> None:
		"""Close connection to broker"""

	async def declare(self, queue: str) -> None:
		"""
		Declare a queue to broker

		Args:
				queue: queue to declare
		"""

	async def enqueue(self, msg: Message) -> None:
		"""
		Enqueue message

		Args:
				msg: message to enqueue
		"""

	async def schedule(self, msg: Message, not_before: datetime) -> None:
		"""
		Schedule a message to execute at or after specified datetime

		Args:
				msg: message to schedule
				not_before: datetime by which message will be scheduled
		"""

	def consume(self, queues: list[str], prefetch: int) -> AsyncIterator[Delivery]:
		"""
		Consume messages from specified queues, prefetching them in batch of N

		Args:
				queues: queues from which deliveries would be consumed
				prefetch: size of a fetch batch

		Returns:
				AsyncIterator[Delivery], where Delivery.receipt is typed after Broker
		"""

	async def ack(self, delivery: Delivery) -> None:
		"""
		Acknowledge delivery

		Args:
				delivery;

		Raises:
				InvalidReceiptType: if Delivery from Redis got fed into NATS; when deliveries came from different brokers
		"""

	async def nack(
		self, delivery: Delivery, requeue: bool = True, delay: float | None = None
	) -> None:
		"""
		Negatively acknowledge delivery

		Args:
				delivery;
				requeue: If to requeue delivery in case of soft-failure. Defaults to True.
				delay: How delayed requeued message would be sent. `None` - instant.  Defaults to None.

		Raises:
				InvalidReceiptType: if Delivery from Redis got fed into NATS; when deliveries came from different brokers
		"""
