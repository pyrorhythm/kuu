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


class Broker[Receipt](Protocol):
	"""Transport for enqueuing and consuming task messages."""

	async def connect(self) -> None:
		"""Open the broker connection."""

	async def close(self) -> None:
		"""Close the broker connection."""

	async def declare(self, queue: str) -> None:
		"""Declare `queue` so it can be consumed from."""

	async def enqueue(self, msg: Message) -> None:
		"""Publish `msg` to its queue for immediate dispatch."""

	async def schedule(self, msg: Message, not_before: datetime) -> None:
		"""Schedule `msg` to become deliverable at or after `not_before`."""

	def consume(self, queues: list[str], prefetch: int) -> AsyncIterator[Delivery[Receipt]]:
		"""
		Stream deliveries from `queues`, prefetching up to `prefetch` per pull.

		The returned async iterator yields `Delivery` objects whose `receipt`
		is typed against the concrete broker.
		"""

	async def ack(self, delivery: Delivery[Receipt]) -> None:
		"""
		Acknowledge `delivery` as successfully processed.

		Raises `InvalidReceiptType` when a `Delivery` from one broker is
		passed to a different broker (e.g. Redis receipt fed into NATS).
		"""

	async def nack(self,
	               delivery: Delivery[Receipt],
	               requeue: bool = True,
	               delay: float | None = None) -> None:
		"""
		Negatively acknowledge `delivery`.

		- `requeue`: when true, the message is put back for another attempt.
		- `delay`: seconds to wait before re-delivery. `None` requeues immediately.

		Raises `InvalidReceiptType` when receipts cross brokers.
		"""
