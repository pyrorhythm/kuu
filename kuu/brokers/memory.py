from __future__ import annotations

import heapq
import itertools
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import NamedTuple

import anyio
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from ..exceptions import InvalidReceiptType
from ..message import Message
from .base import Broker, Delivery


class MemoryReceipt(NamedTuple):
	queue: str
	seq: int


class _Q(NamedTuple):
	send: MemoryObjectSendStream[Delivery[MemoryReceipt]]
	recv: MemoryObjectReceiveStream[Delivery[MemoryReceipt]]


class MemoryBroker(Broker):
	def __init__(self, buffer: int = 1024, pump_interval: float = 0.05):
		"""
		In-memory broker for tests and single-process setups.

		Uses anyio memory streams plus a heap for scheduled messages.

		- `buffer`: max buffered deliveries per queue.
		- `pump_interval`: seconds between scheduled-message pump checks.
		"""

		self.buffer = buffer
		self.pump_interval = pump_interval
		self._queues: dict[str, _Q] = {}
		# (run_at_ts, seq, queue, message)
		self._scheduled: list[tuple[float, int, str, Message]] = []
		self._sched_lock = anyio.Lock()
		self._sched_event = anyio.Event()
		self._seq = itertools.count()
		self._pending: dict[tuple[str, int], Message] = {}

	async def connect(self) -> None:
		return None  # noop

	async def close(self) -> None:
		for q in self._queues.values():
			await q.send.aclose()
			await q.recv.aclose()
		self._queues.clear()
		self._scheduled.clear()
		self._pending.clear()

	async def declare(self, queue: str) -> None:
		if queue in self._queues:
			return
		send, recv = anyio.create_memory_object_stream[Delivery[MemoryReceipt]](self.buffer)
		self._queues[queue] = _Q(send, recv)

	def _now(self) -> float:
		return datetime.now(timezone.utc).timestamp()

	def _ts(self, dt: datetime) -> float:
		return dt.replace(tzinfo=timezone.utc).timestamp() if dt.tzinfo is None else dt.timestamp()

	async def _push(self, queue: str, msg: Message) -> None:
		await self.declare(queue)
		seq = next(self._seq)
		receipt = MemoryReceipt(queue=queue, seq=seq)
		self._pending[(queue, seq)] = msg
		await self._queues[queue].send.send(Delivery(message=msg, receipt=receipt, queue=queue))

	async def enqueue(self, msg: Message) -> None:
		await self._push(msg.queue, msg)

	async def schedule(self, msg: Message, not_before: datetime) -> None:
		await self.declare(msg.queue)
		async with self._sched_lock:
			heapq.heappush(self._scheduled, (self._ts(not_before), next(self._seq), msg.queue, msg))
			self._sched_event.set()
			self._sched_event = anyio.Event()

	async def _pump_scheduled(self) -> None:
		while True:
			async with self._sched_lock:
				now = self._now()
				due: list[tuple[str, Message]] = []
				while self._scheduled and self._scheduled[0][0] <= now:
					_, _, q, m = heapq.heappop(self._scheduled)
					due.append((q, m))
			for q, m in due:
				await self._push(q, m)
			await anyio.sleep(self.pump_interval)

	async def consume(
		self, queues: list[str], prefetch: int
	) -> AsyncIterator[Delivery[MemoryReceipt]]:
		del prefetch  # buffer size on the memory stream controls prefetch
		for q in queues:
			await self.declare(q)

		send, recv = anyio.create_memory_object_stream[Delivery[MemoryReceipt]](self.buffer)

		async def _forward(q: str) -> None:
			async for delivery in self._queues[q].recv.clone():
				await send.send(delivery)

		async with anyio.create_task_group() as tg:
			tg.start_soon(self._pump_scheduled)
			for q in queues:
				tg.start_soon(_forward, q)
			try:
				async with recv:
					async for delivery in recv:
						yield delivery
			finally:
				tg.cancel_scope.cancel()

	async def ack(self, delivery: Delivery) -> None:
		match delivery.receipt:
			case MemoryReceipt():
				pass
			case _:
				raise InvalidReceiptType(type(delivery.receipt))
		self._pending.pop((delivery.receipt.queue, delivery.receipt.seq), None)

	async def nack(
		self,
		delivery: Delivery,
		requeue: bool = True,
		delay: float | None = None,
	) -> None:
		match delivery.receipt:
			case MemoryReceipt():
				pass
			case _:
				raise InvalidReceiptType(type(delivery.receipt))
		key = (delivery.receipt.queue, delivery.receipt.seq)
		self._pending.pop(key, None)
		if not requeue:
			return
		msg = delivery.message.model_copy(update={"attempt": delivery.message.attempt + 1})
		if delay and delay > 0:
			when = datetime.fromtimestamp(self._now() + delay, tz=timezone.utc)
			await self.schedule(msg, when)
		else:
			await self._push(delivery.receipt.queue, msg)
