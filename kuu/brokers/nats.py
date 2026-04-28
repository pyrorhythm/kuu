from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any

import anyio
import nats
from nats.aio.msg import Msg
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig, RetentionPolicy, StreamConfig

from kuu.exceptions import NotConnected

from .._types import _ensure_connected
from ..exceptions import InvalidReceiptType
from ..message import Message
from ..serializers import JSONSerializer, Serializer
from .base import Broker, Delivery

NatsReceipt = Msg


class NatsBroker(Broker):
	servers: str | list[str]
	stream: str
	sp: str
	dp: str
	fetch_timeout: float
	serializer: Serializer

	_nc: Any
	_js: JetStreamContext | None
	_declared: set[str]
	_scheduled: list[tuple[float, Message]]
	_sched_lock: anyio.Lock
	_sched_event: anyio.Event

	def __init__(
		self,
		servers: str | list[str] = "nats://localhost:4222",
		stream: str = "QQ",
		subject_prefix: str = "qq.q.",
		durable_prefix: str = "qq-",
		fetch_timeout: float = 5.0,
		serializer: Serializer = JSONSerializer(),
	) -> None:
		"""
		NATS JetStream broker.

		- `servers`: NATS URL or list of URLs.
		- `stream`: JetStream stream name.
		- `subject_prefix`: prefix prepended to per-queue subjects.
		- `durable_prefix`: prefix for durable consumer names.
		- `fetch_timeout`: pull-fetch timeout in seconds.
		- `serializer`: message serializer.
		"""
		self.servers = servers
		self.stream = stream
		self.sp = subject_prefix
		self.dp = durable_prefix
		self.fetch_timeout = fetch_timeout
		self.serializer = serializer

		self._init_private()

	def _init_private(self) -> None:
		self._nc = None
		self._js = None
		self._declared = set()
		self._scheduled = []
		self._sched_lock = anyio.Lock()
		self._sched_event = anyio.Event()

	def _subject(self, queue: str) -> str:
		return f"{self.sp}{queue}"

	def _durable(self, queue: str) -> str:
		return f"{self.dp}{queue}"

	@property
	def js(self) -> JetStreamContext:
		if self._js is None:
			raise NotConnected("nats broker not connected")

		return self._js

	async def connect(self) -> None:
		if self._js is not None:
			return
		self._nc = await nats.connect(self.servers)
		self._js = self._nc.jetstream()
		try:
			await self._js.add_stream(
				StreamConfig(
					name=self.stream,
					subjects=[f"{self.sp}>"],
					retention=RetentionPolicy.WORK_QUEUE,
				)
			)
		except Exception:
			pass

	async def close(self) -> None:
		if self._nc is not None:
			await self._nc.drain()
			self._nc = None
			self._js = None

	@_ensure_connected
	async def declare(self, queue: str) -> None:
		if queue in self._declared:
			return
		await self.js.add_consumer(
			stream=self.stream,
			config=ConsumerConfig(
				durable_name=self._durable(queue),
				filter_subject=self._subject(queue),
				ack_wait=60,
				max_deliver=-1,
			),
		)
		self._declared.add(queue)

	@_ensure_connected
	async def enqueue(self, msg: Message) -> None:
		await self.js.publish(self._subject(msg.queue), self.serializer.marshal(msg))

	@_ensure_connected
	async def schedule(self, msg: Message, not_before: datetime) -> None:
		ts = (
			not_before.timestamp()
			if not_before.tzinfo
			else not_before.replace(tzinfo=timezone.utc).timestamp()
		)
		async with self._sched_lock:
			self._scheduled.append((ts, msg))
			self._scheduled.sort(key=lambda x: x[0])
		self._sched_event.set()

	async def _pump_scheduled(self) -> None:
		while True:
			async with self._sched_lock:
				now = datetime.now(timezone.utc).timestamp()
				due: list[Message] = []
				while self._scheduled and self._scheduled[0][0] <= now:
					due.append(self._scheduled.pop(0)[1])
				wait = self._scheduled[0][0] - now if self._scheduled else 1.0
			for m in due:
				await self.enqueue(m)
			self._sched_event = anyio.Event() if self._sched_event.is_set() else self._sched_event
			with anyio.move_on_after(max(0.05, min(wait, 1.0))):
				await self._sched_event.wait()

	async def consume(self, queues: list[str], prefetch: int) -> AsyncIterator[Delivery[Msg]]:
		for q in queues:
			await self.declare(q)

		subs = {
			q: await self.js.pull_subscribe(self._subject(q), durable=self._durable(q))
			for q in queues
		}

		async with anyio.create_task_group() as tg:
			tg.start_soon(self._pump_scheduled)

			try:
				while True:
					for q, sub in subs.items():
						try:
							msgs = await sub.fetch(batch=prefetch, timeout=self.fetch_timeout)
						except TimeoutError:
							continue
						except Exception:
							await anyio.sleep(0.5)
							continue
						for m in msgs:
							yield Delivery(
								message=self.serializer.unmarshal(m.data, into=Message),
								receipt=m,
								queue=q,
							)
			finally:
				tg.cancel_scope.cancel("failed to consume")
				self._sched_event.set()

	async def ack(self, delivery: Delivery) -> None:
		match delivery.receipt:
			case NatsReceipt():
				await delivery.receipt.ack()
			case _:
				raise InvalidReceiptType(type(delivery.receipt))

	async def nack(
		self, delivery: Delivery, requeue: bool = True, delay: float | None = None
	) -> None:
		match delivery.receipt:
			case NatsReceipt():
				if not requeue:
					await delivery.receipt.term()
					return
				await delivery.receipt.nak(delay=delay)
			case _:
				raise InvalidReceiptType(type(delivery.receipt))
