from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Awaitable, Callable, NamedTuple, cast

import anyio
from redis.asyncio import Redis

from ..exceptions import InvalidReceiptType, NotConnected
from ..message import Message
from ..serializers import JSONSerializer, Serializer
from .base import Broker, Delivery

_MOVE_LUA = """
local z = KEYS[1]
local s = KEYS[2]
local now = ARGV[1]
local limit = tonumber(ARGV[2])
local items = redis.call('ZRANGEBYSCORE', z, '-inf', now, 'LIMIT', 0, limit)
for i, raw in ipairs(items) do
  redis.call('XADD', s, '*', 'm', raw)
  redis.call('ZREM', z, raw)
end
return #items
"""

_KT = bytes | str | memoryview
_ST = int | _KT


def _asyncify[**P, T](fn: Callable[P, Awaitable[T] | T]) -> Callable[P, Awaitable[T]]:
	return cast(Callable[P, Awaitable[T]], fn)


class RedisReceipt(NamedTuple):
	queue: str
	stream_id: bytes


class RedisBroker(Broker):
	def __init__(
		self,
		url: str = "redis://localhost:6379/0",
		group: str = "qq",
		consumer: str = "c1",
		stream_prefix: str = "qq:s:",
		zset_prefix: str = "qq:z:",
		block_ms: int = 5000,
		claim_min_idle_ms: int = 60000,
		serializer: Serializer = JSONSerializer(),
	):
		self.url = url
		self.group = group
		self.consumer = consumer
		self.sp = stream_prefix
		self.zp = zset_prefix
		self.block_ms = block_ms
		self.claim_min_idle_ms = claim_min_idle_ms
		self.serializer = serializer
		self._r: Redis | None = None
		self._move_sha: str | None = None
		self._declared: set[str] = set()

	@property
	def r(self) -> Redis:
		assert self._r is not None, "broker not connected"
		return self._r

	def _stream(self, q: str) -> str:
		return f"{self.sp}{q}"

	def _zset(self, q: str) -> str:
		return f"{self.zp}{q}"

	async def connect(self) -> None:
		self._r = Redis.from_url(self.url)
		self._move_sha = await self._r.script_load(_MOVE_LUA)

	async def close(self) -> None:
		if self._r is not None:
			await self._r.aclose()
			self._r = None

	async def declare(self, queue: str) -> None:
		if queue in self._declared:
			return
		try:
			await self.r.xgroup_create(self._stream(queue), self.group, id="$", mkstream=True)
		except Exception as e:
			if "BUSYGROUP" not in str(e):
				raise
		self._declared.add(queue)

	async def enqueue(self, msg: Message) -> None:
		await self.declare(msg.queue)
		await self.r.xadd(self._stream(msg.queue), {"m": self.serializer.marshal(msg)})

	async def schedule(self, msg: Message, not_before: datetime) -> None:
		await self.declare(msg.queue)
		score = (
			not_before.replace(tzinfo=timezone.utc).timestamp()
			if not_before.tzinfo is None
			else not_before.timestamp()
		)
		await self.r.zadd(self._zset(msg.queue), {self.serializer.marshal(msg): score})

	async def _pump_scheduled(self, queues: list[str]) -> None:
		if self._move_sha is None:
			raise NotConnected

		while True:
			now = datetime.now(timezone.utc).timestamp()
			for q in queues:
				await _asyncify(self.r.evalsha)(
					self._move_sha, 2, self._zset(q), self._stream(q), str(now), "128"
				)
			await anyio.sleep(0.5)

	async def _claim_stale(self, queue: str) -> list[tuple[bytes, dict[bytes, bytes]]]:
		try:
			_, items, _ = await self.r.xautoclaim(
				self._stream(queue),
				self.group,
				self.consumer,
				min_idle_time=self.claim_min_idle_ms,
				count=32,
			)
			return items or []
		except Exception:
			return []

	async def consume(
		self, queues: list[str], prefetch: int
	) -> AsyncIterator[Delivery[RedisReceipt]]:
		for q in queues:
			await self.declare(q)

		async with anyio.create_task_group() as tg:
			tg.start_soon(self._pump_scheduled, queues)
			try:
				streams: dict[_KT, _ST] = {self._stream(q): ">" for q in queues}
				q_by_stream = {self._stream(q): q for q in queues}
				while True:
					for q in queues:
						for sid, data in await self._claim_stale(q):
							yield Delivery(
								message=self.serializer.unmarshal(data[b"m"], into=Message),
								receipt=RedisReceipt(queue=q, stream_id=sid),
								queue=q,
							)

					resp = await self.r.xreadgroup(
						self.group, self.consumer, streams, count=prefetch, block=self.block_ms
					)
					if not resp:
						continue
					for stream_name, entries in resp:
						q = q_by_stream[
							stream_name.decode() if isinstance(stream_name, bytes) else stream_name
						]
						for sid, data in entries:
							yield Delivery(
								message=self.serializer.unmarshal(data[b"m"], into=Message),
								receipt=RedisReceipt(queue=q, stream_id=sid),
								queue=q,
							)
			finally:
				tg.cancel_scope.cancel()

	async def ack(self, delivery: Delivery) -> None:
		match delivery.receipt:
			case RedisReceipt():
				pass
			case _:
				raise InvalidReceiptType(type(delivery.receipt))

		q, sid = delivery.receipt
		await self.r.xack(self._stream(q), self.group, sid)
		await self.r.xdel(self._stream(q), sid)

	async def nack(
		self,
		delivery: Delivery,
		requeue: bool = True,
		delay: float | None = None,
	) -> None:
		match delivery.receipt:
			case RedisReceipt():
				pass
			case _:
				raise InvalidReceiptType(type(delivery.receipt))

		q, sid = delivery.receipt
		if not requeue:
			await self.r.xack(self._stream(q), self.group, sid)
			await self.r.xdel(self._stream(q), sid)
			return
		msg = delivery.message.model_copy(update={"attempt": delivery.message.attempt + 1})
		if delay and delay > 0:
			when = datetime.now(timezone.utc).timestamp() + delay
			await self.r.zadd(self._zset(q), {self.serializer.marshal(msg): when})
		else:
			await self.r.xadd(self._stream(q), {"m": self.serializer.marshal(msg)})
		await self.r.xack(self._stream(q), self.group, sid)
		await self.r.xdel(self._stream(q), sid)
