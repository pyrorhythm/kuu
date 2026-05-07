from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Awaitable, Callable, NamedTuple, cast

import anyio
from msgspec import structs
from redis.asyncio import Redis, RedisCluster

from kuu._util import utcnow

from .._types import _ensure_connected
from ..exceptions import InvalidReceiptType, NotConnected
from ..message import Message
from ..serializers import JSONSerializer, Serializer
from ..transports.redis import RedisTransport, StandaloneConfig
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

type _KT = bytes | str | memoryview
type _ST = int | _KT


def _asyncify[**P, T](fn: Callable[P, Awaitable[T] | T]) -> Callable[P, Awaitable[T]]:
	return cast(Callable[P, Awaitable[T]], fn)


class RedisReceipt(NamedTuple):
	queue: str
	stream_id: bytes


class RedisBroker(Broker[RedisReceipt]):
	"""Redis Streams broker.

	Uses streams for the live queue and sorted sets for scheduled messages.
	Consumer groups handle competing consumers across worker subprocesses.

	- `url`: Redis connection URL (convenience alias for ``StandaloneConfig``).
	- `t`: pre-configured :class:`~kuu.transports.redis.RedisTransport`.
	  When given, ``url`` is ignored.
	- `group`: consumer group name.
	- `consumer`: consumer name within the group.
	- `stream_prefix`: key prefix for live streams.
	- `zset_prefix`: key prefix for scheduled sorted sets.
	- `block_ms`: blocking interval for ``XREADGROUP`` (ms).
	- `claim_min_idle_ms`: minimum idle time before claiming stale deliveries.
	- `serializer`: message serializer.
	"""

	def __init__(
		self,
		url: str = "redis://localhost:6379/0",
		*,
		transport: RedisTransport | None = None,
		group: str = "qq",
		consumer: str = "c1",
		stream_prefix: str = "qq:s:",
		zset_prefix: str = "qq:z:",
		block_ms: int = 5000,
		claim_min_idle_ms: int = 60000,
		serializer: Serializer = JSONSerializer(),
	):
		if transport is not None:
			self._redis = transport
			self._owns_transport = False
		else:
			self._redis = RedisTransport(StandaloneConfig(url=url))
			self._owns_transport = True
		self.group = group
		self.consumer = consumer
		self.sp = stream_prefix
		self.zp = zset_prefix
		self.block_ms = block_ms
		self.claim_min_idle_ms = claim_min_idle_ms
		self.serializer = serializer
		self._move_sha: str | None = None
		self._declared: set[str] = set()

	@property
	def r(self) -> Redis | RedisCluster:
		assert self._redis.r is not None
		return self._redis.r

	def _stream(self, q: str) -> str:
		return self._redis.stream_key(self.sp, q)

	def _zset(self, q: str) -> str:
		return self._redis.zset_key(self.zp, q)

	async def connect(self) -> None:
		if self._redis.r is not None and self._move_sha is not None:
			return
		await self._redis.connect()
		self._move_sha = await self.r.script_load(_MOVE_LUA)

	async def close(self) -> None:
		if self._owns_transport:
			await self._redis.close()
		self._move_sha = None

	async def queue_depth(self, queue: str) -> int | None:
		if self._redis.r is None:
			return None
		try:
			stream = await self.r.xlen(self._stream(queue))
			scheduled = await self.r.zcard(self._zset(queue))
			return int(stream) + int(scheduled)
		except Exception:
			return None

	@_ensure_connected
	async def declare(self, queue: str) -> None:
		if queue in self._declared:
			return
		try:
			await self.r.xgroup_create(self._stream(queue), self.group, id="$", mkstream=True)
		except Exception as e:
			if "BUSYGROUP" not in str(e):
				raise
		self._declared.add(queue)

	@_ensure_connected
	async def enqueue(self, msg: Message) -> None:
		await self.declare(msg.queue)
		await self.r.xadd(self._stream(msg.queue), {"m": self.serializer.marshal(msg)})

	@_ensure_connected
	async def schedule(self, msg: Message, not_before: datetime) -> None:
		await self.declare(msg.queue)
		score = (
			not_before.replace(tzinfo=timezone.utc).timestamp()
			if not_before.tzinfo is None
			else not_before.timestamp()
		)
		await self.r.zadd(self._zset(msg.queue), {self.serializer.marshal(msg): score})

	@_ensure_connected
	async def _pump_scheduled(self, queues: list[str]) -> None:
		if self._move_sha is None:
			raise NotConnected("redis broker not connected")

		while True:
			now = utcnow().timestamp()
			await asyncio.gather(
				*[
					_asyncify(self.r.evalsha)(
						self._move_sha, 2, self._zset(q), self._stream(q), str(now), "128"
					)
					for q in queues
				]
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

		last_claim = 0.0
		claim_interval = max(1.0, self.claim_min_idle_ms / 1000 / 2)

		handle = asyncio.ensure_future(self._pump_scheduled(queues))

		try:
			streams: dict[_KT, _ST] = {self._stream(q): ">" for q in queues}
			q_by_stream = {self._stream(q): q for q in queues}
			while True:
				now = anyio.current_time()
				if now - last_claim >= claim_interval:
					last_claim = now
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
			handle.cancel()

	@_ensure_connected
	async def ack(self, delivery: Delivery) -> None:
		match delivery.receipt:
			case RedisReceipt():
				pass
			case _:
				raise InvalidReceiptType(type(delivery.receipt))

		q, sid = delivery.receipt
		async with self.r.pipeline() as pipe:  # pyrefly: ignore[bad-context-manager]
			pipe.xack(self._stream(q), self.group, sid)
			pipe.xdel(self._stream(q), sid)
			await pipe.execute()

	@_ensure_connected
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
			async with self.r.pipeline() as pipe:  # pyrefly: ignore[bad-context-manager]
				pipe.xack(self._stream(q), self.group, sid)
				pipe.xdel(self._stream(q), sid)
				await pipe.execute()
			return
		msg = structs.replace(delivery.message, attempt=delivery.message.attempt + 1)
		async with self.r.pipeline() as pipe:  # pyrefly: ignore[bad-context-manager]
			if delay and delay > 0:
				when = utcnow().timestamp() + delay
				pipe.zadd(self._zset(q), {self.serializer.marshal(msg): when})
			else:
				pipe.xadd(self._stream(q), {"m": self.serializer.marshal(msg)})
			pipe.xack(self._stream(q), self.group, sid)
			pipe.xdel(self._stream(q), sid)
			await pipe.execute()
