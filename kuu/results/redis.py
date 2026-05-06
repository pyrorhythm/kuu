from __future__ import annotations

from .base import Result, ResultBackend
from .._types import _ensure_connected
from ..serializers import JSONSerializer
from ..serializers.base import Serializer
from ..transports.redis import RedisTransport, StandaloneConfig


class RedisResults(ResultBackend):
	def __init__(
			self,
			url: str = "redis://localhost:6379/0",
			prefix: str = "qq:r:",
			*,
			transport: RedisTransport | None = None,
			serializer: Serializer = JSONSerializer(),
			marshal_types: bool = True,
			ttl: float | None = 86400,
			replay: bool = True,
			store_errors: bool = True,
	):
		"""Redis-backed result store.

		:param url: Redis connection URL (convenience alias for ``StandaloneConfig``).
		:param prefix: key prefix for every stored result.
		:param transport: pre-configured :class:`~kuu.transports.redis.RedisTransport`. When given, ``url`` is ignored.
		:param serializer: payload serializer.
		:param marshal_types: persist type info alongside the payload.
		:param ttl: default expiry in seconds; ``None`` means no expiry.
		:param replay: short-circuit task execution when a cached result is present.
		:param store_errors: persist terminal failures so callers can observe them.
		"""
		self.serializer = serializer
		self.marshal_types = marshal_types
		self.ttl = ttl
		self.replay = replay
		self.store_errors = store_errors
		self.prefix = prefix
		if transport is not None:
			self._redis = transport
			self._owns_transport = False
		else:
			self._redis = RedisTransport(StandaloneConfig(url=url))
			self._owns_transport = True

	@property
	def r(self):
		assert self._redis.r is not None
		return self._redis.r

	def _k(self, key: str) -> str:
		return self._redis.result_key(self.prefix, key)

	async def connect(self) -> None:
		if self._redis.r is not None:
			return
		await self._redis.connect()

	async def close(self) -> None:
		if self._owns_transport:
			await self._redis.close()

	@_ensure_connected
	async def get(self, key: str, **kwargs) -> Result | None:
		data = await self.r.get(self._k(key))
		return self.serializer.unmarshal(data, into=Result) if data else None

	@_ensure_connected
	async def set(self, key: str, result: Result, ttl: float | None = None) -> None:
		await self.r.set(
				self._k(key), self.serializer.marshal(result), ex=int(ttl) if ttl else None
		)

	@_ensure_connected
	async def set_not_exists(self, key: str, result: Result, ttl: float | None = None) -> bool:
		return bool(
				await self.r.set(
						self._k(key), self.serializer.marshal(result), ex=int(ttl) if ttl else None, nx=True
				)
		)
