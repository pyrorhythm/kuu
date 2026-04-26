from __future__ import annotations

from redis.asyncio import Redis

from ..exceptions import NotConnected
from ..serializers import JSONSerializer
from ..serializers.base import Serializer
from .base import Result, ResultBackend


class RedisResults(ResultBackend):
	def __init__(
		self,
		url: str = "redis://localhost:6379/0",
		prefix: str = "qq:r:",
		*,
		serializer: Serializer = JSONSerializer(),
		marshal_types: bool = True,
		ttl: float | None = 86400,
		replay: bool = True,
		store_errors: bool = True,
	):
		"""
		Create Redis result backend

		Args:
			url: Redis connection URL
			prefix: key prefix for all stored results
			serializer: serializer instance
			marshal_types: persist type info with payload
			ttl: default expiry in seconds
			replay: enable replay from cache
			store_errors: persist terminal errors
		"""
		super().__init__(
			serializer=serializer,
			marshal_types=marshal_types,
			ttl=ttl,
			replay=replay,
			store_errors=store_errors,
		)
		self.url = url
		self.prefix = prefix
		self._r: Redis | None = None

	@property
	def r(self) -> Redis:

		if self._r is None:
			raise NotConnected("result backend not connected")
		return self._r

	def _k(self, key: str) -> str:
		return f"{self.prefix}{key}"

	async def connect(self) -> None:
		self._r = Redis.from_url(self.url)

	async def close(self) -> None:
		if self._r is not None:
			await self._r.aclose()
			self._r = None

	async def get(self, key: str) -> Result | None:
		"""
		Fetch result by key from Redis

		Args:
			key: result cache key

		Returns:
			Deserialized result; or None if key absent
		"""
		data = await self.r.get(self._k(key))
		return self.serializer.unmarshal(data, into=Result) if data else None

	async def set(self, key: str, result: Result, ttl: float | None = None) -> None:
		"""
		Store serialized result in Redis with optional expiry

		Args:
			key: result cache key
			result: result object to serialize and store
			ttl: expiry in seconds
		"""
		await self.r.set(
			self._k(key), self.serializer.marshal(result), ex=int(ttl) if ttl else None
		)

	async def setnx(self, key: str, result: Result, ttl: float | None = None) -> bool:
		"""
		Store serialized result only if key absent

		Args:
			key: result cache key
			result: result object to serialize and store
			ttl: expiry in seconds

		Returns:
			True if key was set; False otherwise
		"""
		return bool(
			await self.r.set(
				self._k(key), self.serializer.marshal(result), ex=int(ttl) if ttl else None, nx=True
			)
		)
