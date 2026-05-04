from __future__ import annotations

from typing import Any

from redis.asyncio import Redis, RedisCluster
from redis.asyncio.sentinel import Sentinel

from ._config import ClusterConfig, RedisConfig, SentinelConfig, StandaloneConfig


class RedisTransport:
	"""Manages a redis-py client lifecycle with cluster-aware key formatting.

	Accepts any :class:`RedisConfig` variant or a pre-built client.

	.. code-block::
	    RedisTransport(StandaloneConfig(url="redis://localhost:6379/0"))
	    RedisTransport(ClusterConfig(url="redis://node1:6379"))
	    RedisTransport(SentinelConfig(
	        hosts=(("s1", 26379), ("s2", 26379)),
	        service_name="mymaster",
	    ))
	    RedisTransport(client=Redis.from_url("redis://localhost:6379/0"))
	    RedisTransport(client=RedisCluster.from_url("redis://n1:6379"))

	After :meth:`connect`, the underlying client is available as ``.r``.
	"""

	def __init__(
		self,
		config: RedisConfig | None = None,
		*,
		client: Redis | RedisCluster | None = None,
	) -> None:
		self._config = config or StandaloneConfig()
		self._client = client
		self.r: Redis | RedisCluster | None = None

	async def connect(self) -> None:
		if self.r is not None:
			return
		if self._client is not None:
			self.r = self._client
			return

		cfg = self._config
		kwargs = self._connection_kwargs()

		match cfg:
			case SentinelConfig(hosts=hosts, service_name=service_name):
				sentinel = Sentinel(list(hosts), **kwargs)
				self.r = sentinel.master_for(service_name)  # type: ignore[arg-type]
			case ClusterConfig(url=url):
				read_from = kwargs.pop("read_from_replicas", False)
				self.r = RedisCluster.from_url(url, read_from_replicas=read_from, **kwargs)
			case StandaloneConfig(url=url):
				self.r = Redis.from_url(url, **kwargs)
			case _:
				raise TypeError(f"unsupported config type: {type(cfg).__name__}")

	async def close(self) -> None:
		if self.r is not None:
			await self.r.aclose()
			self.r = None

	def stream_key(self, prefix: str, queue: str) -> str:
		if self._is_cluster():
			# wrap in {{}} so related keys (stream + sorted-set for the same queue) hash to the same slot
			return f"{prefix}{{{queue}}}"
		return f"{prefix}{queue}"

	def zset_key(self, prefix: str, queue: str) -> str:
		if self._is_cluster():
			return f"{prefix}{{{queue}}}"
		return f"{prefix}{queue}"

	def result_key(self, prefix: str, key: str) -> str:
		return f"{prefix}{key}"

	def _is_cluster(self) -> bool:
		if self.r is not None:
			return isinstance(self.r, RedisCluster)
		return isinstance(self._config, ClusterConfig)

	def _connection_kwargs(self) -> dict[str, Any]:
		cfg = self._config
		kwargs: dict[str, Any] = {}
		if isinstance(cfg, StandaloneConfig | ClusterConfig | SentinelConfig):
			if cfg.socket_timeout is not None:
				kwargs["socket_timeout"] = cfg.socket_timeout
			if cfg.socket_connect_timeout is not None:
				kwargs["socket_connect_timeout"] = cfg.socket_connect_timeout
			if cfg.retry_on_timeout:
				kwargs["retry_on_timeout"] = True
		if isinstance(cfg, ClusterConfig) and cfg.read_from_replicas:
			kwargs["read_from_replicas"] = True
		return kwargs
