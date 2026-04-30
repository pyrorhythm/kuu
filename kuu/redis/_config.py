from __future__ import annotations

from dataclasses import dataclass


class RedisConfig:
	"""Marker base for Redis connection configurations.

	Concrete variants use :func:`match` / ``match ... case`` for dispatch.
	"""


@dataclass(frozen=True)
class StandaloneConfig(RedisConfig):
	"""Single-node Redis (or Sentinel-managed)."""

	url: str = "redis://localhost:6379/0"
	socket_timeout: float | None = None
	socket_connect_timeout: float | None = None
	retry_on_timeout: bool = False


@dataclass(frozen=True)
class ClusterConfig(RedisConfig):
	"""Redis Cluster; keys are hash-tagged automatically."""

	url: str = "redis://localhost:6379/0"
	socket_timeout: float | None = None
	socket_connect_timeout: float | None = None
	retry_on_timeout: bool = False
	read_from_replicas: bool = False


@dataclass(frozen=True)
class SentinelConfig(RedisConfig):
	"""Redis Sentinel: automatic master discovery and failover.

	``hosts`` is a sequence of ``(host, port)`` pairs pointing to sentinel
	nodes.  ``service_name`` is the monitored master name.
	"""

	hosts: tuple[tuple[str, int], ...] = ()
	service_name: str = ""
	socket_timeout: float | None = None
	socket_connect_timeout: float | None = None
	retry_on_timeout: bool = False
