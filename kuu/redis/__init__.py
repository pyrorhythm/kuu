from warnings import warn
from kuu.transports.redis import (
	RedisTransport,
	SentinelConfig,
	StandaloneConfig,
	RedisConfig,
	ClusterConfig,
)

warn(
	"kuu.redis module is deprecated and is scheduled to be removed on 0.2; use kuu.transports.redis",
	category=DeprecationWarning,
)

__all__ = [
	"RedisConfig",
	"StandaloneConfig",
	"ClusterConfig",
	"SentinelConfig",
	"RedisTransport",
]
