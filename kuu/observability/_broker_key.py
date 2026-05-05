from __future__ import annotations

import hashlib
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import orjson


def broker_key(obj: Any) -> str:
	identity = _identity(_unwrap_broker(obj))
	canonical = orjson.dumps(identity, option=orjson.OPT_SORT_KEYS)
	return hashlib.sha256(canonical).hexdigest()


def _unwrap_broker(obj: Any) -> Any:
	cls = type(obj).__name__
	if cls == "RedisBroker":
		return obj._redis
	if cls == "NatsBroker":
		return obj.t
	if cls == "PostgresBroker":
		return getattr(obj, "_pg", None) or getattr(obj, "t", obj)
	return obj


def _identity(obj: Any) -> dict[str, Any]:
	cls = type(obj).__name__
	if cls == "MemoryBroker":
		return {"type": "memory", "id": id(obj)}

	if cls in {"RedisTransport", "StandaloneConfig", "ClusterConfig", "SentinelConfig"}:
		return _redis_identity(obj)
	if cls == "NatsTransport":
		return _nats_identity(obj)
	if cls in {"PostgresTransport", "PostgresDSN", "PostgresParams"}:
		return _postgres_identity(obj)

	raise TypeError(f"unsupported transport/config for broker_key: {cls}")


def _redis_identity(obj: Any) -> dict[str, Any]:
	cfg = getattr(obj, "_config", obj)
	cfg_cls = type(cfg).__name__

	if cfg_cls == "SentinelConfig":
		hosts = sorted(tuple(h) for h in getattr(cfg, "hosts", ()))
		return {
			"type": "redis",
			"variant": "sentinel",
			"hosts": hosts,
			"service_name": getattr(cfg, "service_name", ""),
		}

	url = getattr(cfg, "url", "")
	variant = "cluster" if cfg_cls == "ClusterConfig" else "standalone"
	return {
		"type": "redis",
		"variant": variant,
		"url": _strip_url_creds(url),
	}


def _nats_identity(obj: Any) -> dict[str, Any]:
	servers = getattr(obj, "_servers", "")
	if isinstance(servers, str):
		normalized = [_strip_url_creds(servers)]
	else:
		normalized = sorted(_strip_url_creds(s) for s in servers)
	return {"type": "nats", "servers": normalized}


def _postgres_identity(obj: Any) -> dict[str, Any]:
	cfg = obj
	if type(obj).__name__ == "PostgresTransport":
		cfg = getattr(obj, "_config", None)

	cls = type(cfg).__name__
	if cls == "PostgresDSN":
		return {"type": "postgres", "dsn": _strip_url_creds(getattr(cfg, "dsn", ""))}
	if cls == "PostgresParams":
		return {
			"type": "postgres",
			"host": getattr(cfg, "host", None),
			"port": getattr(cfg, "port", None),
			"database": getattr(cfg, "database", None),
		}
	return {"type": "postgres"}


def _strip_url_creds(url: str) -> str:
	if not url or "://" not in url:
		return url
	parts = urlsplit(url)
	netloc = parts.hostname or ""
	if parts.port is not None:
		netloc = f"{netloc}:{parts.port}"
	return urlunsplit((parts.scheme, netloc, parts.path, parts.query, ""))
