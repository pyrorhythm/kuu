from __future__ import annotations

from collections.abc import Iterator

import pytest
from testcontainers.redis import RedisContainer

from kuu.app import Kuu
from kuu.brokers.redis import RedisBroker
from kuu.results.redis import RedisResults


@pytest.fixture(scope="session")
def redis_container() -> Iterator[RedisContainer]:
	with RedisContainer("redis:8-alpine") as c:
		yield c


@pytest.fixture(scope="session")
def redis_url(redis_container: RedisContainer) -> str:
	host = redis_container.get_container_host_ip()
	port = redis_container.get_exposed_port(6379)
	return f"redis://{host}:{port}/0"


@pytest.fixture
async def redis_flushed(redis_url: str):
	from redis.asyncio import Redis

	r = Redis.from_url(redis_url)
	try:
		await r.flushdb()
		yield redis_url
		await r.flushdb()
	finally:
		await r.aclose()


@pytest.fixture
async def make_app(redis_flushed: str):
	created: list[Kuu] = []

	async def _make(
		*,
		queue: str = "default",
		stream_prefix: str = "qqit:s:",
		zset_prefix: str = "qqit:z:",
		group: str = "qqit",
		consumer: str = "c1",
		results_prefix: str = "qqit:r:",
		with_results: bool = True,
		**kuu_kwargs,
	) -> Kuu:
		broker = RedisBroker(
			url=redis_flushed,
			group=group,
			consumer=consumer,
			stream_prefix=stream_prefix,
			zset_prefix=zset_prefix,
			block_ms=200,
		)
		results = RedisResults(url=redis_flushed, prefix=results_prefix) if with_results else None
		app = Kuu(broker=broker, default_queue=queue, results=results, **kuu_kwargs)
		await app.broker.connect()
		await app.broker.declare(queue)
		created.append(app)
		return app

	yield _make
	for app in created:
		try:
			await app.broker.close()
		except Exception:
			pass
