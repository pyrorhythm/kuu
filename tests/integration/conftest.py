from __future__ import annotations

import uuid
from collections.abc import Iterator
from typing import AsyncIterator

import pytest
from testcontainers.nats import NatsContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import AsyncRedisContainer

from kuu import Broker
from kuu.app import Kuu
from kuu.brokers.memory import MemoryBroker
from kuu.brokers.nats import NatsBroker
from kuu.brokers.redis import RedisBroker
from kuu.results.postgres import PostgresResults
from kuu.results.redis import RedisResults
from kuu.transports.nats import NatsTransport
from kuu.transports.postgres import PostgresParams, PostgresTransport
from kuu.transports.redis import RedisTransport

pytestmark = pytest.mark.anyio


@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"


@pytest.fixture(scope="session")
def redis_container() -> Iterator[AsyncRedisContainer]:
    with AsyncRedisContainer("redis:8-alpine") as c:
        yield c


@pytest.fixture(scope="session")
def postgres_container() -> Iterator[PostgresContainer]:
    with PostgresContainer("postgres:18-alpine") as c:
        yield c


@pytest.fixture(scope="session")
def nats_container() -> Iterator[NatsContainer]:
    with NatsContainer("nats:2.14-alpine", jetstream=True) as c:
        yield c


# === transport fixtures (shared, do not use directly in tests)


def _pg_params(container: PostgresContainer) -> PostgresParams:
    return PostgresParams(
        host=container.get_container_host_ip(),
        port=int(container.get_exposed_port(container.port)),
        user=container.username,
        password=container.password,
        database=container.dbname,
    )


@pytest.fixture(scope="session")
async def postgres_transport(postgres_container: PostgresContainer):
    tpt = PostgresTransport(config=_pg_params(postgres_container))
    await tpt.connect()
    yield tpt
    try:
        async with tpt.acq() as conn:
            await conn.execute("drop schema if exists kuu cascade")
    except Exception:
        pass
    await tpt.close()


@pytest.fixture(scope="session")
async def redis_transport(
    redis_container: AsyncRedisContainer,
) -> AsyncIterator[RedisTransport]:
    client = await redis_container.get_async_client()
    tpt = RedisTransport(client=client)
    await tpt.connect()
    yield tpt
    await tpt.close()


@pytest.fixture(scope="session")
async def nats_transport(nats_container: NatsContainer) -> AsyncIterator[NatsTransport]:
    tpt = NatsTransport(servers=nats_container.nats_uri())
    await tpt.connect()
    yield tpt
    try:
        jsm = await tpt._client.jetstream_manager()
        streams = await jsm.streams_info()
        for stream in streams:
            if stream.config.name.startswith("kuu_"):
                await jsm.delete_stream(stream.config.name)
    except Exception:
        pass
    await tpt.close()


# === internal broker / results factories


def _redis_broker(transport: RedisTransport, uid: str) -> RedisBroker:
    return RedisBroker(
        transport=transport,
        group=f"g_{uid}",
        consumer="c1",
        stream_prefix=f"s:{uid}:",
        zset_prefix=f"z:{uid}:",
        block_ms=200,
    )


def _nats_broker(transport: NatsTransport, uid: str) -> NatsBroker:
    return NatsBroker(
        transport=transport,
        stream=f"kuu_{uid}",
        subject_prefix=f"kuu.{uid}.",
        durable_prefix=f"kuu-{uid}-",
        fetch_timeout=2.0,
    )


def _redis_results(
    transport: RedisTransport,
    uid: str,
    *,
    ttl: float | None = 86400,
    replay: bool = True,
    store_errors: bool = True,
) -> RedisResults:
    return RedisResults(
        transport=transport,
        prefix=f"r:{uid}:",
        ttl=ttl,
        replay=replay,
        store_errors=store_errors,
    )


def _postgres_results(
    transport: PostgresTransport,
    *,
    ttl: float | None = 86400,
    replay: bool = True,
    store_errors: bool = True,
) -> PostgresResults:
    return PostgresResults(
        transport=transport,
        ttl=ttl,
        replay=replay,
        store_errors=store_errors,
    )


def _build_broker(
    param: str,
    redis_transport: RedisTransport,
    nats_transport: NatsTransport,
    uid: str,
) -> Broker:
    if param == "redis":
        return _redis_broker(redis_transport, uid)
    if param == "nats":
        return _nats_broker(nats_transport, uid)
    return MemoryBroker()


async def _delete_redis_keys(r, *patterns: str) -> None:
    for pattern in patterns:
        cursor = 0
        while True:
            cursor, keys = await r.scan(cursor, match=pattern, count=100)
            if keys:
                await r.delete(*keys)
            if cursor == 0:
                break


# === any_broker: all brokers, no app


@pytest.fixture(params=["memory", "redis", "nats"])
async def any_broker(
    request,
    redis_transport: RedisTransport,
    nats_transport: NatsTransport,
):
    uid = uuid.uuid4().hex[:8]
    b = _build_broker(request.param, redis_transport, nats_transport, uid)
    await b.connect()
    yield b
    if isinstance(b, RedisBroker):
        await _delete_redis_keys(redis_transport.r, f"s:{uid}:*", f"z:{uid}:*")
    elif isinstance(b, NatsBroker):
        try:
            await nats_transport.js.delete_stream(b.stream)
        except Exception:
            pass


# === make_app: all brokers, no results backend


@pytest.fixture(params=["memory", "redis", "nats"])
async def make_app(
    request,
    redis_transport: RedisTransport,
    nats_transport: NatsTransport,
):
    param = request.param
    created: list[tuple[Kuu, str]] = []

    async def _make(*, queue: str = "default", **kuu_kwargs) -> Kuu:
        uid = uuid.uuid4().hex[:8]
        broker = _build_broker(param, redis_transport, nats_transport, uid)
        app = Kuu(broker=broker, default_queue=queue, **kuu_kwargs)
        created.append((app, uid))
        return app

    yield _make

    for app, uid in created:
        if isinstance(app.broker, RedisBroker):
            await _delete_redis_keys(redis_transport.r, f"s:{uid}:*", f"z:{uid}:*")
        elif isinstance(app.broker, NatsBroker):
            try:
                await nats_transport.js.delete_stream(app.broker.stream)
            except Exception:
                pass


# === make_app_with_results: all broker * results combos


@pytest.fixture(params=["redis+redis", "nats+redis", "redis+postgres", "nats+postgres"])
async def make_app_with_results(
    request,
    redis_transport: RedisTransport,
    nats_transport: NatsTransport,
    postgres_transport: PostgresTransport,
):
    broker_type, results_type = request.param.split("+")
    created: list[tuple[Kuu, str]] = []

    async def _make(
        *,
        queue: str = "default",
        with_results: bool = True,
        result_ttl: float | None = 86400,
        result_replay: bool = True,
        result_store_errors: bool = True,
        **kuu_kwargs,
    ) -> Kuu:
        uid = uuid.uuid4().hex[:8]
        broker: Broker = _build_broker(broker_type, redis_transport, nats_transport, uid)

        results = None
        if with_results:
            if results_type == "redis":
                results = _redis_results(
                    redis_transport,
                    uid,
                    ttl=result_ttl,
                    replay=result_replay,
                    store_errors=result_store_errors,
                )
            else:
                results = _postgres_results(
                    postgres_transport,
                    ttl=result_ttl,
                    replay=result_replay,
                    store_errors=result_store_errors,
                )

        app = Kuu(broker=broker, default_queue=queue, results=results, **kuu_kwargs)
        created.append((app, uid))
        return app

    yield _make

    for app, uid in created:
        if app.results is not None:
            try:
                await app.results.close()
            except Exception:
                pass

        if isinstance(app.broker, RedisBroker):
            await _delete_redis_keys(redis_transport.r, f"s:{uid}:*", f"z:{uid}:*")
        elif isinstance(app.broker, NatsBroker):
            try:
                await nats_transport.js.delete_stream(app.broker.stream)
            except Exception:
                pass

        if app.results is not None:
            if isinstance(app.results, RedisResults):
                await _delete_redis_keys(redis_transport.r, f"r:{uid}:*")
            elif isinstance(app.results, PostgresResults):
                try:
                    async with postgres_transport.acq() as conn:
                        await conn.execute("delete from kuu.task_results")
                except Exception:
                    pass
