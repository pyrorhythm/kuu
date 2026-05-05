from __future__ import annotations

from datetime import datetime, timedelta, timezone

import anyio
import pytest

from kuu.brokers.redis import RedisBroker
from kuu.message import Message, Payload
from kuu.transports.redis import RedisTransport

pytestmark = pytest.mark.anyio


def _msg(queue: str = "q", **kw) -> Message:
    return Message(task="t", queue=queue, payload=Payload(), **kw)


def _broker(transport: RedisTransport, uid: str, **kw) -> RedisBroker:
    return RedisBroker(
        transport=transport,
        group=f"g_{uid}",
        consumer="c1",
        stream_prefix=f"s:{uid}:",
        zset_prefix=f"z:{uid}:",
        block_ms=200,
        **kw,
    )


async def test_nack_with_delay_increments_attempt(redis_transport: RedisTransport):
    broker = _broker(redis_transport, "inc")
    await broker.connect()
    try:
        await broker.declare("q")
        await broker.enqueue(_msg())

        results: list = []

        async def handler(idx, delivery, b):
            if idx == 0:
                await b.nack(delivery, requeue=True, delay=0.2)
            else:
                await b.ack(delivery)

        async def _worker(scope: anyio.CancelScope):
            async for delivery in broker.consume(["q"], prefetch=8):
                idx = len(results)
                results.append(delivery)
                await handler(idx, delivery, broker)
                if len(results) >= 2:
                    scope.cancel()

        with anyio.fail_after(10.0):
            async with anyio.create_task_group() as tg:
                tg.start_soon(_worker, tg.cancel_scope)

        assert results[0].message.attempt == 0
        assert results[1].message.attempt == 1
        assert results[0].message.id == results[1].message.id
    finally:
        await broker.close()


async def test_pump_scheduled_moves_due_messages(redis_transport: RedisTransport):
    broker = _broker(redis_transport, "pump_ok")
    await broker.connect()
    try:
        await broker.declare("q")

        msg = _msg()
        past = datetime.now(timezone.utc) - timedelta(seconds=1)
        await broker.r.zadd(broker._zset("q"), {broker.serializer.marshal(msg): past.timestamp()})

        with anyio.move_on_after(0.8):
            await broker._pump_scheduled(["q"])

        count = await broker.r.xlen(broker._stream("q"))
        assert count == 1, f"expected 1 message in stream, got {count}"
    finally:
        await broker.close()


async def test_pump_scheduled_does_not_move_future_messages(
    redis_transport: RedisTransport,
):
    broker = _broker(redis_transport, "pump_fut")
    await broker.connect()
    try:
        await broker.declare("q")

        msg = _msg()
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        await broker.r.zadd(broker._zset("q"), {broker.serializer.marshal(msg): future.timestamp()})

        with anyio.move_on_after(0.8):
            await broker._pump_scheduled(["q"])

        assert await broker.r.xlen(broker._stream("q")) == 0
        assert await broker.r.zcard(broker._zset("q")) == 1
    finally:
        await broker.close()


async def test_pump_scheduled_multiple_queues(redis_transport: RedisTransport):
    broker = _broker(redis_transport, "pump_multi")
    await broker.connect()
    try:
        past = datetime.now(timezone.utc) - timedelta(seconds=1)
        for q in ["a", "b", "c"]:
            await broker.declare(q)
            msg = _msg(queue=q)
            await broker.r.zadd(broker._zset(q), {broker.serializer.marshal(msg): past.timestamp()})

        with anyio.move_on_after(0.8):
            await broker._pump_scheduled(["a", "b", "c"])

        for q in ["a", "b", "c"]:
            count = await broker.r.xlen(broker._stream(q))
            assert count == 1, f"queue {q}: expected 1 message in stream, got {count}"
    finally:
        await broker.close()


async def test_consume_delivers_scheduled_message_via_pump(
    redis_transport: RedisTransport,
):
    broker = _broker(redis_transport, "e2e_sched")
    await broker.connect()
    try:
        await broker.declare("q")

        msg = _msg()
        when = datetime.now(timezone.utc) + timedelta(milliseconds=200)
        await broker.r.zadd(broker._zset("q"), {broker.serializer.marshal(msg): when.timestamp()})

        deliveries: list = []

        async def _consumer(scope: anyio.CancelScope):
            async for delivery in broker.consume(["q"], prefetch=1):
                deliveries.append(delivery)
                await broker.ack(delivery)
                scope.cancel()

        with anyio.fail_after(10.0):
            async with anyio.create_task_group() as tg:
                tg.start_soon(_consumer, tg.cancel_scope)

        assert len(deliveries) == 1
        assert deliveries[0].message.id == msg.id
    finally:
        await broker.close()
