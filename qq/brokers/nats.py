from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any

import anyio
import nats
from nats.aio.msg import Msg
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig, RetentionPolicy, StreamConfig

from ..message import Message
from .base import Broker, Delivery


class NatsBroker(Broker):
    def __init__(
        self,
        servers: str | list[str] = "nats://localhost:4222",
        stream: str = "QQ",
        subject_prefix: str = "qq.q.",
        durable_prefix: str = "qq-",
        fetch_timeout: float = 5.0,
    ) -> None:
        self.servers = servers
        self.stream = stream
        self.sp = subject_prefix
        self.dp = durable_prefix
        self.fetch_timeout = fetch_timeout
        self._nc: Any = None
        self._js: JetStreamContext | None = None
        self._declared: set[str] = set()
        self._scheduled: list[tuple[float, Message]] = []
        self._sched_lock = anyio.Lock()
        self._sched_event = anyio.Event()

    def _subject(self, queue: str) -> str:
        return f"{self.sp}{queue}"

    def _durable(self, queue: str) -> str:
        return f"{self.dp}{queue}"

    @property
    def js(self) -> JetStreamContext:
        assert self._js is not None, "broker not connected"
        return self._js

    async def connect(self) -> None:
        self._nc = await nats.connect(self.servers)
        self._js = self._nc.jetstream()
        try:
            await self._js.add_stream(
                StreamConfig(
                    name=self.stream,
                    subjects=[f"{self.sp}>"],
                    retention=RetentionPolicy.WORK_QUEUE,
                )
            )
        except Exception:
            pass

    async def close(self) -> None:
        if self._nc is not None:
            await self._nc.drain()
            self._nc = None
            self._js = None

    async def declare(self, queue: str) -> None:
        if queue in self._declared:
            return
        await self.js.add_consumer(
            stream=self.stream,
            config=ConsumerConfig(
                durable_name=self._durable(queue),
                filter_subject=self._subject(queue),
                ack_wait=60,
                max_deliver=-1,
            ),
        )
        self._declared.add(queue)

    async def enqueue(self, msg: Message) -> None:
        await self.js.publish(self._subject(msg.queue), msg.marshal())

    async def schedule(self, msg: Message, not_before: datetime) -> None:
        ts = (
            not_before.timestamp()
            if not_before.tzinfo
            else not_before.replace(tzinfo=timezone.utc).timestamp()
        )
        async with self._sched_lock:
            self._scheduled.append((ts, msg))
            self._scheduled.sort(key=lambda x: x[0])
        self._sched_event.set()

    async def _pump_scheduled(self) -> None:
        while True:
            async with self._sched_lock:
                now = datetime.now(timezone.utc).timestamp()
                due: list[Message] = []
                while self._scheduled and self._scheduled[0][0] <= now:
                    due.append(self._scheduled.pop(0)[1])
                wait = self._scheduled[0][0] - now if self._scheduled else 1.0
            for m in due:
                await self.enqueue(m)
            self._sched_event = anyio.Event() if self._sched_event.is_set() else self._sched_event
            with anyio.move_on_after(max(0.05, min(wait, 1.0))):
                await self._sched_event.wait()

    async def consume(self, queues: list[str], prefetch: int) -> AsyncIterator[Delivery[Msg]]:
        for q in queues:
            await self.declare(q)

        subs = {
            q: await self.js.pull_subscribe(self._subject(q), durable=self._durable(q))
            for q in queues
        }

        async with anyio.create_task_group() as tg:
            tg.start_soon(self._pump_scheduled)

            try:
                while True:
                    for q, sub in subs.items():
                        try:
                            msgs = await sub.fetch(batch=prefetch, timeout=self.fetch_timeout)
                        except TimeoutError:
                            continue
                        except Exception:
                            await anyio.sleep(0.5)
                            continue
                        for m in msgs:
                            yield Delivery(message=Message.unmarshal(m.data), receipt=m, queue=q)
            finally:
                tg.cancel_scope.cancel("failed to consume")
                self._sched_event.set()

    async def ack(self, delivery: Delivery[Msg]) -> None:
        await delivery.receipt.ack()

    async def nack(
        self, delivery: Delivery[Msg], requeue: bool = True, delay: float | None = None
    ) -> None:
        if not requeue:
            await delivery.receipt.term()
            return
        await delivery.receipt.nak(delay=delay)
