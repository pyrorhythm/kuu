from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime, timezone

import anyio
from nats.aio.msg import Msg
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig, RetentionPolicy, StreamConfig

from .._types import _ensure_connected
from ..exceptions import InvalidReceiptType
from ..message import Message
from ..serializers import JSONSerializer, Serializer
from ..transports.nats._transport import NatsTransport
from .base import Broker, Delivery

NatsReceipt = Msg


class NatsBroker(Broker[NatsReceipt]):
    servers: str | list[str]
    stream: str
    sp: str
    dp: str
    fetch_timeout: float
    serializer: Serializer

    _declared: set[str]
    _scheduled: list[tuple[float, Message]]
    _sched_lock: anyio.Lock
    _sched_event: anyio.Event

    def __init__(
        self,
        transport: NatsTransport | None = None,
        *,
        servers: str | list[str] = "nats://localhost:4222",
        stream: str = "kuu",
        subject_prefix: str = "kuu.task_queue.",
        durable_prefix: str = "kuu-",
        fetch_timeout: float = 5.0,
        serializer: Serializer = JSONSerializer(),
    ) -> None:
        """
        NATS JetStream broker.

        - `servers`: NATS URL or list of URLs.
        - `stream`: JetStream stream name.
        - `subject_prefix`: prefix prepended to per-queue subjects.
        - `durable_prefix`: prefix for durable consumer names.
        - `fetch_timeout`: pull-fetch timeout in seconds.
        - `serializer`: message serializer.
        """
        self.t = transport or NatsTransport(servers=servers)
        self._owns_transport = transport is None

        self.servers = servers
        self.stream = stream
        self.sp = subject_prefix
        self.dp = durable_prefix
        self.fetch_timeout = fetch_timeout
        self.serializer = serializer

        self._declared = set()
        self._scheduled = []
        self._sched_lock = anyio.Lock()
        self._sched_event = anyio.Event()

    def _subject(self, queue: str) -> str:
        return f"{self.sp}{queue}"

    def _durable(self, queue: str) -> str:
        return f"{self.dp}{queue}"

    @property
    def js(self) -> JetStreamContext:
        return self.t.js

    async def connect(self) -> None:
        await self.t.connect()
        # Always ensure this broker's stream exists
        try:
            await self.t.js.add_stream(
                StreamConfig(
                    name=self.stream,
                    subjects=[f"{self.sp}>"],
                    retention=RetentionPolicy.WORK_QUEUE,
                )
            )
        except Exception as e:
            # Ignore "stream already exists" errors
            if "already exists" not in str(e):
                raise

    async def close(self) -> None:
        if self._owns_transport:
            await self.t.close()

    @_ensure_connected
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

    @_ensure_connected
    async def enqueue(self, msg: Message) -> None:
        await self.js.publish(self._subject(msg.queue), self.serializer.marshal(msg))

    @_ensure_connected
    async def schedule(self, msg: Message, not_before: datetime) -> None:
        """No support for DeliverAt in nats-py currently, so scheduling on orchestrator thread. Not persisted."""

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
            self._sched_event = (
                anyio.Event() if self._sched_event.is_set() else self._sched_event
            )
            with anyio.move_on_after(max(0.05, min(wait, 1.0))):
                await self._sched_event.wait()

    async def consume(
        self, queues: list[str], prefetch: int
    ) -> AsyncIterator[Delivery[NatsReceipt]]:
        for q in queues:
            await self.declare(q)

        subs = {
            q: await self.js.pull_subscribe(self._subject(q), durable=self._durable(q))
            for q in queues
        }

        # Use asyncio.create_task instead of anyio task group to avoid
        # cancel-scope "entered in different task" errors when the async
        # generator is closed from a different task.
        pump_task = asyncio.create_task(self._pump_scheduled())

        try:
            while True:
                for q, sub in subs.items():
                    try:
                        msgs = await sub.fetch(
                            batch=prefetch, timeout=self.fetch_timeout
                        )
                    except TimeoutError:
                        continue
                    except:
                        await anyio.sleep(0.5)
                        continue

                    for m in msgs:
                        msg = self.serializer.unmarshal(m.data, into=Message)
                        # Sync attempt with NATS delivery count (num_delivered is 1-indexed)
                        if m.metadata is not None and m.metadata.num_delivered > 1:
                            msg = msg.model_copy(
                                update={"attempt": m.metadata.num_delivered - 1}
                            )
                        yield Delivery(message=msg, receipt=m, queue=q)
        finally:
            pump_task.cancel()
            try:
                await pump_task
            except asyncio.CancelledError:
                pass
            self._sched_event.set()
            # Tear down pull subscriptions so the next consume() on the same
            # transport doesn't share a durable with orphaned inboxes — that
            # leaves pending pulls on the server that swallow new deliveries.
            for sub in subs.values():
                try:
                    await sub.unsubscribe()
                except Exception:
                    pass

    async def ack(self, delivery: Delivery[NatsReceipt]) -> None:
        match delivery.receipt:
            case NatsReceipt():
                await delivery.receipt.ack()
            case _:
                raise InvalidReceiptType(type(delivery.receipt))

    async def nack(
        self,
        delivery: Delivery[NatsReceipt],
        requeue: bool = True,
        delay: float | None = None,
    ) -> None:
        match delivery.receipt:
            case NatsReceipt():
                pass
            case _:
                raise InvalidReceiptType(type(delivery.receipt))

        if not requeue:
            await delivery.receipt.term()
            return
        await delivery.receipt.nak(delay=delay)
