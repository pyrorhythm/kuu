from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from ..message import Message


@dataclass
class Delivery[Receipt = Any]:
    message: Message
    receipt: Receipt
    queue: str


class Broker(Protocol):
    async def connect(self) -> None: ...
    async def close(self) -> None: ...

    async def declare(self, queue: str) -> None: ...

    async def enqueue(self, msg: Message) -> None: ...
    async def schedule(self, msg: Message, not_before: datetime) -> None: ...

    def consume(self, queues: list[str], prefetch: int) -> AsyncIterator[Delivery]: ...

    async def ack(self, delivery: Delivery) -> None: ...
    async def nack(
        self, delivery: Delivery, requeue: bool = True, delay: float | None = None
    ) -> None: ...
