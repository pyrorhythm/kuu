from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel

from .brokers.base import Broker
from .context import Context
from .events import Events
from .message import Message
from .middleware.base import Middleware, run_chain
from .serializers import dump_args
from .task import Registry, Task, infer_args_model

_Q_TASK_ATTR = "__qq_task"


class Q:
    def __init__(
        self,
        broker: Broker,
        default_queue: str = "default",
        middleware: list[Middleware] | None = None,
    ):
        self.broker = broker
        self.default_queue = default_queue
        self.middleware: list[Middleware] = list(middleware or [])
        self.registry = Registry()
        self.events = Events()

    def task(
        self,
        name: str | None = None,
        queue: str | None = None,
        max_attempts: int = 5,
        timeout: float | None = None,
    ) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
        def wrapper(fn: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
            t = Task(
                name=name or f"{fn.__module__}.{fn.__qualname__}",
                queue=queue or self.default_queue,
                fn=fn,
                args_model=infer_args_model(fn),
                max_attempts=max_attempts,
                timeout=timeout,
            )
            self.registry.add(t)
            setattr(fn, _Q_TASK_ATTR, t)
            return fn

        return wrapper

    async def _send(
        self,
        task_name: str,
        args: BaseModel | dict[str, Any] | None,
        *,
        queue: str | None = None,
        not_before: datetime | None = None,
        headers: dict[str, str] | None = None,
        max_attempts: int | None = None,
    ) -> Message:
        t = self.registry.get(task_name)
        q = queue or (t.queue if t else self.default_queue)
        msg = Message(
            task=task_name,
            queue=q,
            payload=dump_args(args),
            headers=headers or {},
            max_attempts=max_attempts if max_attempts is not None else (t.max_attempts if t else 5),
            not_before=not_before,
        )
        ctx = Context(app=self, message=msg, phase="enqueue", task=t, args=args)

        async def _terminal(_c: Context) -> None:
            if not_before is not None and not_before > datetime.now(timezone.utc):
                await self.broker.schedule(msg, not_before)
            else:
                await self.broker.enqueue(msg)
            await self.events.task_enqueued.send(msg)

        await run_chain(ctx, self.middleware, _terminal)
        return msg

    async def enqueue(
        self,
        task: str | Callable[..., Any],
        args: BaseModel | dict[str, Any] | None = None,
        **opts: Any,
    ) -> Message:
        name = task if isinstance(task, str) else getattr(task, _Q_TASK_ATTR).name
        return await self._send(name, args, **opts)
