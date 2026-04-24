from __future__ import annotations

import logging
import signal
import time
from typing import Any

import anyio
from anyio.abc import CancelScope, TaskGroup

from .app import Q
from .brokers.base import Delivery
from .context import Context
from .exceptions import Reject, Retry, UnknownTask
from .middleware.base import run_chain
from .serializers import load_args

log = logging.getLogger("qq.worker")


class Worker:
    def __init__(
        self,
        app: Q,
        queues: list[str] | None = None,
        concurrency: int = 64,
        prefetch: int | None = None,
        shutdown_timeout: float = 30.0,
    ):
        self.app = app
        self.queues = queues or sorted(app.registry.queues() or {app.default_queue})
        self.concurrency = concurrency
        self.prefetch = prefetch or max(1, concurrency // 4)
        self.shutdown_timeout = shutdown_timeout
        self._sem = anyio.Semaphore(concurrency)
        self._inflight = 0
        self._idle = anyio.Event()
        self._idle.set()

    async def run(self) -> None:
        await self.app.broker.connect()
        for q in self.queues:
            await self.app.broker.declare(q)
        try:
            async with anyio.create_task_group() as handlers:
                consumer_scope = anyio.CancelScope()
                async with anyio.create_task_group() as control:
                    control.start_soon(self._signal_watcher, consumer_scope)
                    with consumer_scope:
                        await self._consume(handlers)
                    control.cancel_scope.cancel()

                with anyio.move_on_after(self.shutdown_timeout) as drain:
                    await self._idle.wait()
                if drain.cancel_called:
                    log.warning("shutdown timeout, cancelling %d in-flight", self._inflight)
                    handlers.cancel_scope.cancel()
        finally:
            await self.app.broker.close()

    async def _signal_watcher(self, scope: CancelScope) -> None:
        try:
            with anyio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
                async for _sig in signals:
                    scope.cancel()
                    return
        except NotImplementedError:
            await anyio.sleep_forever()

    async def _consume(self, handlers: TaskGroup) -> None:
        async for delivery in self.app.broker.consume(self.queues, self.prefetch):
            await self._sem.acquire()
            if self._inflight == 0:
                self._idle = anyio.Event()
            self._inflight += 1
            handlers.start_soon(self._handle, delivery)

    async def _handle(self, delivery: Delivery[Any]) -> None:
        msg = delivery.message
        task = self.app.registry.get(msg.task)
        ctx = Context(app=self.app, message=msg, phase="process", task=task, delivery=delivery)

        with anyio.CancelScope(shield=True):
            await self.app.events.task_received.send(msg)

        outcome: tuple[str, Any] | None = None
        cancelled = False
        try:
            if task is None:
                raise UnknownTask(msg.task)
            ctx.args = load_args(msg.payload, task.args_model)

            async def _terminal(c: Context) -> Any:
                await self.app.events.task_started.send(c.message)
                if c.args is None:
                    return await task.fn()
                return await task.fn(c.args)

            started = time.perf_counter()
            ctx.result = await run_chain(ctx, self.app.middleware, _terminal)
            outcome = ("ok", time.perf_counter() - started)
        except Retry as r:
            outcome = ("retry", r.delay if r.delay is not None else ctx.state.get("retry_delay"))
        except Reject as r:
            outcome = ("reject", r.requeue)
        except BaseException as e:
            ctx.exc = e
            outcome = ("fail", e)
            cancelled = isinstance(e, anyio.get_cancelled_exc_class())

        with anyio.CancelScope(shield=True):
            await self._finalize(delivery, msg, outcome)

        self._sem.release()
        self._inflight -= 1
        if self._inflight == 0:
            self._idle.set()

        if outcome[0] == "fail" and cancelled:
            raise outcome[1]

    async def _finalize(self, delivery: Delivery[Any], msg: Any, outcome: tuple[str, Any]) -> None:
        kind = outcome[0]
        if kind == "ok":
            await self.app.broker.ack(delivery)
            await self.app.events.task_succeeded.send(msg, float(outcome[1]))
        elif kind == "retry":
            delay = outcome[1]
            await self.app.broker.nack(delivery, requeue=True, delay=delay)
            await self.app.events.task_retried.send(msg, float(delay or 0.0))
        elif kind == "reject":
            await self.app.broker.nack(delivery, requeue=bool(outcome[1]))
            await self.app.events.task_dead.send(msg)
        else:
            await self.app.events.task_failed.send(msg, outcome[1])
            if msg.attempt + 1 >= msg.max_attempts:
                await self.app.broker.nack(delivery, requeue=False)
                await self.app.events.task_dead.send(msg)
            else:
                await self.app.broker.nack(delivery, requeue=True)
