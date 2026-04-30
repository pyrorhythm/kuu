from __future__ import annotations

import inspect
import logging
import signal
import time
from typing import TYPE_CHECKING, Any

import anyio
import anyio.to_thread
from anyio.abc import CancelScope, TaskGroup

from kuu._import import import_object
from kuu.context import Context
from kuu.exceptions import RejectErr, RetryErr, UnknownTask
from kuu.middleware.base import run_chain
from kuu.outcome import Fail, Ok, Outcome, Reject, Retry
from kuu.result import Result
from kuu.results.base import result_key

if TYPE_CHECKING:
	from kuu.app import Kuu
	from kuu.brokers.base import Delivery
	from kuu.config import Settings

log = logging.getLogger("kuu.worker")


class Worker:
	def __init__(self, config: Settings, *, app: Kuu | None = None) -> None:
		"""
		Worker process.

		- `config`: carries every tunable (queues, concurrency, prefetch,
		  shutdown timeout, app spec).
		- `app`: optional already-imported `Kuu` instance. When provided, the
		  worker skips the dotted-spec import round-trip; otherwise it resolves
		  `config.app` itself.
		"""

		self.app = app if app is not None else import_object(config.app)
		self.queues = config.queues or sorted(
			self.app.registry.queues() or {self.app.default_queue}
		)
		self.concurrency = config.concurrency
		self.prefetch = config.prefetch or max(1, self.concurrency // 4)
		self.shutdown_timeout = config.shutdown_timeout
		self._sem = anyio.Semaphore(self.concurrency)
		self._inflight = 0
		self._idle = anyio.Event()
		self._idle.set()  # idle until first task starts; only waited on during shutdown

	async def run(self) -> None:
		"""Connect broker and results, then run the consumer loop."""
		await self.app.broker.connect()
		if self.app.results is not None:
			await self.app.results.connect()
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
			if self.app.results is not None:
				await self.app.results.close()

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
				self._idle = anyio.Event()  # reset: no longer idle
			self._inflight += 1
			handlers.start_soon(self._handle, delivery)

	async def _handle(self, delivery: Delivery) -> None:
		msg = delivery.message
		task = self.app.registry.get(msg.task)
		ctx = Context(app=self.app, message=msg, phase="process", task=task)

		with anyio.CancelScope(shield=True):
			await self.app.events.task_received.send(msg)

		outcome: Outcome
		cancelled = False
		results = self.app.results
		key = result_key(msg)

		if results is not None and results.replay:
			cached = await results.get(key)
			if cached is not None and cached.status == "ok":
				with anyio.CancelScope(shield=True):
					await self._finalize(delivery, msg, Ok(0.0))
				self._inflight -= 1
				if self._inflight == 0:
					self._idle.set()
				self._sem.release()
				return

		try:
			if task is None:
				raise UnknownTask(msg.task)

			notnil_task = task

			async def _terminal(c: Context) -> Any:
				await self.app.events.task_started.send(c.message)
				payload = c.message.payload
				if notnil_task.blocking:

					def _call() -> Any:
						return notnil_task.original_func(*payload.args, **payload.kwargs)  # type:ignore

					return await anyio.to_thread.run_sync(_call, abandon_on_cancel=True)
				r = notnil_task.original_func(*payload.args, **payload.kwargs)  # type:ignore
				if inspect.isawaitable(r):
					r = await r
				return r

			started = time.perf_counter()
			value = await run_chain(ctx, self.app.middleware, _terminal)
			if results is not None:
				payload, type_fqn = results.encode(value)
				await results.set(
					key,
					Result(status="ok", value=payload, type=type_fqn),
					ttl=results.ttl,
				)
			outcome = Ok(time.perf_counter() - started)
		except RetryErr as r:
			outcome = Retry(
				r.delay  # |
				if r.delay is not None  # |
				else ctx.state.get("retry_delay", 0)
			)
		except RejectErr as r:
			outcome = Reject(r.requeue)
		except BaseException as e:
			outcome = Fail(e)
			cancelled = isinstance(e, anyio.get_cancelled_exc_class())
			if (
				results is not None
				and results.store_errors
				and msg.attempt + 1 >= msg.max_attempts
				and not cancelled
			):
				with anyio.CancelScope(shield=True):
					await results.set(
						key,
						Result(status="error", error=f"{type(e).__name__}: {e}"),
						ttl=results.ttl,
					)

		with anyio.CancelScope(shield=True):
			await self._finalize(delivery, msg, outcome)

		self._inflight -= 1
		if self._inflight == 0:
			self._idle.set()

		self._sem.release()

		if isinstance(outcome, Fail) and cancelled:
			raise outcome.exc

	async def _finalize(self, delivery: Delivery[Any], msg: Any, outcome: Outcome) -> None:
		match outcome:
			case Ok(elapsed):
				await self.app.broker.ack(delivery)
				await self.app.events.task_succeeded.send(msg, elapsed)
			case Retry(delay):
				await self.app.broker.nack(delivery, requeue=True, delay=delay)
				await self.app.events.task_retried.send(msg, delay)
			case Reject(requeue):
				await self.app.broker.nack(delivery, requeue=requeue)
				await self.app.events.task_dead.send(msg)
			case Fail(exc):
				await self.app.events.task_failed.send(msg, exc)
				if msg.attempt + 1 >= msg.max_attempts:
					await self.app.broker.nack(delivery, requeue=False)
					await self.app.events.task_dead.send(msg)
				else:
					await self.app.broker.nack(delivery, requeue=True)
