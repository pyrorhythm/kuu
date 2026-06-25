from __future__ import annotations

import inspect
import logging
import signal
import time
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from uuid import UUID

import anyio
import anyio.to_thread
from anyio.abc import CancelScope, TaskGroup

from kuu._import import import_object
from kuu._types import _coerce_payload
from kuu._util import utcnow
from kuu.context import Context
from kuu.exceptions import RejectErr, RetryErr
from kuu.middleware.base import run_chain
from kuu.observability import _log_capture
from kuu.outcome import Cancelled, Fail, Ok, Outcome, Reject, Retry
from kuu.result import Result
from kuu.results.base import result_key

# how long a revoked id is remembered so a not-yet-delivered task is still
# dropped when it finally arrives; bounds the local revoked-set memory.
_REVOKED_TTL = 3600.0

if TYPE_CHECKING:
	from kuu.app import Kuu
	from kuu.brokers.base import Delivery
	from kuu.config import Settings

log = logging.getLogger("kuu.worker")


class Worker:
	def __init__(self, config: Settings, *, app: Kuu | None = None) -> None:
		"""
		Worker process.

		- `conn_config`: carries every tunable (queues, concurrency, prefetch,
		  shutdown timeout, app spec).
		- `app`: optional already-imported `Kuu` instance. When provided, the
		  worker skips the dotted-spec import round-trip; otherwise it resolves
		  `conn_config.app` itself.
		"""

		self.app = app if app is not None else import_object(config.app)  # type:ignore
		self.queues = config.queues or sorted(
			self.app.registry.queues() or {self.app.default_queue}
		)
		self.concurrency = config.concurrency
		self.prefetch = config.prefetch or max(1, self.concurrency // 4)
		self.shutdown_timeout = config.shutdown_timeout
		self.unknown_task_delay = config.unknown_task_delay
		self._sem = anyio.Semaphore(self.concurrency)
		self._inflight = 0
		self._idle = anyio.Event()
		self._idle.set()  # idle until first task starts; only waited on during shutdown
		self._scopes: dict[UUID, CancelScope] = {}  # inflight task id -> its cancel scope
		self._revoked: dict[UUID, float] = {}  # revoked task id -> expiry (current_time)

	async def run(self) -> None:
		"""Connect broker and results, then run the consumer loop."""
		limiter = anyio.to_thread.current_default_thread_limiter()
		if self.concurrency > limiter.total_tokens:
			limiter.total_tokens = self.concurrency
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
					control.start_soon(self._watch_revocations)
					with consumer_scope:
						await self._consume(handlers)
					control.cancel_scope.cancel()

				with anyio.move_on_after(self.shutdown_timeout) as drain:
					await self._idle.wait()
				if drain.cancel_called:
					log.warning(
						"event=worker.shutdown_timeout cancelled_inflight=%d", self._inflight
					)
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

	async def _watch_revocations(self) -> None:
		try:
			async for raw in self.app.broker.watch_revocations():
				try:
					tid = UUID(raw)
				except (ValueError, AttributeError):
					continue
				self._revoked[tid] = anyio.current_time() + _REVOKED_TTL
				self._prune_revoked()
				scope = self._scopes.get(tid)
				if scope is not None:
					log.info("event=worker.revoke_running task_id=%s", tid)
					scope.cancel()
		except Exception as e:
			log.warning("event=worker.revocation_watch_error err=%r", e)

	def _prune_revoked(self) -> None:
		if len(self._revoked) < 1024:
			return
		now = anyio.current_time()
		self._revoked = {tid: exp for tid, exp in self._revoked.items() if exp > now}

	async def _consume(self, handlers: TaskGroup) -> None:
		backoff = 0.5
		while True:
			try:
				async for delivery in self.app.broker.consume(self.queues, self.prefetch):
					backoff = 0.5
					await self._sem.acquire()
					if self._inflight == 0:
						self._idle = anyio.Event()  # reset: no longer idle
					self._inflight += 1
					try:
						handlers.start_soon(self._handle, delivery)
					except BaseException:
						self._release()
						raise
				return
			except Exception as e:
				log.warning("event=worker.consume_error err=%r retry_in=%.1fs", e, backoff)
				await anyio.sleep(backoff)
				backoff = min(backoff * 2, 30.0)
				try:
					await self.app.broker.connect()
				except Exception as ce:
					log.warning("event=worker.reconnect_failed err=%r", ce)

	async def _handle(self, delivery: Delivery) -> None:
		msg = delivery.message
		task = self.app.registry.get(msg.task)
		ctx = Context(app=self.app, message=msg, phase="process", task=task)

		token = _log_capture.set_current_msg(msg)
		try:
			await self._handle_inner(delivery, msg, task, ctx)
		except Exception as e:
			log.exception(
				"event=worker.handler_failed task=%s key=%s error=%s",
				msg.task,
				result_key(msg),
				e,
			)
		finally:
			try:
				_log_capture.reset_current_msg(token)
				_log_capture.flush()
			finally:
				self._release()

	async def _handle_inner(self, delivery: Delivery, msg: Any, task: Any, ctx: Context) -> None:

		log.debug(
			"event=worker.handle task=%s key=%s attempt=%s", msg.task, result_key(msg), msg.attempt
		)

		with anyio.CancelScope(shield=True):
			await self.app.events.task_received.send(msg)

		if msg.id in self._revoked:
			# revoked before it ever started: drop it, never run the body
			log.info("event=worker.revoke_pending task=%s id=%s", msg.task, msg.id)
			with anyio.CancelScope(shield=True):
				await self._finalize(delivery, msg, Cancelled())
			return

		if task is None:
			# schedule+ack instead of nack: keeps attempt untouched so the
			# message survives until a worker that knows the task claims it
			log.warning(
				"event=worker.unknown_task task=%s queue=%s requeue_delay=%s",
				msg.task,
				delivery.queue,
				self.unknown_task_delay,
			)
			with anyio.CancelScope(shield=True):
				await self.app.broker.schedule(
					msg, utcnow() + timedelta(seconds=self.unknown_task_delay)
				)
				await self.app.broker.ack(delivery)
			return

		outcome: Outcome
		cancelled = False
		results = self.app.results
		key = result_key(msg)

		if results is not None and results.replay:
			log.debug("event=worker.handle.replay_check key=%s", key)
			cached = await results.get(key, listen_timeout=0)
			if cached is not None and cached.status == "ok":
				log.debug("event=worker.handle.replay_hit key=%s", key)
				with anyio.CancelScope(shield=True):
					await self._finalize(delivery, msg, Ok(0.0))
				return

		scope = anyio.CancelScope()
		self._scopes[msg.id] = scope
		try:
			with scope:
				try:

					async def _terminal(c: Context) -> Any:
						await self.app.events.task_started.send(c.message)
						payload = (
							_coerce_payload(task.original_func, c.message.payload)
							if c.message.payload.args or c.message.payload.kwargs
							else c.message.payload
						)
						if task.blocking:

							def _call() -> Any:
								return task.original_func(*payload.args, **payload.kwargs)

							return await anyio.to_thread.run_sync(_call, abandon_on_cancel=True)
						r = task.original_func(*payload.args, **payload.kwargs)
						if inspect.isawaitable(r):
							r = await r
						return r

					started = time.perf_counter()
					log.debug("event=worker.handle.executing_task task=%s", msg.task)
					value = await run_chain(ctx, self.app.middleware, _terminal)
					log.debug("event=worker.handle.task_completed task=%s", msg.task)
					with anyio.CancelScope(shield=True):
						if results is not None:
							payload, type_fqn = results.encode(value)
							await results.set(
								key,
								Result(status="ok", value=payload, type=type_fqn),
								ttl=results.ttl,
							)
							log.debug("event=worker.handle.result_stored key=%s", key)
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
					cancelled = isinstance(e, anyio.get_cancelled_exc_class())
					if cancelled and msg.id in self._revoked:
						# deliberate revoke of a running task: drop, don't requeue
						outcome = Cancelled()
					else:
						outcome = Fail(e)
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
		finally:
			self._scopes.pop(msg.id, None)

		with anyio.CancelScope(shield=True):
			await self._finalize(delivery, msg, outcome)

		if isinstance(outcome, Fail) and cancelled:
			raise outcome.exc

	def _release(self) -> None:
		self._inflight -= 1
		if self._inflight == 0:
			self._idle.set()
		self._sem.release()

	async def _finalize(self, delivery: Delivery[Any], msg: Any, outcome: Outcome) -> None:
		match outcome:
			case Cancelled():
				await self.app.broker.ack(delivery)  # drop — never requeue a revoked task
				if self.app.results is not None:
					await self.app.results.set(
						result_key(msg),
						Result(status="cancelled"),
						ttl=self.app.results.ttl,
					)
				await self.app.events.task_cancelled.send(msg)
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
