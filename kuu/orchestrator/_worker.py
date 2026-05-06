from __future__ import annotations

import logging
import multiprocessing as mp
import time
import traceback as _traceback_module
from dataclasses import dataclass
from multiprocessing.context import SpawnProcess
from typing import TYPE_CHECKING, Any, Literal

import anyio
import anyio.to_thread

from kuu._import import import_object, import_tasks
from kuu.app import Kuu
from kuu.config import Settings
from kuu.worker import Worker

if TYPE_CHECKING:
	from kuu.orchestrator._watcher import Changes

log = logging.getLogger("kuu.orchestrator.worker_pool")


WorkerEventKind = Literal["started", "succeeded", "failed", "retried", "dead"]


_MAX_ARG_REPR = 500
_MAX_TRACEBACK = 8192


def _safe_repr(obj: Any) -> str | None:
	"""repr an object, truncated to _MAX_ARG_REPR chars"""
	if obj is None:
		return None
	try:
		s = repr(obj)
		if len(s) > _MAX_ARG_REPR:
			return s[:_MAX_ARG_REPR] + "...<truncated>"
		return s
	except Exception:
		return "<unrepresentable>"


def _args_repr(args: tuple[Any, ...]) -> str | None:
	if not args:
		return None
	joined = ", ".join(_safe_repr(a) or "?" for a in args)
	if len(joined) > _MAX_ARG_REPR:
		return joined[:_MAX_ARG_REPR] + "...<truncated>"
	return joined


def _kwargs_repr(kwargs: dict[str, Any]) -> str | None:
	if not kwargs:
		return None
	parts = [f"{k}={_safe_repr(v) or '?'}" for k, v in kwargs.items()]
	joined = ", ".join(parts)
	if len(joined) > _MAX_ARG_REPR:
		return joined[:_MAX_ARG_REPR] + "...<truncated>"
	return joined


@dataclass(frozen=True, slots=True)
class WorkerEvent:
	"""ipc record pushed by a worker subprocess onto the shared mp.Queue

	the supervisor consumes these to update in-flight counters, current-task
	tracking, and to build outgoing :class:`kuu.observability.Event` envelopes
	"""

	kind: WorkerEventKind
	task: str
	queue: str
	ts: float
	pid: int
	elapsed: float | None = None
	message_id: str | None = None
	attempt: int | None = None
	args_repr: str | None = None
	kwargs_repr: str | None = None
	exc_type: str | None = None
	exc_message: str | None = None
	traceback: str | None = None


class WorkerPool:
	_config: Settings
	_stop_event: anyio.Event
	_processes: list[SpawnProcess]
	events_queue: mp.Queue

	def __init__(self, config: Settings) -> None:
		self._config = config
		self._mp_ctx = mp.get_context("spawn")
		self._processes = []
		self.events_queue = mp.Queue()

	async def run(self, stop_event: anyio.Event) -> None:
		self._stop_event = stop_event
		try:
			await self._start_workers()
			await stop_event.wait()
		finally:
			await self._stop_workers()

	async def on_change_callback(self, changes: Changes) -> None:
		await self._stop_workers()
		if self._stop_event.is_set():
			return
		await self._start_workers()

	async def _start_workers(self) -> None:
		current_limiter = anyio.to_thread.current_default_thread_limiter()

		if self._config.concurrency > current_limiter.available_tokens:
			current_limiter.total_tokens += int(
				(self._config.concurrency - current_limiter.available_tokens) * 1.2
			)

		for i in range(self._config.processes):
			if self._stop_event.is_set():
				break
			log.info("starting worker process %d/%d", i + 1, self._config.processes)
			p = self._mp_ctx.Process(
				target=_run_worker,
				args=(self._config, self.events_queue),
				daemon=False,
			)
			await anyio.to_thread.run_sync(p.start)
			self._processes.append(p)

	async def _stop_workers(self) -> None:
		if not self._processes:
			return

		log.info("stopping %d worker process(es)", len(self._processes))
		processes = self._processes
		self._processes = []

		await anyio.to_thread.run_sync(self._terminate_and_wait, processes)

	def _terminate_and_wait(self, processes: list[SpawnProcess]) -> None:
		for p in processes:
			if p.is_alive():
				p.terminate()

		deadline = time.monotonic() + self._config.shutdown_timeout
		for p in processes:
			remaining = deadline - time.monotonic()
			if remaining > 0:
				p.join(timeout=remaining)

		for p in processes:
			if p.is_alive():
				log.warning("worker %s did not terminate gracefully, killing", p.pid)
				p.kill()
				p.join(timeout=5)

		if self._config.metrics.enable:
			from kuu.prometheus import mark_worker_dead

			for p in processes:
				if p.pid is not None:
					try:
						mark_worker_dead(p.pid)
					except Exception:
						log.exception("mark_worker_dead failed for pid=%s", p.pid)


def _run_worker(config: Settings, events_queue: mp.Queue | None = None) -> None:
	log.info("worker process starting")
	app = import_object(config.app)  # type:ignore
	import_tasks(config.task_modules, "", False)

	if config.metrics.enable:
		from kuu.prometheus import WorkerMetrics

		WorkerMetrics(app)

	if events_queue is not None:
		from kuu.observability import _log_capture

		level = _resolve_log_level(config.persistence.log_level)
		_log_capture.install(events_queue, level=level)
		_install_event_forwarder(app, events_queue)

	try:
		anyio.run(Worker(config, app=app).run)
	finally:
		if events_queue is not None:
			from kuu.observability import _log_capture

			_log_capture.shutdown()
	log.info("worker process exiting")


def _resolve_log_level(name: str) -> int:
	"""coerce a level name to logging.* int; falls back to INFO"""
	try:
		val = logging.getLevelNamesMapping().get(name.upper())
	except AttributeError:
		val = getattr(logging, name.upper(), None)
	return val if isinstance(val, int) else logging.INFO


def _install_event_forwarder(app: Kuu, q: mp.Queue) -> None:
	"""push :class:`WorkerEvent` records onto the inter-process queue"""
	import os

	pid = os.getpid()

	def _put(
		kind: WorkerEventKind,
		task: str,
		queue: str,
		elapsed: float | None = None,
		msg: Any = None,
		exc: BaseException | None = None,
	) -> None:
		message_id: str | None = None
		attempt: int | None = None
		args_repr_val: str | None = None
		kwargs_repr_val: str | None = None
		exc_type: str | None = None
		exc_message: str | None = None
		traceback_str: str | None = None
		if msg is not None:
			message_id = str(msg.id)
			attempt = msg.attempt
			if msg.payload.args:
				args_repr_val = _args_repr(msg.payload.args)
			if msg.payload.kwargs:
				kwargs_repr_val = _kwargs_repr(msg.payload.kwargs)
		if exc is not None:
			exc_type = type(exc).__name__
			exc_message_val = str(exc)
			if len(exc_message_val) > _MAX_ARG_REPR:
				exc_message_val = exc_message_val[:_MAX_ARG_REPR] + "...<truncated>"
			exc_message = exc_message_val
			tb_obj = exc.__traceback__
			if tb_obj is not None:
				tb = "".join(_traceback_module.format_exception(type(exc), exc, tb_obj))
			else:
				tb = f"{type(exc).__name__}: {exc}"
			if len(tb) > _MAX_TRACEBACK:
				tb = tb[:_MAX_TRACEBACK] + "...<truncated>"
			traceback_str = tb
		try:
			q.put_nowait(
				WorkerEvent(
					kind=kind,
					task=task,
					queue=queue,
					ts=time.time(),
					pid=pid,
					elapsed=elapsed,
					message_id=message_id,
					attempt=attempt,
					args_repr=args_repr_val,
					kwargs_repr=kwargs_repr_val,
					exc_type=exc_type,
					exc_message=exc_message,
					traceback=traceback_str,
				)
			)
		except Exception:
			pass

	ev = app.events
	ev.task_started.connect(lambda msg: _put("started", msg.task, msg.queue, msg=msg))
	ev.task_succeeded.connect(
		lambda msg, elapsed: _put("succeeded", msg.task, msg.queue, elapsed=elapsed, msg=msg)
	)
	ev.task_failed.connect(lambda msg, exc: _put("failed", msg.task, msg.queue, msg=msg, exc=exc))
	ev.task_retried.connect(lambda msg, delay: _put("retried", msg.task, msg.queue, msg=msg))
	ev.task_dead.connect(lambda msg: _put("dead", msg.task, msg.queue, msg=msg))
