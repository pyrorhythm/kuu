from __future__ import annotations

import json
import logging
import math
import multiprocessing as mp
import time
from multiprocessing.context import SpawnProcess
from typing import TYPE_CHECKING, Any, Literal

import anyio
import anyio.to_thread

from kuu._import import import_object, import_tasks
from kuu.app import Kuu
from kuu.config import Settings
from kuu.result import RemoteFailure, capture_remote_failure
from kuu.worker import Worker

if TYPE_CHECKING:
	from kuu.orchestrator._watcher import Changes

log = logging.getLogger("kuu.orchestrator.worker_pool")


WorkerEventKind = Literal["started", "succeeded", "failed", "retried", "dead"]


_MONITOR_INTERVAL = 1.0
_SENSITIVE_PARTS = (
	"access_key",
	"api_key",
	"apikey",
	"authorization",
	"cookie",
	"password",
	"private_key",
	"secret",
	"token",
)


def _safe_capture(value: Any, limit: int) -> tuple[Any, bool]:
	def scrub(item: Any, depth: int = 0) -> tuple[Any, bool]:
		if item is None or isinstance(item, bool | int | str):
			return item, True
		if isinstance(item, float):
			return (item, True) if math.isfinite(item) else (repr(item), False)
		if depth >= 8:
			return "<max-depth>", False
		if isinstance(item, dict):
			out: dict[str, Any] = {}
			complete = len(item) <= 1000
			for key, child in list(item.items())[:1000]:
				name = str(key)
				if any(part in name.lower() for part in _SENSITIVE_PARTS):
					out[name] = "<redacted>"
					complete = False
					continue
				out[name], child_complete = scrub(child, depth + 1)
				complete = complete and child_complete
			return out, complete
		if isinstance(item, list | tuple):
			values = [scrub(child, depth + 1) for child in list(item)[:1000]]
			return [child for child, _ in values], (
				len(item) <= 1000 and all(complete for _, complete in values)
			)
		try:
			return repr(item), False
		except Exception:
			return "<unrepresentable>", False

	safe, complete = scrub(value)
	encoded = json.dumps(safe, ensure_ascii=False, separators=(",", ":")).encode()
	if len(encoded) <= limit:
		return safe, complete
	prefix_bytes = min(len(encoded), limit)
	while prefix_bytes >= 0:
		preview = encoded[:prefix_bytes].decode(errors="ignore")
		truncated = {"_truncated": preview}
		final_size = len(json.dumps(truncated, ensure_ascii=False, separators=(",", ":")).encode())
		if final_size <= limit:
			return truncated, False
		prefix_bytes -= max(1, final_size - limit)
	return "", False


class WorkerPool:
	_config: Settings
	_stop_event: anyio.Event
	_processes: list[SpawnProcess]
	events_queue: mp.Queue

	def __init__(self, config: Settings) -> None:
		self._config = config
		self._mp_ctx = mp.get_context("spawn")
		self._processes = []
		self._workers_lock = anyio.Lock()
		self.events_queue = self._mp_ctx.Queue()

	@property
	def processes(self) -> list[SpawnProcess]:
		return self._processes

	async def run(self, stop_event: anyio.Event) -> None:
		self._stop_event = stop_event
		try:
			await self._start_workers()
			async with anyio.create_task_group() as tg:
				tg.start_soon(self._monitor_workers)
				await stop_event.wait()
				tg.cancel_scope.cancel()
		finally:
			await self._stop_workers()

	async def on_change_callback(self, changes: Changes) -> None:
		await self._stop_workers()
		if self._stop_event.is_set():
			return
		await self._start_workers()

	async def _start_workers(self) -> None:
		async with self._workers_lock:
			for i in range(self._config.processes):
				if self._stop_event.is_set():
					break
				await self._spawn_worker_locked(i + 1)

	async def _spawn_worker_locked(self, index: int) -> None:
		log.info("event=worker_pool.starting index=%d total=%d", index, self._config.processes)
		p = self._mp_ctx.Process(
			target=_run_worker,
			args=(self._config, self.events_queue),
			daemon=False,
		)
		await anyio.to_thread.run_sync(p.start)
		self._processes.append(p)

	async def _monitor_workers(self) -> None:
		while not self._stop_event.is_set():
			async with self._workers_lock:
				for p in list(self._processes):
					if p.is_alive():
						continue
					p.join(timeout=0)
					self._processes.remove(p)
					self._mark_worker_dead(p)
					log.error(
						"event=worker_pool.worker_exited pid=%s exitcode=%s action=restart",
						p.pid,
						p.exitcode,
					)
					if not self._stop_event.is_set():
						await self._spawn_worker_locked(len(self._processes) + 1)
			with anyio.move_on_after(_MONITOR_INTERVAL):
				await self._stop_event.wait()

	async def _stop_workers(self) -> None:
		async with self._workers_lock:
			if not self._processes:
				return

			log.info("event=worker_pool.stopping count=%d", len(self._processes))
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
				log.warning("event=worker_pool.killing pid=%s", p.pid)
				p.kill()
				p.join(timeout=5)

		for p in processes:
			self._mark_worker_dead(p)

	def _mark_worker_dead(self, p: SpawnProcess) -> None:
		if not self._config.metrics.enable or p.pid is None:
			return
		from kuu.contrib.prometheus import mark_worker_dead

		try:
			mark_worker_dead(p.pid)
		except Exception as e:
			log.exception("event=worker_pool.mark_dead_failed pid=%s error=%s", p.pid, e)


def _run_worker(config: Settings, events_queue: mp.Queue | None = None) -> None:
	log.info("event=worker_pool.process_starting")
	app = import_object(config.app)  # type:ignore
	import_tasks(config.task_modules, "", False)

	if config.metrics.enable:
		from kuu.contrib.prometheus import WorkerMetrics

		WorkerMetrics(app)

	if events_queue is not None:
		from kuu.observability import _log_capture

		level = _resolve_log_level(config.persistence.log_level)
		_log_capture.install(
			events_queue,
			level=level,
			max_attempt_bytes=config.persistence.attempt_observation_bytes,
		)
		_install_event_forwarder(
			app,
			events_queue,
			capture_args=config.persistence.capture_args,
			capture_headers=config.persistence.capture_headers,
			capture_result=config.persistence.capture_result,
			preview_bytes=config.persistence.preview_bytes,
		)

	try:
		anyio.run(Worker(config, app=app).run)
	except BaseException as e:
		log.exception("event=worker_pool.process_failed error=%s", e)
		raise
	finally:
		if events_queue is not None:
			from kuu.observability import _log_capture

			_log_capture.shutdown()
	log.info("event=worker_pool.process_exiting")


def _resolve_log_level(name: str) -> int:
	"""coerce a level name to logging.* int; falls back to INFO"""
	try:
		val = logging.getLevelNamesMapping().get(name.upper())
	except AttributeError:
		val = getattr(logging, name.upper(), None)
	return val if isinstance(val, int) else logging.INFO


def _install_event_forwarder(
	app: Kuu,
	q: mp.Queue,
	*,
	capture_args: bool = False,
	capture_headers: bool = False,
	capture_result: bool = False,
	preview_bytes: int = 16 * 1024,
) -> None:
	"""push :class:`WorkerEvent` records onto the inter-process queue"""
	import os

	from kuu.observability import Event, EventKind

	pid = os.getpid()
	result_previews: dict[tuple[str, int], Any] = {}

	def _put(
		kind: EventKind,
		task: str,
		queue: str,
		elapsed: float | None = None,
		msg: Any = None,
		exc: BaseException | None = None,
		result_preview: Any = None,
	) -> None:
		message_id: str | None = None
		attempt: int | None = None
		args: Any = None
		kwargs: Any = None
		headers: Any = None
		input_complete = False
		exc_type: str | None = None
		exc_message: str | None = None
		traceback_str: str | None = None
		failure: RemoteFailure | None = None
		replay_of: str | None = None
		if msg is not None:
			message_id = str(msg.id)
			attempt = msg.attempt
			replay_of = msg.headers.get("kuu.replay_of")
			definition = app.registry.get(msg.task)
			if capture_args or (definition is not None and definition.capture_args):
				captured, input_complete = _safe_capture(
					{"args": msg.payload.args, "kwargs": msg.payload.kwargs},
					preview_bytes,
				)
				if isinstance(captured, dict) and "args" in captured:
					args = captured["args"]
					kwargs = captured["kwargs"]
			if capture_headers or (definition is not None and definition.capture_headers):
				headers, _ = _safe_capture(msg.headers, preview_bytes)
		if exc is not None:
			failure = capture_remote_failure(exc)
			exc_type = failure.type_name
			exc_message = failure.message
			traceback_str = failure.traceback
		try:
			q.put_nowait(
				Event(
					worker_pid=pid,
					kind=kind,
					task=task,
					queue=queue,
					elapsed=elapsed,
					message_id=message_id,
					attempt=attempt,
					args=args,
					kwargs=kwargs,
					headers=headers,
					input_complete=input_complete,
					result_preview=result_preview,
					exc_type=exc_type,
					exc_message=exc_message,
					traceback=traceback_str,
					failure=failure,
					replay_of=replay_of,
				)
			)
		except Exception:
			pass

	def _capture_result(msg: Any, value: Any) -> None:
		definition = app.registry.get(msg.task)
		if capture_result or (definition is not None and definition.capture_result):
			result_previews[(str(msg.id), msg.attempt)] = _safe_capture(value, preview_bytes)[0]

	def _succeeded(msg: Any, elapsed: float) -> None:
		_put(
			"succeeded",
			msg.task,
			msg.queue,
			elapsed=elapsed,
			msg=msg,
			result_preview=result_previews.pop((str(msg.id), msg.attempt), None),
		)

	app.events.task_enqueued.connect(lambda msg: _put("enqueued", msg.task, msg.queue, msg=msg))
	app.events.task_started.connect(lambda msg: _put("started", msg.task, msg.queue, msg=msg))
	app.events.task_result.connect(_capture_result)
	app.events.task_succeeded.connect(_succeeded)
	app.events.task_failed.connect(
		lambda msg, exc: _put("failed", msg.task, msg.queue, msg=msg, exc=exc)
	)
	app.events.task_retried.connect(
		lambda msg, delay: _put("retried", msg.task, msg.queue, msg=msg)
	)
	app.events.task_dead.connect(lambda msg: _put("dead", msg.task, msg.queue, msg=msg))
	app.events.task_cancelled.connect(lambda msg: _put("cancelled", msg.task, msg.queue, msg=msg))
