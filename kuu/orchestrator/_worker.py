from __future__ import annotations

import logging
import multiprocessing as mp
import time
from typing import TYPE_CHECKING, Any

import anyio
import anyio.to_thread

from kuu._import import import_object, import_tasks
from kuu.config import Kuunfig
from kuu.worker import Worker

if TYPE_CHECKING:
	from kuu.orchestrator._watcher import Changes

log = logging.getLogger("kuu.orchestrator.worker_pool")


class WorkerPool:
	_config: Kuunfig
	_stop_event: anyio.Event
	_processes: list[mp.Process]
	events_queue: mp.Queue  # type: ignore[type-arg]

	def __init__(self, config: Kuunfig) -> None:
		self._config = config
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
			p = mp.Process(
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

	def _terminate_and_wait(self, processes: list[mp.Process]) -> None:
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


def _run_worker(config: Kuunfig, events_queue: mp.Queue | None = None) -> None:  # type: ignore[type-arg]
	log.info("worker process starting")
	app = import_object(config.app)
	import_tasks(config.task_modules, "", False)

	if config.metrics.enable:
		from kuu.prometheus import WorkerMetrics

		WorkerMetrics(app)

	if events_queue is not None:
		_install_event_forwarder(app, events_queue)

	anyio.run(Worker(config, app=app).run)
	log.info("worker process exiting")


def _install_event_forwarder(app: Any, q: mp.Queue) -> None:  # type: ignore[type-arg]
	"""Push lightweight event records onto the inter-process queue so the dashboard can consume them."""
	import os

	pid = os.getpid()

	def _put(event: str, task: str) -> None:
		try:
			q.put_nowait((event, task, time.time(), pid))
		except Exception:
			pass

	ev = app.events
	ev.task_succeeded.connect(lambda msg, elapsed: _put("succeeded", msg.task))
	ev.task_failed.connect(lambda msg, exc: _put("failed", msg.task))
	ev.task_retried.connect(lambda msg, delay: _put("retried", msg.task))
	ev.task_dead.connect(lambda msg: _put("dead", msg.task))
