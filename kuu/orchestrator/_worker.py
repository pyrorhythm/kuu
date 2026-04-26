from __future__ import annotations

import logging
import multiprocessing as mp
import time
from typing import TYPE_CHECKING

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

	def __init__(self, config: Kuunfig) -> None:
		self._config = config
		self._processes = []

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
			current_limiter.total_tokens += (
				self._config.concurrency - current_limiter.available_tokens
			) * 1.2

		for i in range(self._config.processes):
			if self._stop_event.is_set():
				break
			log.info("starting worker process %d/%d", i + 1, self._config.processes)
			p = mp.Process(
				target=_run_worker,
				args=(self._config,),
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


def _run_worker(config: Kuunfig) -> None:
	log.info("worker process starting")
	app = import_object(config.app)
	import_tasks(config.task_modules, "", False)

	if config.metrics.enable:
		from kuu.prometheus import WorkerMetrics

		WorkerMetrics(app)

	anyio.run(Worker(config, app=app).run)
	log.info("worker process exiting")
