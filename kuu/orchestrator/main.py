from __future__ import annotations

import logging
import multiprocessing
import os
import shutil
import signal
import tempfile
import threading
import time
import typing
from pathlib import Path

import anyio
import anyio.to_thread
import watchfiles

if typing.TYPE_CHECKING:
	from wsgiref.simple_server import WSGIServer

from ._worker import run_worker

log = logging.getLogger("kuu.orchestrator")


class Orchestrator:
	def __init__(
		self,
		app_spec: str,
		task_modules: list[str],
		queues: list[str] | None = None,
		concurrency: int = 64,
		prefetch: int | None = None,
		shutdown_timeout: float = 30.0,
		subprocesses: int = 1,
		watch_fs: bool = True,
		path_to_watch: Path | str = Path.cwd(),
		reload_delay: float = 0.25,
		reload_debounce: float = 0.5,
		metrics_port: int | None = None,
		metrics_host: str = "0.0.0.0",
	) -> None:
		self.app_spec = app_spec
		self.task_modules = task_modules
		self.queues = queues
		self.concurrency = concurrency
		self.prefetch = prefetch or max(1, concurrency // 4)
		self.shutdown_timeout = shutdown_timeout
		self.subprocesses = subprocesses
		self.watch_fs = watch_fs
		self.path_to_watch = path_to_watch
		self.reload_delay = reload_delay
		self.reload_debounce = reload_debounce
		self.metrics_port = metrics_port
		self.metrics_host = metrics_host
		self._processes: list[multiprocessing.Process] = []
		self._stop_event = anyio.Event()
		self._metrics_dir: str | None = None
		self._metrics_server: WSGIServer | None = None

	async def start(self) -> None:
		"""
		run until shutdown is requested
		non-blocking from the callers loop
		"""
		log.info(
			"starting orchestrator subprocesses=%d watch_fs=%s watch_paths=%s",
			self.subprocesses,
			self.watch_fs,
			self.path_to_watch,
		)
		try:
			self._start_metrics_server()
			async with anyio.create_task_group() as tg:
				tg.start_soon(self._signal_listener)
				try:
					await self._start_workers()
					if self.watch_fs:
						await self._watch_files()
					else:
						await self._stop_event.wait()
				finally:
					self._stop_event.set()
					tg.cancel_scope.cancel()
		except Exception:
			log.exception("orchestrator loop failed")
		finally:
			await self._stop_workers()
			self._stop_metrics_server()

	def _start_metrics_server(self) -> None:
		if self.metrics_port is None:
			return
		from kuu.prometheus import serve

		self._metrics_dir = tempfile.mkdtemp(prefix="kuu-prom-")
		os.environ["PROMETHEUS_MULTIPROC_DIR"] = self._metrics_dir

		self._metrics_server, _ = serve(
			host=self.metrics_host,
			port=self.metrics_port,
			multiprocess_dir=self._metrics_dir,
		)
		log.info(
			"prometheus aggregator on %s:%d (dir=%s)",
			self.metrics_host,
			self.metrics_port,
			self._metrics_dir,
		)

	def _stop_metrics_server(self) -> None:
		srv = self._metrics_server
		self._metrics_server = None
		if srv is not None:
			try:
				srv.shutdown()
			except Exception:
				log.exception("failed to shut down metrics server")
		if self._metrics_dir:
			shutil.rmtree(self._metrics_dir, ignore_errors=True)
			self._metrics_dir = None

	async def _signal_listener(self) -> None:
		if threading.current_thread() is not threading.main_thread():
			log.debug("not on main thread; skipping signal handler installation")
			return
		with anyio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
			async for signum in signals:
				log.info("received signal %d, initiating shutdown", signum)
				self._stop_event.set()
				return

	async def _start_workers(self) -> None:
		for i in range(self.subprocesses):
			if self._stop_event.is_set():
				break
			log.info("starting worker process %d/%d", i + 1, self.subprocesses)
			p = multiprocessing.Process(
				target=run_worker,
				args=(
					self.app_spec,
					self.task_modules,
					self.queues,
					self.concurrency,
					self.prefetch,
					self.shutdown_timeout,
					self.metrics_port is not None,
				),
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

	def _terminate_and_wait(self, processes: list[multiprocessing.Process]) -> None:
		for p in processes:
			if p.is_alive():
				p.terminate()

		deadline = time.monotonic() + self.shutdown_timeout
		for p in processes:
			remaining = deadline - time.monotonic()
			if remaining > 0:
				p.join(timeout=remaining)

		for p in processes:
			if p.is_alive():
				log.warning("worker %s did not terminate gracefully, killing", p.pid)
				p.kill()
				p.join(timeout=5)

		if self.metrics_port is not None:
			from kuu.prometheus import mark_worker_dead

			for p in processes:
				if p.pid is not None:
					try:
						mark_worker_dead(p.pid)
					except Exception:
						log.exception("mark_worker_dead failed for pid=%s", p.pid)

	async def _watch_files(self) -> None:
		log.info(
			"watching %s (step=%dms, debounce=%dms)",
			self.path_to_watch,
			int(self.reload_delay * 1000),
			int(self.reload_debounce * 1000),
		)

		async for changes in watchfiles.awatch(
			self.path_to_watch,
			stop_event=self._stop_event,
			step=int(self.reload_delay * 1000),
			debounce=int(self.reload_debounce * 1000),
			recursive=True,
		):
			if self._stop_event.is_set():
				break

			log.info("detected changes in %d file(s), reloading workers", len(changes))
			await self._stop_workers()

			if self._stop_event.is_set():
				break

			await self._start_workers()
