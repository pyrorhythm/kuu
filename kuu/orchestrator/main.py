from __future__ import annotations

import logging
import os
import shutil
import signal
import threading
import typing

import anyio
import anyio.to_thread

from kuu.config import Kuunfig
from kuu.orchestrator._dashboard import DashboardRunner
from kuu.orchestrator._watcher import Watcher
from kuu.orchestrator._worker import WorkerPool

if typing.TYPE_CHECKING:
	from wsgiref.simple_server import WSGIServer


log = logging.getLogger("kuu.orchestrator")


class Orchestrator:
	config: Kuunfig

	_wp: WorkerPool
	_dash: DashboardRunner
	_watcher: Watcher

	_stop_event = anyio.Event()
	_metrics_dir: str | None = None
	_metrics_server: WSGIServer | None = None

	def __init__(
		self,
		config: Kuunfig,
	) -> None:
		self.config = config
		self._wp = WorkerPool(config)
		self._dash = DashboardRunner(self, config)
		self._watcher = Watcher(config, self._wp.on_change_callback)

	async def _signal_listener(self) -> None:
		if threading.current_thread() is not threading.main_thread():
			log.debug("not on main thread; skipping signal handler installation")
			return
		with anyio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
			async for signum in signals:
				log.info("received signal %d, initiating shutdown", signum)
				self._stop_event.set()
				return

	async def start(self) -> None:
		"""
		run until shutdown is requested
		non-blocking from the callers loop
		"""

		log.info(
			"starting orchestrator processes=%d watcher=%s dashboard=%s metrics=%s",
			self.config.processes,
			self.config.watch.enable,
			self.config.dashboard.enable,
			self.config.metrics.enable,
		)
		try:
			await self._start_metrics_server()
			async with anyio.create_task_group() as tg:
				tg.start_soon(self._signal_listener)
				tg.start_soon(self._dash.run, self._stop_event)
				tg.start_soon(self._wp.run, self._stop_event)
				tg.start_soon(self._watcher.run, self._stop_event)
				await self._stop_event.wait()
				tg.cancel_scope.cancel()
		except Exception:
			log.exception("orchestrator loop failed")
		finally:
			self._stop_event.set()
			self._stop_metrics_server()

	async def _start_metrics_server(self) -> None:
		metrics_config = self.config.metrics
		if not metrics_config.enable:
			return

		from kuu.prometheus import serve

		self._metrics_dir = await anyio.mkdtemp(prefix="kuu-prom-")
		os.environ["PROMETHEUS_MULTIPROC_DIR"] = self._metrics_dir

		self._metrics_server, _ = serve(
			host=metrics_config.host,
			port=metrics_config.port,
			multiprocess_dir=self._metrics_dir,
		)
		log.info(
			"prometheus aggregator on %s:%d (dir=%s)",
			metrics_config.host,
			metrics_config.port,
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
