from __future__ import annotations

import queue
from logging import getLogger
from typing import TYPE_CHECKING

import anyio

from kuu.config import Settings

if TYPE_CHECKING:
	import multiprocessing as mp

	from kuu.orchestrator.main import PresetSupervisor
	from kuu.web.stats import StatsCollector

log = getLogger("kuu.orchestrator.dashboard-runner")


class DashboardRunner:
	_orch: PresetSupervisor
	_config: Settings

	def __init__(self, orch: PresetSupervisor, config: Settings) -> None:
		self._orch = orch
		self._config = config

	async def run(self, stop_event: anyio.Event) -> None:
		dash_config = self._config.dashboard

		if not dash_config.enable:
			return

		try:
			import uvicorn

			from kuu._import import import_object, import_tasks
			from kuu.web.dashboard import Dashboard
		except ImportError:
			log.exception("dashboard dependencies missing; install kuu[dashboard]")
			return

		kuu = import_object(self._config.app)
		import_tasks(self._config.task_modules, pattern=(), fs_discover=False)
		dashboard = Dashboard(app=kuu, orchestrator=self._orch)
		asgi_app = dashboard.build_app()

		if dash_config.path and dash_config.path != "/":
			from starlette.applications import Starlette
			from starlette.routing import Mount

			asgi_app = Starlette(routes=[Mount(dash_config.path, app=asgi_app)])

		cfg = uvicorn.Config(
			asgi_app,
			host=dash_config.host,
			port=dash_config.port,
			log_level="warning",
		)
		server = uvicorn.Server(cfg)
		log.info(
			"dashboard serving on http://%s:%d%s",
			dash_config.host,
			dash_config.port,
			dash_config.path,
		)
		try:
			async with anyio.create_task_group() as tg:
				tg.start_soon(server.serve)
				tg.start_soon(self._drain_events, dashboard.stats, stop_event)
				await stop_event.wait()
		finally:
			with anyio.move_on_after(delay=self._config.shutdown_timeout):
				await server.shutdown()
			if server.started:
				server.force_exit = True

	def _worker_events_queue(self) -> mp.Queue | None:
		try:
			return self._orch._wp.events_queue
		except AttributeError:
			return None

	async def _drain_events(self, stats: StatsCollector, stop_event: anyio.Event) -> None:
		"""drain :class:`WorkerEvent` and feed StatsCollector"""
		q = self._worker_events_queue()
		if q is None:
			return
		while not stop_event.is_set():
			try:
				while True:
					we = q.get_nowait()
					stats.ingest(we.kind, we.task, we.ts)
			except queue.Empty:
				pass
			await anyio.sleep(1.0)
