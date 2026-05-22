from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING

import anyio

from kuu._queue_drain import drain_sync_queue
from kuu.config import Settings
from kuu.orchestrator._serve import serve_uvicorn_until_stop

if TYPE_CHECKING:
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
			from kuu._import import import_object, import_tasks
			from kuu.web.dashboard import Dashboard
		except ImportError as e:
			log.exception("event=dashboard_runner.dependencies_missing error=%s", e)
			return

		kuu = import_object(self._config.app)  # type: ignore[arg-type]
		import_tasks(self._config.task_modules, pattern=(), fs_discover=False)
		dashboard = Dashboard(app=kuu, orchestrator=self._orch)
		asgi_app = dashboard.build_app()

		async def _serve_dashboard() -> None:
			await serve_uvicorn_until_stop(
				asgi_app,
				dash_config,
				stop_event,
				shutdown_timeout=self._config.shutdown_timeout,
				log_event="event=dashboard_runner.serving",
			)

		async with anyio.create_task_group() as tg:
			tg.start_soon(_serve_dashboard)
			tg.start_soon(self._drain_events, dashboard.stats, stop_event)
			await stop_event.wait()
			tg.cancel_scope.cancel()

	async def _drain_events(self, stats: StatsCollector, stop_event: anyio.Event) -> None:
		q = self._orch.worker_pool.events_queue

		def _ingest(we: object) -> None:
			from kuu.observability._protocol import Event

			if not isinstance(we, Event):
				return
			stats.ingest(we.kind, we.task, we.ts)

		await drain_sync_queue(
			q,
			_ingest,
			stop_event=stop_event,
			batch_size=100,
			idle_sleep=1.0,
			error_sleep=1.0,
			logger=log,
			error_event="event=dashboard_runner.drain_error",
		)
