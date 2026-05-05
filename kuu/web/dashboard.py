from __future__ import annotations

from pathlib import Path

import orjson
from jinja2 import Environment, FileSystemLoader, select_autoescape
from starlette.applications import Starlette
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles

from kuu.app import Kuu
from kuu.observability import InstanceRegistry
from kuu.orchestrator.main import PresetSupervisor
from kuu.scheduler.scheduler import Scheduler
from kuu.web.api import DashbordAPIMixin
from kuu.web.fragments import DashboardFragmentsMixin
from kuu.web.stats import StatsCollector


class Dashboard(DashboardFragmentsMixin, DashbordAPIMixin):
	def __init__(
		self,
		app: Kuu,
		scheduler: Scheduler | None = None,
		orchestrator: PresetSupervisor | None = None,
		registry: InstanceRegistry | None = None,
		title: str = "kuu dashboard",
	) -> None:
		"""
		``orchestrator`` is the legacy single-process supervisor whose
		``_wp._processes`` provides the workers fragment. ``registry``
		is the control-plane roster; when present, fragments prefer it
		and aggregate workers across all live instances.
		"""
		self.app = app
		self.scheduler = scheduler
		self.orchestrator = orchestrator
		self.registry = registry
		self.title = title
		self.stats = StatsCollector(app, connect_app_events=registry is None)
		here = Path(__file__).parent
		self.jinja = Environment(
			loader=FileSystemLoader(str(here / "templates")),
			autoescape=select_autoescape(["html", "xml"]),
		)
		self.jinja.filters["tojson"] = orjson.dumps

	def build_app(self) -> Starlette:
		static_dir = Path(__file__).parent / "static"
		return Starlette(
			debug=True,
			routes=[
				Route("/", self._index),
				Route("/fragments/stats", self._frag_stats),
				Route("/fragments/tasks", self._frag_tasks),
				Route("/fragments/scheduler", self._frag_scheduler),
				Route("/fragments/workers", self._frag_workers),
				Route("/api/activity", self._api_activity),
				Route("/api/task-params", self._api_task_params),
				Route("/api/run-task", self._api_run_task, methods=["POST"]),
				Route("/api/trigger-job", self._api_trigger_job, methods=["POST"]),
				Route("/api/remove-job", self._api_remove_job, methods=["POST"]),
				Mount("/static", StaticFiles(directory=str(static_dir)), name="static"),
			],
		)

	def serve(self, host: str = "0.0.0.0", port: int = 8000) -> None:
		import uvicorn

		uvicorn.run(self.build_app(), host=host, port=port, log_level="warning")

	async def start_server(self, host: str = "0.0.0.0", port: int = 8000) -> None:
		import uvicorn

		cfg = uvicorn.Config(self.build_app(), host=host, port=port, log_level="warning")
		await uvicorn.Server(cfg).serve()
