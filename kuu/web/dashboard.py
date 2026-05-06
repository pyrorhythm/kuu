from __future__ import annotations

import typing
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

from kuu.marshal import marshal as _marshal
from kuu.persistence._backend import PersistenceBackend
from starlette.applications import Starlette
from starlette.routing import Mount, Route, WebSocketRoute
from starlette.staticfiles import StaticFiles
from starlette.websockets import WebSocket

from kuu.app import Kuu
from kuu.observability import (
	Bye,
	Envelope,
	Event,
	Hello,
	InstanceRegistry,
	State,
	envelope_from_bytes,
)
from kuu.orchestrator.main import PresetSupervisor
from kuu.scheduler.scheduler import Scheduler
from kuu.web.api import DashbordAPIMixin
from kuu.web.fragments import DashboardFragmentsMixin
from kuu.web.stats import StatsCollector

if typing.TYPE_CHECKING:
	from kuu.orchestrator._control import ControlPlane


class Dashboard(DashboardFragmentsMixin, DashbordAPIMixin):
	def __init__(
		self,
		app: Kuu | None = None,
		scheduler: Scheduler | None = None,
		orchestrator: PresetSupervisor | None = None,
		registry: InstanceRegistry | None = None,
		control: "ControlPlane | None" = None,
		title: str = "kuu dashboard",
		ingest_token: str | None = None,
		persistence_backend: PersistenceBackend | None = None,
	) -> None:
		self.app = app
		self.scheduler = scheduler
		self.orchestrator = orchestrator
		self.registry = registry
		self.control = control
		self.title = title
		self.persistence_backend = persistence_backend
		self._ingest_token = ingest_token
		self.stats = StatsCollector(app, connect_app_events=registry is None and app is not None)
		here = Path(__file__).parent
		self.jinja = Environment(
			loader=FileSystemLoader(str(here / "templates")),
			autoescape=select_autoescape(["html", "xml"]),
		)
		self.jinja.filters["tojson"] = lambda v: _marshal.json_encode(v).decode()

	def build_app(self) -> Starlette:
		static_dir = Path(__file__).parent / "static"
		return Starlette(
			debug=True,
			routes=[
				Route("/", self._index),
				Route("/fragments/stats", self._frag_stats),
				Route("/fragments/tasks", self._frag_tasks),
				Route("/fragments/task-runs", self._frag_task_runs),
				Route("/fragments/task-run-detail", self._frag_task_run_detail),
				Route("/fragments/scheduler", self._frag_scheduler),
				Route("/fragments/presets", self._frag_presets),
				Route("/fragments/queues", self._frag_queues),
				Route("/api/activity", self._api_activity),
				Route("/api/task-params", self._api_task_params),
				Route("/api/run-task", self._api_run_task, methods=["POST"]),
				Route("/api/trigger-job", self._api_trigger_job, methods=["POST"]),
				Route("/api/remove-job", self._api_remove_job, methods=["POST"]),
				Route("/api/task-runs", self._api_task_runs),
				Route("/api/task-run-attempts", self._api_task_run_attempts),
				Route("/api/task-run-logs", self._api_task_run_logs),
				WebSocketRoute("/_ingest", self._ws_ingest),
				Mount("/static", StaticFiles(directory=str(static_dir)), name="static"),
			],
		)

	def ingest_envelope(self, env: Envelope) -> None:
		if self.registry is not None:
			self.registry.ingest(env)
		match env.body:
			case Event() as e:
				self.stats.ingest(e.kind, e.task, env.ts)
			case Hello() | State() | Bye():
				pass
			case _:
				pass

	async def _ws_ingest(self, websocket: WebSocket) -> None:
		if not self._authorized(websocket):
			await websocket.close(code=4401)
			return
		await websocket.accept()
		try:
			while True:
				data = await websocket.receive_bytes()
				try:
					env = envelope_from_bytes(data)
				except Exception:
					continue
				self.ingest_envelope(env)
		except Exception:
			pass
		finally:
			try:
				await websocket.close()
			except Exception:
				pass

	def _authorized(self, websocket: WebSocket) -> bool:
		if self._ingest_token is None:
			return True
		auth = websocket.headers.get("authorization", "")
		expected = f"Bearer {self._ingest_token}"
		return auth == expected

	def serve(self, host: str = "0.0.0.0", port: int = 8000) -> None:
		import uvicorn

		uvicorn.run(self.build_app(), host=host, port=port, log_level="warning")

	async def start_server(self, host: str = "0.0.0.0", port: int = 8000) -> None:
		import uvicorn

		cfg = uvicorn.Config(self.build_app(), host=host, port=port, log_level="warning")
		await uvicorn.Server(cfg).serve()
