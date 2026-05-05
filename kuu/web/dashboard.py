from __future__ import annotations

import typing
from pathlib import Path

import orjson
from jinja2 import Environment, FileSystemLoader, select_autoescape
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
			app: Kuu,
			scheduler: Scheduler | None = None,
			orchestrator: PresetSupervisor | None = None,
			registry: InstanceRegistry | None = None,
			control: "ControlPlane | None" = None,
			title: str = "kuu dashboard",
	) -> None:
		"""
		``orchestrator`` is the legacy single-process supervisor whose
		``_wp._processes`` provides the workers fragment. ``registry``
		is the control-plane roster; when present, fragments prefer it
		and aggregate workers across all live instances. ``control``
		is the control-plane handle used to route per-instance commands
		(enqueue / trigger / remove); when set, run-task and scheduler
		actions are dispatched via RPC to the target supervisor.
		"""
		self.app = app
		self.scheduler = scheduler
		self.orchestrator = orchestrator
		self.registry = registry
		self.control = control
		self.title = title
		self.stats = StatsCollector(app, connect_app_events=registry is None)
		here = Path(__file__).parent
		self.jinja = Environment(
				loader=FileSystemLoader(str(here / "templates")),
				autoescape=select_autoescape(["html", "xml"]),
		)
		self.jinja.filters["tojson"] = lambda v: orjson.dumps(v).decode()

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
					WebSocketRoute("/_ingest", self._ws_ingest),
					Mount("/static", StaticFiles(directory=str(static_dir)), name="static"),
				],
		)

	def ingest_envelope(self, env: Envelope) -> None:
		"""feed a remote envelope into the dashboard's registry + stats

		called by the ``/_ingest`` ws endpoint and may be called directly
		by an in-process source for testing
		"""
		if self.registry is not None:
			self.registry.ingest(env)
		match env.body:
			case Event() as e:
				print(f"ingest {e=}")
				self.stats.ingest(e.kind, e.task, env.ts)
			case Hello() | State() | Bye():
				pass
			case _:
				pass

	async def _ws_ingest(self, websocket: WebSocket) -> None:
		print("harro everynyan")
		await websocket.accept()
		print("accepted")
		try:
			while True:
				print(f"WEBSOCKET try to recv state={websocket.client_state}")
				data = await websocket.receive_bytes()
				print(f"WEBSOCKET recv {data=}")
				try:
					env = envelope_from_bytes(data)
					print(f"WEBSOCKET envelope {env=}")
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

	def serve(self, host: str = "0.0.0.0", port: int = 8000) -> None:
		import uvicorn

		uvicorn.run(self.build_app(), host=host, port=port, log_level="warning")

	async def start_server(self, host: str = "0.0.0.0", port: int = 8000) -> None:
		import uvicorn

		cfg = uvicorn.Config(self.build_app(), host=host, port=port, log_level="warning")
		await uvicorn.Server(cfg).serve()
