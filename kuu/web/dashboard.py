from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from jinja2 import Environment, FileSystemLoader, select_autoescape
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles

from kuu.brokers.redis import RedisBroker
from kuu.web.stats import StatsCollector

if TYPE_CHECKING:
	from kuu.app import Kuu
	from kuu.orchestrator.main import Orchestrator
	from kuu.scheduler.scheduler import Scheduler


class Dashboard:
	def __init__(
		self,
		app: Kuu,
		scheduler: Scheduler | None = None,
		orchestrator: Orchestrator | None = None,
		title: str = "kuu dashboard",
	) -> None:
		self.app = app
		self.scheduler = scheduler
		self.orchestrator = orchestrator
		self.title = title
		self.stats = StatsCollector(app)
		here = Path(__file__).parent
		self.jinja = Environment(
			loader=FileSystemLoader(str(here / "templates")),
			autoescape=select_autoescape(["html", "xml"]),
		)

	def _render(self, name: str, **ctx) -> str:
		return self.jinja.get_template(name).render(**ctx)

	async def _index(self, request: Request) -> HTMLResponse:
		broker_info = await self._broker_stats()
		return HTMLResponse(
			self._render(
				"index.html",
				title=self.title,
				orchestrator=self.orchestrator,
				broker_info=broker_info,
			)
		)

	async def _frag_stats(self, request: Request) -> HTMLResponse:
		broker_info = await self._broker_stats()
		return HTMLResponse(
			self._render(
				"fragments/stats.html",
				totals=self.stats.totals,
				broker_info=broker_info,
			)
		)

	async def _frag_tasks(self, request: Request) -> HTMLResponse:
		names = sorted(self.app.registry.names())
		tasks = [self.app.registry.get(n) for n in names]
		return HTMLResponse(self._render("fragments/tasks.html", tasks=tasks))

	async def _frag_scheduler(self, request: Request) -> HTMLResponse:
		jobs = self.scheduler.jobs if self.scheduler else []
		return HTMLResponse(self._render("fragments/scheduler.html", jobs=jobs))

	async def _frag_workers(self, request: Request) -> HTMLResponse:
		processes = (
			self.orchestrator._wp._processes if self.orchestrator and self.orchestrator._wp else []
		)
		return HTMLResponse(self._render("fragments/workers.html", processes=processes))

	async def _api_activity(self, request: Request) -> JSONResponse:
		return JSONResponse(self.stats.activity_series())

	async def _broker_stats(self) -> dict:
		broker = self.app.broker
		out: dict = {}

		if isinstance(broker, RedisBroker):
			try:
				await broker.connect()
				queues = self.app.registry.queues() or {self.app.default_queue}
				depths: dict = {}
				for q in queues:
					s = await broker.r.xlen(broker._stream(q))
					z = await broker.r.zcard(broker._zset(q))
					depths[q] = {"stream": s, "scheduled": z}
				out["queues"] = depths
			except Exception:
				pass
		elif hasattr(broker, "_scheduled"):
			out["scheduled"] = len(broker._scheduled)
		if hasattr(broker, "_pending"):
			out["pending"] = len(broker._pending)
		return out

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
