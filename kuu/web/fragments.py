from jinja2 import Environment
from starlette.requests import Request
from starlette.responses import HTMLResponse

from kuu.app import Kuu
from kuu.brokers.redis import RedisBroker
from kuu.observability import InstanceRegistry
from kuu.orchestrator.main import PresetSupervisor
from kuu.scheduler.scheduler import Scheduler
from kuu.web.stats import StatsCollector


class DashboardFragmentsMixin:
	app: Kuu
	title: str = "kuu dashboard"
	scheduler: Scheduler | None = None
	orchestrator: PresetSupervisor | None = None
	registry: InstanceRegistry | None = None
	stats: StatsCollector
	jinja: Environment

	def _render(self, name: str, **ctx) -> str:
		return self.jinja.get_template(name).render(**ctx)

	async def _index(self, _: Request) -> HTMLResponse:
		broker_info = await self._broker_stats()
		return HTMLResponse(
			self._render(
				"index.html",
				title=self.title,
				orchestrator=self.orchestrator,
				broker_info=broker_info,
			)
		)

	async def _frag_stats(self, _: Request) -> HTMLResponse:
		broker_info = await self._broker_stats()
		return HTMLResponse(
			self._render(
				"fragments/stats.html",
				totals=self.stats.totals,
				broker_info=broker_info,
			)
		)

	async def _frag_tasks(self, _: Request) -> HTMLResponse:
		names = sorted(self.app.registry.names())
		tasks = [self.app.registry.get(n) for n in names]
		return HTMLResponse(self._render("fragments/tasks.html", tasks=tasks))

	async def _frag_scheduler(self, _: Request) -> HTMLResponse:
		jobs = self.scheduler.jobs if self.scheduler else []
		return HTMLResponse(self._render("fragments/scheduler.html", jobs=jobs))

	async def _frag_workers(self, _: Request) -> HTMLResponse:
		processes = (
			self.orchestrator._wp._processes if self.orchestrator and self.orchestrator._wp else []
		)
		return HTMLResponse(self._render("fragments/workers.html", processes=processes))

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
