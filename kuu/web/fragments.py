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

	async def _frag_workers(self, _: Request) -> HTMLResponse:
		rows = self._workers_rows()
		return HTMLResponse(self._render("fragments/workers.html", processes=rows))

	async def _frag_queues(self, _: Request) -> HTMLResponse:
		rows = self._queues_rows()
		return HTMLResponse(self._render("fragments/queues.html", queues=rows))

	def _queues_rows(self) -> list[dict]:
		if self.registry is None:
			return []
		agg: dict[str, dict] = {}
		for entry in self.registry.all():
			if entry.last_state is None:
				continue
			for qname, qs in entry.last_state.queues.items():
				row = agg.setdefault(
					qname,
					{"name": qname, "in_flight": 0, "depth": None, "instances": 0},
				)
				row["in_flight"] += qs.in_flight
				row["instances"] += 1
				if qs.depth is not None:
					row["depth"] = qs.depth
		return sorted(agg.values(), key=lambda r: r["name"])

	def _workers_rows(self) -> list[dict]:
		if self.registry is not None:
			rows: list[dict] = []
			for entry in self.registry.all():
				preset = entry.hello.preset
				if entry.last_state is None:
					continue
				for w in entry.last_state.workers:
					rows.append(
						{
							"pid": w.pid,
							"alive": w.alive,
							"preset": preset,
							"instance": entry.instance_id[:8],
						}
					)
			return rows
		if self.orchestrator and self.orchestrator._wp:
			return [
				{"pid": p.pid, "alive": p.is_alive(), "preset": None, "instance": None}
				for p in self.orchestrator._wp._processes
			]
		return []

	async def _frag_scheduler(self, _: Request) -> HTMLResponse:
		if self.registry is not None:
			jobs: list = []
			for entry in self.registry.all():
				if entry.last_state is None:
					continue
				for j in entry.last_state.jobs:
					jobs.append(
						{
							"id": j.id,
							"task": j.task,
							"next_run": j.next_run,
							"instance": entry.instance_id,
						}
					)
			return HTMLResponse(
				self._render("fragments/scheduler.html", jobs=jobs, aggregated=True)
			)
		jobs = self.scheduler.jobs if self.scheduler else []
		return HTMLResponse(self._render("fragments/scheduler.html", jobs=jobs, aggregated=False))

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
