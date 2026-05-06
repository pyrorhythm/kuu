from __future__ import annotations

import typing

from jinja2 import Environment
from starlette.requests import Request
from starlette.responses import HTMLResponse

from kuu.app import Kuu
from kuu.brokers.redis import RedisBroker
from kuu.observability import InstanceRegistry
from kuu.orchestrator.main import PresetSupervisor
from kuu.scheduler.scheduler import Scheduler
from kuu.web.stats import StatsCollector

if typing.TYPE_CHECKING:
	from kuu.persistence._backend import PersistenceBackend


class DashboardFragmentsMixin:
	app: Kuu | None = None
	title: str = "kuu dashboard"
	scheduler: Scheduler | None = None
	orchestrator: PresetSupervisor | None = None
	registry: InstanceRegistry | None = None
	stats: StatsCollector
	jinja: Environment
	persistence_backend: "PersistenceBackend | None" = None

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
		groups = self._tasks_by_preset()
		return HTMLResponse(self._render("fragments/tasks.html", groups=groups))

	async def _frag_presets(self, _: Request) -> HTMLResponse:
		rows = self._presets_rows()
		return HTMLResponse(self._render("fragments/presets.html", presets=rows))

	async def _frag_queues(self, _: Request) -> HTMLResponse:
		rows = self._queues_rows()
		return HTMLResponse(self._render("fragments/queues.html", queues=rows))

	def _tasks_by_preset(self) -> list[dict]:
		if self.registry is not None:
			groups: dict[str, dict] = {}
			for entry in self.registry.all():
				preset = entry.hello.preset
				g = groups.setdefault(
					preset,
					{"preset": preset, "instance": entry.instance_id, "tasks": []},
				)
				if g["tasks"]:
					continue
				g["instance"] = entry.instance_id
				g["tasks"] = list(entry.hello.tasks)
			return sorted(groups.values(), key=lambda g: g["preset"])
		if self.app is not None:
			tasks = []
			for name in sorted(self.app.registry.names()):
				t = self.app.registry.get(name)
				if t is not None:
					tasks.append(t)
			return [{"preset": "default", "instance": None, "tasks": tasks}] if tasks else []
		return []

	def _presets_rows(self) -> list[dict]:
		if self.registry is None:
			return []
		groups: dict[str, dict] = {}
		for entry in self.registry.all():
			preset = entry.hello.preset
			row = groups.setdefault(
				preset,
				{
					"preset": preset,
					"instances": 0,
					"workers_alive": 0,
					"workers_total": 0,
					"capacity": 0,
					"in_flight": 0,
				},
			)
			row["instances"] += 1
			row["capacity"] += entry.hello.processes * entry.hello.concurrency
			if entry.last_state is not None:
				row["workers_total"] += len(entry.last_state.workers)
				row["workers_alive"] += sum(1 for w in entry.last_state.workers if w.alive)
				row["in_flight"] += sum(qs.in_flight for qs in entry.last_state.queues.values())
		for row in groups.values():
			cap = row["capacity"] or row["workers_total"] or 1
			row["load"] = min(1.0, row["in_flight"] / cap) if cap else 0.0
			row["load_pct"] = int(round(row["load"] * 100))
		return sorted(groups.values(), key=lambda r: r["preset"])

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

	# ── task-run fragments ─────────────────────────────────────────

	async def _frag_task_runs(self, _: Request) -> HTMLResponse:
		return HTMLResponse(self._render("fragments/task_runs.html"))

	async def _frag_task_run_detail(self, request: Request) -> HTMLResponse:
		mid = request.query_params.get("message_id", "")
		return HTMLResponse(
			self._render("fragments/task_run_detail.html", message_id=mid)
		)

	# ── broker ──────────────────────────────────────────────────────

	async def _broker_stats(self) -> dict:
		if self.app is None:
			return {}
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
