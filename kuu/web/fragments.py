from __future__ import annotations

import typing
from datetime import timedelta

from jinja2 import Environment
from starlette.requests import Request
from starlette.responses import HTMLResponse

from kuu._util import utcnow
from kuu.app import Kuu
from kuu.observability import InstanceRegistry
from kuu.orchestrator.main import PresetSupervisor
from kuu.scheduler.scheduler import Scheduler
from kuu.web.stats import StatsCollector

if typing.TYPE_CHECKING:
	from kuu.persistence import PersistenceBackend


class DashboardFragmentsMixin:
	app: Kuu | None = None
	title: str = "kuu dashboard"
	scheduler: Scheduler | None = None
	orchestrator: PresetSupervisor | None = None
	registry: InstanceRegistry | None = None
	stats: StatsCollector
	jinja: Environment
	persistence_backend: "PersistenceBackend | None" = None
	trace_url_template: str | None = None

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
		since = utcnow() - timedelta(hours=24)
		try:
			summary = (
				await self.persistence_backend.query_dashboard_stats(since)
				if self.persistence_backend is not None
				else self.stats.dashboard_stats(since)
			)
		except Exception:
			summary = self.stats.dashboard_stats(since)
		return HTMLResponse(
			self._render(
				"fragments/stats.html",
				totals=summary.totals,
				queues=self._summary_queues(broker_info),
			)
		)

	async def _frag_tasks(self, _: Request) -> HTMLResponse:
		groups = self._tasks_by_broker()
		return HTMLResponse(self._render("fragments/tasks.html", groups=groups))

	async def _frag_presets(self, _: Request) -> HTMLResponse:
		rows = self._presets_rows()
		return HTMLResponse(self._render("fragments/presets.html", presets=rows))

	async def _frag_queues(self, _: Request) -> HTMLResponse:
		rows = self._queues_rows()
		return HTMLResponse(self._render("fragments/queues.html", queues=rows))

	def _tasks_by_broker(self) -> list[dict]:
		"""group tasks by unique broker key - presets sharing a broker collapse
		into one group, since they serve the same task registry"""
		if self.registry is not None:
			groups: dict[str, dict] = {}
			for entry in self.registry.all():
				broker = entry.hello.broker
				broker_key = broker.key or f"{broker.type}:?"
				preset = entry.hello.preset
				key = f"{broker_key}:{preset}"
				g = groups.setdefault(
					key,
					{
						"broker_type": broker.type,
						"broker_key": broker_key,
						"broker_key_short": (broker_key[:12] + "…")
						if len(broker_key) > 13
						else broker_key,
						"presets": [preset],
						"target": preset,
						"tasks": [],
						"_seen_task_names": set(),
					},
				)
				for t in entry.hello.tasks:
					if t.name in g["_seen_task_names"]:
						continue
					g["_seen_task_names"].add(t.name)
					g["tasks"].append(t)
			out = []
			for g in groups.values():
				g.pop("_seen_task_names", None)
				g["presets"].sort()
				g["tasks"].sort(key=lambda t: t.name)
				out.append(g)
			return sorted(out, key=lambda g: g["broker_type"] + g["broker_key"])
		if self.app is not None:
			tasks = []
			for name in sorted(self.app.registry.names()):
				t = self.app.registry.get(name)
				if t is not None:
					tasks.append(
						{
							"name": t.task_name,
							"queue": t.task_queue,
							"max_attempts": t.max_attempts,
							"timeout": t.timeout,
						}
					)
			if not tasks:
				return []
			return [
				{
					"broker_type": type(self.app.broker).__name__,
					"broker_key": "",
					"broker_key_short": "",
					"presets": ["default"],
					"target": "",
					"tasks": tasks,
				}
			]
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
					{
						"name": qname,
						"in_flight": 0,
						"depth": None,
						"pending": None,
						"scheduled": None,
						"dead": None,
						"instances": 0,
					},
				)
				row["in_flight"] += qs.in_flight
				row["instances"] += 1
				if qs.depth is not None:
					row["depth"] = qs.depth
				for name in ("pending", "scheduled", "dead"):
					if (value := getattr(qs, name)) is not None:
						row[name] = value
		return sorted(agg.values(), key=lambda r: r["name"])

	def _summary_queues(self, broker_info: dict) -> list[dict]:
		if queues := broker_info.get("queues"):
			return [
				{
					"name": name,
					"pending": info.get("stream", info.get("total")),
					"scheduled": info.get("scheduled"),
					"dead": info.get("dead"),
					"in_flight": None,
				}
				for name, info in sorted(queues.items())
			]
		return self._queues_rows()

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
							"target": entry.hello.preset,
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
		tab = request.query_params.get("tab", "")
		return HTMLResponse(
			self._render(
				"fragments/task_run_detail.html",
				message_id=mid,
				tab=tab,
				trace_url_template=self.trace_url_template,
			)
		)

	# ── broker ──────────────────────────────────────────────────────

	async def _broker_stats(self) -> dict:
		if self.app is None:
			return {}
		broker = self.app.broker
		out: dict = {}

		try:
			await broker.connect()
			queues = self.app.registry.queues() or {self.app.default_queue}
			depths: dict = {}
			for q in queues:
				breakdown = await broker.queue_breakdown(q)
				if breakdown is not None:
					depths[q] = breakdown
				else:
					depth = await broker.queue_depth(q)
					if depth is not None:
						depths[q] = {"total": depth}
			if depths:
				out["queues"] = depths
		except Exception:
			pass

		if hasattr(broker, "scheduled_count"):
			out["scheduled"] = broker.scheduled_count
		if hasattr(broker, "pending_count"):
			out["pending"] = broker.pending_count

		return out
