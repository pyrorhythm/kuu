from __future__ import annotations

import inspect
import json
import typing
from pathlib import Path
from typing import TYPE_CHECKING, Any

from jinja2 import Environment, FileSystemLoader, select_autoescape
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles

from kuu.brokers.redis import RedisBroker
from kuu.message import Payload
from kuu.web.stats import StatsCollector

if TYPE_CHECKING:
	from kuu.app import Kuu
	from kuu.orchestrator.main import Orchestrator
	from kuu.scheduler.scheduler import Scheduler

_SIMPLE_TYPES = (bool, int, float, str)


def _sig_params(fn: Any) -> tuple[list[dict], bool]:
	"""Returns (params_list, has_varargs) for a callable."""
	sig = inspect.signature(fn)
	try:
		hints = typing.get_type_hints(fn)
	except Exception:
		hints = {}
	params: list[dict] = []
	has_varargs = False
	for name, param in sig.parameters.items():
		if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
			has_varargs = True
			continue
		ann = hints.get(name, param.annotation)
		if ann is inspect.Parameter.empty:
			ann_str = None
		elif isinstance(ann, type):
			ann_str = ann.__name__
		else:
			ann_str = str(ann).replace("typing.", "")
		required = param.default is inspect.Parameter.empty
		default: Any = None
		if not required:
			try:
				json.dumps(param.default)
				default = param.default
			except (TypeError, ValueError):
				default = repr(param.default)
		params.append({
			"name": name,
			"annotation": ann_str,
			"default": default,
			"required": required,
		})
	return params, has_varargs


def _validate_payload(fn: Any, args: list, kwargs: dict) -> str | None:
	"""Returns an error message or None if valid."""
	sig = inspect.signature(fn)
	try:
		hints = typing.get_type_hints(fn)
	except Exception:
		hints = {}
	try:
		bound = sig.bind(*args, **kwargs)
	except TypeError as exc:
		return str(exc)
	bound.apply_defaults()
	for name, value in bound.arguments.items():
		ann = hints.get(name)
		if ann is None:
			continue
		if getattr(ann, "__origin__", None) is not None:
			continue
		if not isinstance(ann, type) or ann not in _SIMPLE_TYPES:
			continue
		# bool is a subclass of int — check it before int
		if ann is int and isinstance(value, bool):
			return f"parameter {name!r}: expected int, got bool"
		if not isinstance(value, ann):
			return f"parameter {name!r}: expected {ann.__name__}, got {type(value).__name__}"
	return None


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
		self.jinja.filters["tojson"] = json.dumps

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

	async def _api_task_params(self, request: Request) -> JSONResponse:
		task_name = request.query_params.get("task")
		if not task_name:
			return JSONResponse({"error": "task required"}, status_code=400)
		task = self.app.registry.get(task_name)
		if not task:
			return JSONResponse({"error": "task not found"}, status_code=404)
		params, has_varargs = _sig_params(task.original_func)
		return JSONResponse({"params": params, "raw": has_varargs})

	async def _api_run_task(self, request: Request) -> JSONResponse:
		try:
			body = await request.json()
		except Exception:
			return JSONResponse({"error": "invalid json"}, status_code=400)
		task_name = body.get("task")
		if not task_name:
			return JSONResponse({"error": "task required"}, status_code=400)
		raw_args = body.get("args", [])
		raw_kwargs = body.get("kwargs", {})
		if not isinstance(raw_args, list) or not isinstance(raw_kwargs, dict):
			return JSONResponse(
				{"error": "args must be array, kwargs must be object"}, status_code=400
			)
		task = self.app.registry.get(task_name)
		if not task:
			return JSONResponse({"error": "task not found"}, status_code=404)
		err = _validate_payload(task.original_func, raw_args, raw_kwargs)
		if err:
			return JSONResponse({"error": err}, status_code=422)
		try:
			await self.app.enqueue_by_name(
				task_name, Payload(args=tuple(raw_args), kwargs=raw_kwargs)
			)
			return JSONResponse({"ok": True})
		except Exception as exc:
			return JSONResponse({"error": str(exc)}, status_code=400)

	async def _api_trigger_job(self, request: Request) -> JSONResponse:
		if not self.scheduler:
			return JSONResponse({"error": "no scheduler"}, status_code=400)
		try:
			body = await request.json()
		except Exception:
			return JSONResponse({"error": "invalid json"}, status_code=400)
		job_id = body.get("job_id")
		job = next((j for j in self.scheduler.jobs if j.id == job_id), None)
		if not job:
			return JSONResponse({"error": "job not found"}, status_code=404)
		try:
			await self.app.enqueue_by_name(
				job.task_name,
				job.args,
				queue=job.queue,
				headers=job.headers,
				max_attempts=job.max_attempts,
			)
			return JSONResponse({"ok": True})
		except Exception as exc:
			return JSONResponse({"error": str(exc)}, status_code=400)

	async def _api_remove_job(self, request: Request) -> JSONResponse:
		if not self.scheduler:
			return JSONResponse({"error": "no scheduler"}, status_code=400)
		try:
			body = await request.json()
		except Exception:
			return JSONResponse({"error": "invalid json"}, status_code=400)
		job_id = body.get("job_id")
		before = len(self.scheduler.jobs)
		self.scheduler.jobs = [j for j in self.scheduler.jobs if j.id != job_id]
		if len(self.scheduler.jobs) == before:
			return JSONResponse({"error": "job not found"}, status_code=404)
		return JSONResponse({"ok": True})

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
