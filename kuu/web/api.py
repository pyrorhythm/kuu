from __future__ import annotations

import inspect
import typing
import uuid
from datetime import datetime, timezone

from msgspec import to_builtins
from starlette.requests import Request
from starlette.responses import JSONResponse

from kuu._util import utcnow
from kuu.app import Kuu
from kuu.observability import (
	CancelCmd,
	EnqueueCmd,
	InstanceRegistry,
	RemoveJobCmd,
	ReplayCmd,
	RetryCmd,
	TaskInfo,
	TriggerJobCmd,
)
from kuu.orchestrator import PresetSupervisor
from kuu.scheduler import Scheduler
from kuu.web.stats import StatsCollector

if typing.TYPE_CHECKING:
	from kuu.orchestrator._control import ControlPlane
	from kuu.persistence import PersistenceBackend


class DashbordAPIMixin:
	app: Kuu | None = None
	scheduler: Scheduler | None = None
	orchestrator: PresetSupervisor | None = None
	control: "ControlPlane | None" = None
	registry: InstanceRegistry | None = None
	stats: StatsCollector
	persistence_backend: "PersistenceBackend | None" = None

	async def _api_activity(self, request: Request) -> JSONResponse:
		return JSONResponse(self.stats.activity_series())

	async def _api_task_params(self, request: Request) -> JSONResponse:
		task_name = request.query_params.get("task")
		target = request.query_params.get("preset") or request.query_params.get("instance")
		if not task_name:
			return Err("task required")
		info = self._lookup_task(task_name, target)
		if info is None:
			return Err("task not found", 404)
		params = [to_builtins(p) for p in info.params]
		return Ok(
			{
				"params": params,
				"raw": info.has_varargs,
				"queue": info.queue,
				"max_attempts": info.max_attempts,
				"timeout": info.timeout,
			}
		)

	async def _api_run_task(self, request: Request) -> JSONResponse:
		try:
			body = await request.json()
		except Exception:
			return Err("invalid json")
		task_name = body.get("task")
		if not task_name:
			return Err("task required")
		raw_args = body.get("args")
		raw_kwargs = body.get("kwargs")
		if not isinstance(raw_args, list) or not isinstance(raw_kwargs, dict):
			return Err("args must be array, kwargs must be object")

		target = body.get("preset") or body.get("instance")
		info = self._lookup_task(task_name, target)
		if info is None:
			return Err("task not found", 404)

		if self.control is not None and self.registry is not None:
			if not target:
				target = self._target_for_task(task_name)
				if target is None:
					return Err("preset required", 409)
			cmd = EnqueueCmd(
				request_id=uuid.uuid4().hex,
				task=task_name,
				args=raw_args,
				kwargs=raw_kwargs,
			)
			return await _route(self.control, target, cmd)

		if self.app is None:
			return Err("no active application", 400)
		try:
			from kuu.message import Payload

			definition = self.app.registry.get(task_name)
			if definition is None:
				return Err("task not found", 404)
			inspect.signature(definition.original_func).bind(*raw_args, **raw_kwargs)
			handle = await self.app.enqueue_by_name(
				task_name, Payload(args=tuple(raw_args), kwargs=raw_kwargs)
			)
			return Ok({"ok": True, "run_id": str(handle.message.id)})
		except Exception as exc:
			return Err(str(exc))

	async def _api_cancel_run(self, request: Request) -> JSONResponse:
		try:
			body = await request.json()
		except Exception:
			return Err("invalid json")
		run_id = body.get("run_id")
		if not run_id:
			return Err("run_id required")
		target = body.get("preset") or await self._target_for_run(run_id)
		if self.control is None or target is None:
			return Err("no active Preset for Run", 409)
		response = await _route(
			self.control,
			target,
			CancelCmd(request_id=body.get("request_id") or uuid.uuid4().hex, run_id=run_id),
		)
		if response.status_code < 300:
			if self.persistence_backend is not None:
				await self.persistence_backend.mark_cancel_requested(run_id, utcnow())
			return Ok({"ok": True, "run_id": run_id, "status": "cancel_requested"})
		return response

	async def _api_retry_run(self, request: Request) -> JSONResponse:
		try:
			body = await request.json()
		except Exception:
			return Err("invalid json")
		run_id = body.get("run_id")
		if not run_id:
			return Err("run_id required")
		if self.persistence_backend is None:
			return Err("Retry requires persistence", 409)
		logical = await self.persistence_backend.get_logical_run(run_id)
		if logical is None:
			return Err("Run not found", 404)
		if logical.status != "unknown":
			return Err("Retry is only available for an Unknown active Run", 409)
		rows = await self.persistence_backend.query_run_attempts(run_id)
		source = next(
			(
				row
				for row in rows
				if row.input_complete and row.args is not None and row.kwargs is not None
			),
			None,
		)
		if source is None:
			return Err("Retry unavailable: complete input was not retained", 409)
		if not isinstance(source.args, (list, tuple)) or not isinstance(source.kwargs, dict):
			return Err("Retry unavailable: retained input is invalid", 409)
		target = body.get("preset") or self._target_for_task(source.task)
		if self.control is None or target is None:
			return Err("no active Preset for task", 409)
		return await _route(
			self.control,
			target,
			RetryCmd(
				request_id=body.get("request_id") or uuid.uuid4().hex,
				run_id=run_id,
				attempt=max((row.attempt for row in rows), default=-1) + 1,
				task=source.task,
				args=list(source.args),
				kwargs=source.kwargs,
				queue=source.queue,
			),
		)

	async def _api_replay_run(self, request: Request) -> JSONResponse:
		try:
			body = await request.json()
		except Exception:
			return Err("invalid json")
		run_id = body.get("run_id")
		if not run_id:
			return Err("run_id required")
		if self.persistence_backend is None:
			return Err("Replay requires persistence", 409)
		logical = await self.persistence_backend.get_logical_run(run_id)
		if logical is None:
			return Err("Run not found", 404)
		if logical.status not in {"succeeded", "failed", "cancelled"}:
			return Err("Replay is only available for a terminal Run", 409)
		rows = await self.persistence_backend.query_run_attempts(run_id)
		source = next(
			(
				row
				for row in rows
				if row.input_complete and row.args is not None and row.kwargs is not None
			),
			None,
		)
		if source is None:
			return Err("Replay unavailable: complete input was not retained", 409)
		args = source.args
		kwargs = source.kwargs
		if not isinstance(args, (list, tuple)) or not isinstance(kwargs, dict):
			return Err("Replay unavailable: retained input is invalid", 409)
		target = body.get("preset") or self._target_for_task(source.task)
		if self.control is None or target is None:
			return Err("no active Preset for task", 409)
		return await _route(
			self.control,
			target,
			ReplayCmd(
				request_id=body.get("request_id") or uuid.uuid4().hex,
				replay_of=run_id,
				task=source.task,
				args=list(args),
				kwargs=kwargs,
				queue=source.queue,
			),
		)

	async def _api_trigger_job(self, request: Request) -> JSONResponse:
		try:
			body = await request.json()
		except Exception:
			return Err("invalid json")
		job_id = body.get("job_id")
		if not job_id:
			return Err("job_id required")

		target = body.get("preset") or body.get("instance") or self._target_for_job(job_id)
		if self.control is not None and self.registry is not None:
			if target is None:
				return Err("preset required", 409)
			cmd = TriggerJobCmd(request_id=uuid.uuid4().hex, job_id=job_id)
			return await _route(self.control, target, cmd)

		if not self.scheduler or self.app is None:
			return Err("no scheduler")
		job = next((j for j in self.scheduler.jobs if j.id == job_id), None)
		if not job:
			return Err("job not found", 404)
		try:
			await self.app.enqueue_by_name(
				job.task_name,
				job.args,
				queue=job.queue,
				headers=job.headers,
				max_attempts=job.max_attempts,
			)
			return Ok()
		except Exception as exc:
			return Err(str(exc))

	async def _api_remove_job(self, request: Request) -> JSONResponse:
		try:
			body = await request.json()
		except Exception:
			return Err("invalid json")
		job_id = body.get("job_id")
		if not job_id:
			return Err("job_id required")

		target = body.get("preset") or body.get("instance") or self._target_for_job(job_id)
		if self.control is not None and self.registry is not None:
			if target is None:
				return Err("preset required", 409)
			cmd = RemoveJobCmd(request_id=uuid.uuid4().hex, job_id=job_id)
			return await _route(self.control, target, cmd)

		if not self.scheduler:
			return Err("no scheduler")
		before = len(self.scheduler.jobs)
		self.scheduler.jobs = [j for j in self.scheduler.jobs if j.id != job_id]
		if len(self.scheduler.jobs) == before:
			return Err("job not found", 404)
		return Ok()

	async def _api_task_runs(self, request: Request) -> JSONResponse:
		be = self.persistence_backend
		if be is None:
			return Ok({"runs": [], "rows": [], "limit": 0, "offset": 0, "live_only": True})

		qp = request.query_params
		task = qp.get("task")
		status = qp.get("status")
		before = qp.get("before")
		after = qp.get("after")
		limit = max(1, min(500, int(qp.get("limit", "100"))))
		offset = max(0, int(qp.get("offset", "0")))

		try:
			runs = await be.query_logical_runs(
				task=task or None,
				status=status or None,
				before=datetime.fromtimestamp(float(before), tz=timezone.utc) if before else None,
				after=datetime.fromtimestamp(float(after), tz=timezone.utc) if after else None,
				limit=limit,
				offset=offset,
			)
			attempts = await be.query_attempts_for_runs([run.message_id for run in runs])
		except Exception as exc:
			return Err(str(exc), 500)

		by_run: dict[str, list] = {run.message_id: [] for run in runs}
		for attempt in attempts:
			by_run.setdefault(attempt.message_id, []).append(attempt.asdict())
		return Ok(
			{
				"runs": [
					{**run.asdict(), "attempts": by_run.get(run.message_id, [])} for run in runs
				],
				"rows": [attempt.asdict() for attempt in attempts],
				"limit": limit,
				"offset": offset,
			}
		)

	async def _api_task_run_attempts(self, request: Request) -> JSONResponse:
		"""all attempts for a given message_id"""
		be = self.persistence_backend
		if be is None:
			return Err("persistence disabled", 503)

		mid = request.query_params.get("message_id")
		if not mid:
			return Err("message_id required")

		try:
			rows = await be.query_run_attempts(mid)
			logical = await be.get_logical_run(mid)
		except Exception as exc:
			return Err(str(exc), 500)

		return Ok(
			{
				"message_id": mid,
				"run": logical.asdict() if logical is not None else None,
				"attempts": [r.asdict() for r in rows],
			}
		)

	async def _api_task_run_logs(self, request: Request) -> JSONResponse:
		be = self.persistence_backend
		if be is None:
			return Err("persistence disabled", 503)

		qp = request.query_params
		message_id = qp.get("message_id")
		if not message_id:
			return Err("message_id required")
		try:
			attempt = int(qp.get("attempt", "0"))
			limit = max(1, min(2000, int(qp.get("limit", "500"))))
			after_id = int(qp.get("after_id", "0"))
		except ValueError:
			return Err("attempt, limit, and after_id must be integers")

		try:
			rows = await be.query_logs(message_id, attempt, limit=limit, after_id=after_id or None)
		except Exception as exc:
			return Err(str(exc), 500)

		return Ok(
			{
				"message_id": message_id,
				"attempt": attempt,
				"logs": [r.asdict() for r in rows],
				"cursor": rows[-1].id if rows else after_id,
			}
		)

	def _lookup_task(self, task_name: str, target: str | None) -> TaskInfo | None:
		if self.registry is not None:
			if target:
				entry = self.registry.get(target)
				if entry is None:
					entries = [e for e in self.registry.all() if e.hello.preset == target]
					entry = max(entries, key=lambda e: e.last_seen) if entries else None
				if entry is None:
					return None
				return next((t for t in entry.hello.tasks if t.name == task_name), None)
			for entry in self.registry.all():
				for t in entry.hello.tasks:
					if t.name == task_name:
						return t
			return None
		if self.app is None:
			return None
		task = self.app.registry.get(task_name)
		if task is None:
			return None
		from kuu._sig import sig_params
		from kuu.observability import TaskParam

		raw_params, has_varargs = sig_params(task.original_func)
		params = [
			TaskParam(
				name=p["name"],
				annotation=p["annotation"],
				default=p["default"],
				required=p["required"],
			)
			for p in raw_params
		]
		return TaskInfo(
			name=task.task_name,
			queue=task.task_queue,
			max_attempts=task.max_attempts,
			timeout=task.timeout,
			params=params,
			has_varargs=has_varargs,
		)

	def _target_for_task(self, task_name: str) -> str | None:
		if self.registry is None:
			return None
		presets = {
			entry.hello.preset
			for entry in self.registry.all()
			if any(task.name == task_name for task in entry.hello.tasks)
		}
		return next(iter(presets)) if len(presets) == 1 else None

	def _target_for_job(self, job_id: str) -> str | None:
		if self.registry is None:
			return None
		presets = {
			entry.hello.preset
			for entry in self.registry.all()
			if entry.last_state is not None
			and any(job.id == job_id for job in entry.last_state.jobs)
		}
		return next(iter(presets)) if len(presets) == 1 else None

	async def _target_for_run(self, run_id: str) -> str | None:
		if self.persistence_backend is None:
			return None
		rows = await self.persistence_backend.query_run_attempts(run_id)
		return self._target_for_task(rows[0].task) if rows else None


async def _route(control: "ControlPlane", instance: str, cmd: typing.Any) -> JSONResponse:
	try:
		resp = await control.send_command(instance, cmd)
	except KeyError:
		return Err("unknown Preset", 404)
	if resp.ok:
		return Ok({"ok": True, "run_id": resp.run_id})
	return Err(resp.error or "command failed")


class Ok(JSONResponse):
	def __init__(self, content: typing.Any = None, **kwargs):
		super().__init__(content if content else {"ok": True}, **kwargs)


class Err(JSONResponse):
	def __init__(self, error: str, status_code: int = 400, **kwargs):
		super().__init__({"error": error}, status_code, **kwargs)
