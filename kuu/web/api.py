from __future__ import annotations

import typing
import uuid
from datetime import datetime, timezone

from msgspec import to_builtins
from starlette.requests import Request
from starlette.responses import JSONResponse

from kuu.app import Kuu
from kuu.observability import (
	EnqueueCmd,
	InstanceRegistry,
	RemoveJobCmd,
	TaskInfo,
	TriggerJobCmd,
)
from kuu.orchestrator import PresetSupervisor
from kuu.scheduler import Scheduler
from kuu.web.stats import StatsCollector

if typing.TYPE_CHECKING:
	from kuu.orchestrator._control import ControlPlane
	from kuu.persistence._backend import PersistenceBackend


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
		instance = request.query_params.get("instance")
		if not task_name:
			return Err("task required")
		info = self._lookup_task(task_name, instance)
		if info is None:
			return Err("task not found", 404)
		params = [to_builtins(p) for p in info.params]
		return Ok({"params": params, "raw": info.has_varargs})

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

		instance = body.get("instance")
		info = self._lookup_task(task_name, instance)
		if info is None:
			return Err("task not found", 404)

		if self.control is not None and instance:
			cmd = EnqueueCmd(
				request_id=uuid.uuid4().hex,
				task=task_name,
				args=raw_args,
				kwargs=raw_kwargs,
			)
			return await _route(self.control, instance, cmd)

		if self.app is None:
			return Err("no instance specified", 400)
		try:
			from kuu.message import Payload

			await self.app.enqueue_by_name(
				task_name, Payload(args=tuple(raw_args), kwargs=raw_kwargs)
			)
			return Ok()
		except Exception as exc:
			return Err(str(exc))

	async def _api_trigger_job(self, request: Request) -> JSONResponse:
		try:
			body = await request.json()
		except Exception:
			return Err("invalid json")
		job_id = body.get("job_id")
		if not job_id:
			return Err("job_id required")

		instance = body.get("instance")
		if self.control is not None and instance:
			cmd = TriggerJobCmd(request_id=uuid.uuid4().hex, job_id=job_id)
			return await _route(self.control, instance, cmd)

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

		instance = body.get("instance")
		if self.control is not None and instance:
			cmd = RemoveJobCmd(request_id=uuid.uuid4().hex, job_id=job_id)
			return await _route(self.control, instance, cmd)

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
			return Err("persistence disabled", 503)

		qp = request.query_params
		task = qp.get("task")
		status = qp.get("status")
		before = qp.get("before")
		after = qp.get("after")
		limit = max(1, min(500, int(qp.get("limit", "100"))))
		offset = max(0, int(qp.get("offset", "0")))

		try:
			rows = await be.query_runs(
				task=task or None,
				status=status or None,
				before=datetime.fromtimestamp(float(before), tz=timezone.utc) if before else None,
				after=datetime.fromtimestamp(float(after), tz=timezone.utc) if after else None,
				limit=limit,
				offset=offset,
			)
		except Exception as exc:
			return Err(str(exc), 500)

		return Ok(
			{
				"rows": [_run_to_dict(r) for r in rows],
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
		except Exception as exc:
			return Err(str(exc), 500)

		return Ok(
			{
				"message_id": mid,
				"attempts": [_run_to_dict(r) for r in rows],
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
		except ValueError:
			return Err("attempt must be int")
		limit = max(1, min(2000, int(qp.get("limit", "500"))))
		after_ts = float(qp.get("after_ts", "0"))

		try:
			rows = await be.query_logs(
				message_id, attempt, limit=limit, after_dt=datetime.fromtimestamp(after_ts)
			)
		except Exception as exc:
			return Err(str(exc), 500)

		return Ok(
			{
				"message_id": message_id,
				"attempt": attempt,
				"logs": [_log_to_dict(r) for r in rows],
			}
		)

	def _lookup_task(self, task_name: str, instance: str | None) -> TaskInfo | None:
		if self.registry is not None:
			if instance:
				entry = self.registry.get(instance)
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


def _run_to_dict(r: typing.Any) -> dict:
	return {
		"id": r.id,
		"message_id": r.message_id,
		"attempt": r.attempt,
		"task": r.task,
		"queue": r.queue,
		"instance_id": r.instance_id,
		"worker_pid": r.worker_pid,
		"args": r.args,
		"kwargs": r.kwargs,
		"started_at": r.started_at,
		"finished_at": r.finished_at,
		"time_elapsed": r.time_elapsed,
		"status": r.status,
		"exc_type": r.exc_type,
		"exc_message": r.exc_message,
		"traceback": r.traceback,
	}


def _log_to_dict(r: typing.Any) -> dict:
	return {
		"message_id": r.message_id,
		"attempt": r.attempt,
		"ts": r.ts,
		"level": r.level,
		"logger": r.logger,
		"message": r.message,
	}


async def _route(control: "ControlPlane", instance: str, cmd: typing.Any) -> JSONResponse:
	try:
		resp = await control.send_command(instance, cmd)
	except KeyError:
		return Err("unknown instance", 404)
	if resp.ok:
		return Ok()
	return Err(resp.error or "command failed")


class Ok(JSONResponse):
	def __init__(self, content: typing.Any = None, **kwargs):
		super().__init__(content if content else {"ok": True}, **kwargs)


class Err(JSONResponse):
	def __init__(self, error: str, status_code: int = 400, **kwargs):
		content = {"error": error}
		super().__init__(content, status_code, **kwargs)
