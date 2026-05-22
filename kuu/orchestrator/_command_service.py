from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from kuu.observability import (
	Cmd,
	CmdResponse,
	EnqueueCmd,
	RemoveJobCmd,
	TriggerJobCmd,
)

if TYPE_CHECKING:
	from kuu.orchestrator._app_loader import AppLoader

log = logging.getLogger("kuu.orchestrator.commands")


class CommandService:
	def __init__(self, app_loader: AppLoader) -> None:
		self._app_loader = app_loader

	async def dispatch(self, cmd: Cmd) -> CmdResponse:
		match cmd:
			case EnqueueCmd(request_id=rid, task=task, args=args, kwargs=kwargs):
				return await self._enqueue(rid, task, args, kwargs)
			case TriggerJobCmd(request_id=rid, job_id=job_id):
				return await self._trigger_job(rid, job_id)
			case RemoveJobCmd(request_id=rid, job_id=job_id):
				return self._remove_job(rid, job_id)
			case _:
				return CmdResponse(
					request_id="?",
					ok=False,
					error=f"unknown command: {type(cmd).__name__}",
				)

	async def _enqueue(self, rid: str, task: str, args: list, kwargs: dict) -> CmdResponse:
		from kuu.message import Payload

		app = self._app_loader.get()
		if app is None:
			return CmdResponse(request_id=rid, ok=False, error="app not loaded")
		try:
			await app.broker.connect()
			await app.enqueue_by_name(task, Payload(args=tuple(args), kwargs=kwargs))
			return CmdResponse(request_id=rid, ok=True)
		except Exception as exc:
			return CmdResponse(request_id=rid, ok=False, error=str(exc))

	async def _trigger_job(self, rid: str, job_id: str) -> CmdResponse:
		app = self._app_loader.get()
		if app is None:
			return CmdResponse(request_id=rid, ok=False, error="app not loaded")
		job = next((j for j in app.schedule.jobs if j.id == job_id), None)
		if job is None:
			return CmdResponse(request_id=rid, ok=False, error="job not found")
		try:
			await app.broker.connect()
			await app.enqueue_by_name(
				job.task_name,
				job.args,
				queue=job.queue,
				headers=job.headers,
				max_attempts=job.max_attempts,
			)
			return CmdResponse(request_id=rid, ok=True)
		except Exception as exc:
			return CmdResponse(request_id=rid, ok=False, error=str(exc))

	def _remove_job(self, rid: str, job_id: str) -> CmdResponse:
		app = self._app_loader.get()
		if app is None:
			return CmdResponse(request_id=rid, ok=False, error="app not loaded")
		before = len(app.schedule.jobs)
		app.schedule.jobs = [j for j in app.schedule.jobs if j.id != job_id]
		if len(app.schedule.jobs) == before:
			return CmdResponse(request_id=rid, ok=False, error="job not found")
		return CmdResponse(request_id=rid, ok=True)
