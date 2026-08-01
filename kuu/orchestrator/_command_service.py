from __future__ import annotations

import inspect
import logging
from collections import OrderedDict
from typing import TYPE_CHECKING

import anyio

from kuu.observability import (
	CancelCmd,
	Cmd,
	CmdResponse,
	EnqueueCmd,
	RemoveJobCmd,
	ReplayCmd,
	RetryCmd,
	TriggerJobCmd,
)

if TYPE_CHECKING:
	from kuu.orchestrator._app_loader import AppLoader

log = logging.getLogger("kuu.orchestrator.commands")


class CommandService:
	def __init__(self, app_loader: AppLoader, *, dedupe_size: int = 1024) -> None:
		self._app_loader = app_loader
		self._dedupe_size = dedupe_size
		self._responses: OrderedDict[str, CmdResponse] = OrderedDict()
		self._inflight: dict[str, tuple[anyio.Event, list[CmdResponse]]] = {}

	async def dispatch(self, cmd: Cmd) -> CmdResponse:
		rid = getattr(cmd, "request_id", "?")
		cached = self._responses.get(rid)
		if cached is not None:
			return cached
		pending = self._inflight.get(rid)
		if pending is not None:
			event, holder = pending
			await event.wait()
			return holder[0]

		event = anyio.Event()
		holder: list[CmdResponse] = []
		self._inflight[rid] = (event, holder)
		try:
			response = await self._execute(cmd, rid)
		except BaseException:
			response = CmdResponse(request_id=rid, ok=False, error="command interrupted")
			self._remember(rid, response)
			holder.append(response)
			event.set()
			self._inflight.pop(rid, None)
			raise
		self._remember(rid, response)
		holder.append(response)
		event.set()
		self._inflight.pop(rid, None)
		return response

	async def _execute(self, cmd: Cmd, rid: str) -> CmdResponse:
		match cmd:
			case EnqueueCmd(task=task, args=args, kwargs=kwargs, queue=queue):
				return await self._enqueue(rid, task, args, kwargs, queue=queue)
			case ReplayCmd(replay_of=source, task=task, args=args, kwargs=kwargs, queue=queue):
				return await self._enqueue(
					rid,
					task,
					args,
					kwargs,
					queue=queue,
					headers={"kuu.replay_of": source},
				)
			case RetryCmd(
				run_id=run_id,
				attempt=attempt,
				task=task,
				args=args,
				kwargs=kwargs,
				queue=queue,
			):
				return await self._retry(rid, run_id, attempt, task, args, kwargs, queue)
			case CancelCmd(run_id=run_id):
				return await self._cancel(rid, run_id)
			case TriggerJobCmd(job_id=job_id):
				return await self._trigger_job(rid, job_id)
			case RemoveJobCmd(job_id=job_id):
				return self._remove_job(rid, job_id)
			case _:
				return CmdResponse(
					request_id=rid,
					ok=False,
					error=f"unknown command: {type(cmd).__name__}",
				)

	def _remember(self, rid: str, response: CmdResponse) -> None:
		self._responses[rid] = response
		self._responses.move_to_end(rid)
		while len(self._responses) > self._dedupe_size:
			self._responses.popitem(last=False)

	async def _enqueue(
		self,
		rid: str,
		task: str,
		args: list,
		kwargs: dict,
		*,
		queue: str | None = None,
		headers: dict[str, str] | None = None,
	) -> CmdResponse:
		from kuu.message import Payload

		app = self._app_loader.get()
		if app is None:
			return CmdResponse(request_id=rid, ok=False, error="app not loaded")
		definition = app.registry.get(task)
		if definition is None:
			return CmdResponse(request_id=rid, ok=False, error="task not found")
		try:
			inspect.signature(definition.original_func).bind(*args, **kwargs)
			await app.broker.connect()
			handle = await app.enqueue_by_name(
				task,
				Payload(args=tuple(args), kwargs=kwargs),
				queue=queue,
				headers=headers,
			)
			return CmdResponse(request_id=rid, ok=True, run_id=str(handle.message.id))
		except Exception as exc:
			return CmdResponse(request_id=rid, ok=False, error=str(exc))

	async def _retry(
		self,
		rid: str,
		run_id: str,
		attempt: int,
		task: str,
		args: list,
		kwargs: dict,
		queue: str | None,
	) -> CmdResponse:
		from uuid import UUID

		from msgspec.structs import replace

		from kuu.message import Payload

		app = self._app_loader.get()
		if app is None:
			return CmdResponse(request_id=rid, ok=False, error="app not loaded")
		definition = app.registry.get(task)
		if definition is None:
			return CmdResponse(request_id=rid, ok=False, error="task not found")
		try:
			inspect.signature(definition.original_func).bind(*args, **kwargs)
			message = app._build_message(
				task,
				definition,
				Payload(args=tuple(args), kwargs=kwargs),
				queue=queue,
				not_before=None,
				headers={"kuu.operator_retry": "true"},
				max_attempts=max(definition.max_attempts, attempt + 1),
			)
			message = replace(message, id=UUID(run_id), attempt=attempt)
			await app.broker.connect()
			await app._dispatch(message, definition, None)
			return CmdResponse(request_id=rid, ok=True, run_id=run_id)
		except Exception as exc:
			return CmdResponse(request_id=rid, ok=False, error=str(exc))

	async def _cancel(self, rid: str, run_id: str) -> CmdResponse:
		app = self._app_loader.get()
		if app is None:
			return CmdResponse(request_id=rid, ok=False, error="app not loaded")
		try:
			await app.cancel(run_id)
			return CmdResponse(request_id=rid, ok=True, run_id=run_id)
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
			handle = await app.enqueue_by_name(
				job.task_name,
				job.args,
				queue=job.queue,
				headers=job.headers,
				max_attempts=job.max_attempts,
			)
			return CmdResponse(request_id=rid, ok=True, run_id=str(handle.message.id))
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
