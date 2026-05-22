from __future__ import annotations

import logging
import os
import socket
from collections import Counter
from datetime import datetime as dtime
from importlib.metadata import version as _pkg_version
from multiprocessing.context import SpawnProcess

from kuu.app import Kuu
from kuu.config import Settings
from kuu.observability import (
	BrokerInfo,
	Hello,
	JobSnapshot,
	QueueSnapshot,
	State,
	TaskInfo,
	TaskParam,
	WorkerSnapshot,
	broker_key,
)
from kuu.orchestrator._app_loader import AppLoader

log = logging.getLogger("kuu.orchestrator.snapshots")


class SnapshotBuilder:
	def __init__(
		self,
		*,
		config: Settings,
		preset: str,
		instance_id: str,
		started_at: dtime,
		app_loader: AppLoader,
		processes: list[SpawnProcess],
		inflight: Counter[str],
		current_task: dict[int, str],
	) -> None:
		self._config = config
		self._preset = preset
		self._instance_id = instance_id
		self._started_at = started_at
		self._app_loader = app_loader
		self._processes = processes
		self._inflight = inflight
		self._current_task = current_task

	def build_hello(self) -> Hello:
		return Hello(
			preset=self._preset,
			host=socket.gethostname(),
			pid=os.getpid(),
			version=_pkg_version("kuu"),
			started_at=self._started_at,
			broker=self._build_broker_info(),
			scheduler_enabled=self._config.scheduler.enable,
			processes=self._config.processes,
			concurrency=self._config.concurrency,
			tasks=self._build_tasks(),
		)

	def _build_tasks(self) -> list[TaskInfo]:
		app = self._app_loader.get()
		if app is None:
			return []
		try:
			from kuu._sig import sig_params

			out: list[TaskInfo] = []
			for name in sorted(app.registry.names()):
				task = app.registry.get(name)
				if task is None:
					continue
				try:
					raw_params, has_varargs = sig_params(task.original_func)
				except Exception:
					raw_params, has_varargs = [], False
				params = [
					TaskParam(
						name=p["name"],
						annotation=p["annotation"],
						default=p["default"],
						required=p["required"],
					)
					for p in raw_params
				]
				out.append(
					TaskInfo(
						name=task.task_name,
						queue=task.task_queue,
						max_attempts=task.max_attempts,
						timeout=task.timeout,
						params=params,
						has_varargs=has_varargs,
					)
				)
			return out
		except Exception as e:
			log.exception("event=snapshots.build_tasks_failed error=%s", e)
			return []

	def _build_broker_info(self) -> BrokerInfo:
		app = self._app_loader.get()
		if app is None:
			return BrokerInfo(type="unknown", key="")
		try:
			broker = app.broker
			return BrokerInfo(type=type(broker).__name__, key=broker_key(broker))
		except Exception as e:
			log.exception("event=snapshots.broker_info_failed error=%s", e)
			return BrokerInfo(type="unknown", key="")

	async def build_state(self) -> State:
		workers = [
			WorkerSnapshot(
				pid=p.pid or -1,
				alive=p.is_alive(),
				current_task=self._current_task.get(p.pid) if p.pid else None,
			)
			for p in self._processes
		]
		return State(
			workers=workers,
			jobs=self._build_jobs(),
			queues=await self._build_queues(),
		)

	async def _build_queues(self) -> dict[str, QueueSnapshot]:
		known_queues = set(self._inflight.keys())
		app = self._app_loader.get()
		if app is not None:
			try:
				known_queues |= set(app.registry.queues() or [app.default_queue])
			except Exception:
				log.warning("event=snapshots.queues_registry_failed")

		out: dict[str, QueueSnapshot] = {}
		for q in known_queues:
			depth = await self._probe_depth(app, q)
			out[q] = QueueSnapshot(in_flight=self._inflight.get(q, 0), depth=depth)
		return out

	async def _probe_depth(self, app: Kuu | None, queue: str) -> int | None:
		if app is None:
			return None
		broker = app.broker
		probe = getattr(broker, "queue_depth", None)
		if probe is None:
			return None
		try:
			return await probe(queue)
		except Exception:
			log.debug("event=snapshots.probe_depth_failed queue=%s", queue)
			return None

	def _build_jobs(self) -> list[JobSnapshot]:
		if not self._config.scheduler.enable:
			return []
		app = self._app_loader.get()
		if app is None:
			return []
		try:
			return [
				JobSnapshot(id=j.id, task=j.task_name, next_run=j.next_run.timestamp())
				for j in app.schedule.jobs
			]
		except Exception as e:
			log.exception("event=snapshots.jobs_failed error=%s", e)
			return []
