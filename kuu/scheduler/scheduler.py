from __future__ import annotations

import logging
import signal
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import anyio


from ..message import Payload
from ..task import Task
from .job import BaseJob, IntervalJob, ScheduleJob
from .schedule import Schedule

if TYPE_CHECKING:
	from ..app import Kuu

log = logging.getLogger("kuu.scheduler")


class Scheduler:
	def __init__(self, app: Kuu):
		self.app = app
		self.jobs: list[BaseJob] = []

	def _resolve(self, task: str | Task) -> str:
		return task.task_name if isinstance(task, Task) else task

	def add_every(
		self,
		interval: timedelta,
		task: str | Task,
		args: Payload = Payload(),
		*,
		id: str | None = None,
		queue: str | None = None,
		headers: dict[str, str] | None = None,
		max_attempts: int | None = None,
	) -> IntervalJob:
		now = datetime.now(timezone.utc)
		job = IntervalJob(
			id=id or f"interval:{self._resolve(task)}:{len(self.jobs)}",
			task_name=self._resolve(task),
			args=args,
			queue=queue,
			headers=headers,
			max_attempts=max_attempts,
			every=interval,
			next_run=now + interval,
		)
		self.jobs.append(job)
		return job

	def add_schedule(
		self,
		schedule: Schedule,
		task: str | Task,
		args: Payload = Payload(),
		*,
		id: str | None = None,
		queue: str | None = None,
		headers: dict[str, str] | None = None,
		max_attempts: int | None = None,
	) -> ScheduleJob:
		job = ScheduleJob(
			id=id or f"schedule:{self._resolve(task)}:{len(self.jobs)}",
			task_name=self._resolve(task),
			args=args,
			queue=queue,
			headers=headers,
			max_attempts=max_attempts,
			schedule=schedule,
			next_run=schedule.next_after(datetime.now(timezone.utc)),
		)
		self.jobs.append(job)
		return job

	async def run(self, *, install_signals: bool = True) -> None:
		await self.app.broker.connect()
		try:
			if install_signals:
				scope = anyio.CancelScope()
				async with anyio.create_task_group() as tg:
					tg.start_soon(self._signals, scope)
					with scope:
						await self._loop()
					tg.cancel_scope.cancel()
			else:
				await self._loop()
		finally:
			await self.app.broker.close()

	async def _signals(self, scope: anyio.CancelScope) -> None:
		try:
			with anyio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
				async for _sig in signals:
					scope.cancel()
					return
		except NotImplementedError:
			await anyio.sleep_forever()

	async def _loop(self) -> None:
		if not self.jobs:
			log.warning("scheduler started with no jobs")
		while True:
			now = datetime.now(timezone.utc)
			if not self.jobs:
				await anyio.sleep(1.0)
				continue
			earliest = min(j.next_run for j in self.jobs)
			delay = (earliest - now).total_seconds()
			if delay > 0.001:
				await anyio.sleep(min(max(delay, 0), 60.0))
			now = datetime.now(timezone.utc)
			for job in self.jobs:
				if job.next_run <= now:
					try:
						await self.app.enqueue_by_name(
							job.task_name,
							args=job.args,
							queue=job.queue,
							headers=job.headers,
							max_attempts=job.max_attempts,
						)
					except BaseException:
						log.exception("scheduler: enqueue %s failed", job.id)
					job.schedule_next(now)
