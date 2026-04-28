from __future__ import annotations

import logging
import signal
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import anyio

from kuu._types import _Fn, _Wrap

from ..message import Payload
from ..task import Task
from .job import BaseJob, CronJob, IntervalJob, _next_after

if TYPE_CHECKING:
	from ..app import Kuu

log = logging.getLogger("kuu.scheduler")


type CronField = int | str | Iterable[int] | None


def _fmt_field(val: CronField) -> str:
	if val is None:
		return "*"
	if isinstance(val, bool):
		raise TypeError("bool not allowed in cron field")
	if isinstance(val, int):
		return str(val)
	if isinstance(val, str):
		return val.strip() or "*"
	# iterable of ints
	parts = [str(v) for v in val]
	if not parts:
		return "*"
	return ",".join(parts)


@dataclass(frozen=True, slots=True)
class CronExpr:
	second: CronField = None
	minute: CronField = None
	hour: CronField = None
	day: CronField = None
	month: CronField = None
	day_of_week: CronField = None
	second_interval: int | None = None
	minute_interval: int | None = None
	hour_interval: int | None = None

	def _build(self) -> str:
		def merge(val: CronField, interval: int | None, name: str) -> CronField:
			if interval is None:
				return val
			if val is not None:
				raise ValueError(f"specify either {name} or {name}_interval, not both")
			return f"*/{interval}"

		second = merge(self.second, self.second_interval, "second")
		minute = merge(self.minute, self.minute_interval, "minute")
		hour = merge(self.hour, self.hour_interval, "hour")

		largest_set = (
			"month"
			if self.month is not None
			else "day"
			if self.day is not None or self.day_of_week is not None
			else "hour"
			if hour is not None
			else "minute"
			if minute is not None
			else "second"
			if second is not None
			else None
		)
		order = ["second", "minute", "hour", "day", "month"]
		specified: dict[str, CronField] = {
			"second": second,
			"minute": minute,
			"hour": hour,
			"day": self.day,
			"month": self.month,
			"day_of_week": self.day_of_week,
		}
		if largest_set is not None and largest_set in order:
			idx = order.index(largest_set)
			for name in order[:idx]:
				if specified[name] is None:
					specified[name] = 0

		return " ".join([
			_fmt_field(specified["second"]),
			_fmt_field(specified["minute"]),
			_fmt_field(specified["hour"]),
			_fmt_field(specified["day"]),
			_fmt_field(specified["month"]),
			_fmt_field(specified["day_of_week"]),
		])


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

	def every[**P, R](
		self,
		interval: timedelta,
		args: Payload = Payload(),
		*,
		id: str | None = None,
		queue: str | None = None,
		headers: dict[str, str] | None = None,
		max_attempts: int | None = None,
	) -> _Wrap[P, R]:
		def wrap(fn: _Fn[P, R]) -> Task[P, R]:
			if not isinstance(fn, Task):
				fn = self.app.task(fn)

			job = IntervalJob(
				id=id or f"interval:{fn.name}:{len(self.jobs)}",
				task_name=fn.name,
				args=args,
				queue=queue,
				headers=headers,
				max_attempts=max_attempts,
				every=interval,
				next_run=datetime.now(timezone.utc) + interval,
			)
			self.jobs.append(job)
			return fn

		return wrap

	def add_cron(
		self,
		task: str | Task,
		expr: str | CronExpr,
		args: Payload = Payload(),
		*,
		id: str | None = None,
		queue: str | None = None,
		headers: dict[str, str] | None = None,
		max_attempts: int | None = None,
	) -> CronJob:
		if isinstance(expr, CronExpr):
			expr = expr._build()

		job = CronJob(
			id=id or f"cron:{self._resolve(task)}:{len(self.jobs)}",
			task_name=self._resolve(task),
			args=args,
			queue=queue,
			headers=headers,
			max_attempts=max_attempts,
			expr=expr,
			next_run=_next_after(expr, datetime.now(timezone.utc)),
		)
		self.jobs.append(job)
		return job

	def cron[**P, R](
		self,
		expr: str | CronExpr,
		args: Payload = Payload(),
		*,
		id: str | None = None,
		queue: str | None = None,
		headers: dict[str, str] | None = None,
		max_attempts: int | None = None,
	) -> _Wrap[P, R]:
		if isinstance(expr, CronExpr):
			expr = expr._build()

		def wrap(fn: _Fn[P, R]) -> Task[P, R]:
			if not isinstance(fn, Task):
				fn = self.app.task(fn)

			job = CronJob(
				id=id or f"cron:{fn.name}:{len(self.jobs)}",
				task_name=fn.name,
				args=args,
				queue=queue,
				headers=headers,
				max_attempts=max_attempts,
				expr=expr,
				next_run=_next_after(expr, datetime.now(timezone.utc)),
			)
			self.jobs.append(job)

			return fn

		return wrap

	async def run(self, *, install_signals: bool = True) -> None:
		"""
		Run the scheduler loop until cancelled or signalled.

		Pass `install_signals=False` when embedding inside another runtime
		(the orchestrator does this) that already owns SIGINT/SIGTERM and
		will cancel the loop via its own task-group scope.
		"""
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
