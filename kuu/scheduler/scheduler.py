from __future__ import annotations

import logging
import signal
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import anyio
from croniter import croniter

from ..message import Payload
from .job import BaseJob, CronJob, IntervalJob
from ..task import Task

if TYPE_CHECKING:
	from ..app import Kuu

log = logging.getLogger("qq.scheduler")


def _next_after(expr: str, after: datetime) -> datetime:
	if after.tzinfo is None:
		after = after.replace(tzinfo=timezone.utc)
	it = croniter(expr, after, second_at_beginning=True)
	return it.get_next(datetime)


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
	parts = [str(int(v)) for v in val]
	if not parts:
		return "*"
	return ",".join(parts)


def _build_expr(
	*,
	second: CronField,
	minute: CronField,
	hour: CronField,
	day: CronField,
	month: CronField,
	day_of_week: CronField,
	second_interval: int | None,
	minute_interval: int | None,
	hour_interval: int | None,
) -> str:
	def merge(val: CronField, interval: int | None, name: str) -> CronField:
		if interval is None:
			return val
		if val is not None:
			raise ValueError(f"specify either {name} or {name}_interval, not both")
		return f"*/{int(interval)}"

	second = merge(second, second_interval, "second")
	minute = merge(minute, minute_interval, "minute")
	hour = merge(hour, hour_interval, "hour")

	largest_set = (
		"month"
		if month is not None
		else "day"
		if day is not None or day_of_week is not None
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
		"day": day,
		"month": month,
		"day_of_week": day_of_week,
	}
	if largest_set is not None and largest_set in order:
		idx = order.index(largest_set)
		for name in order[:idx]:
			if specified[name] is None:
				specified[name] = 0

	# croniter 6-field with second_at_beginning=True: sec min hour dom mon dow
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

	def every(
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

	def cron(
		self,
		task: str | Task,
		args: Payload = Payload(),
		*,
		expr: str | None = None,
		second: CronField = None,
		minute: CronField = None,
		hour: CronField = None,
		second_interval: int | None = None,
		minute_interval: int | None = None,
		hour_interval: int | None = None,
		day: CronField = None,
		month: CronField = None,
		day_of_week: CronField = None,
		id: str | None = None,
		queue: str | None = None,
		headers: dict[str, str] | None = None,
		max_attempts: int | None = None,
	) -> CronJob:
		if expr is None:
			expr = _build_expr(
				second=second,
				minute=minute,
				hour=hour,
				day=day,
				month=month,
				day_of_week=day_of_week,
				second_interval=second_interval,
				minute_interval=minute_interval,
				hour_interval=hour_interval,
			)
		else:
			any_kw = any(
				v is not None
				for v in (
					second,
					minute,
					hour,
					day,
					month,
					day_of_week,
					second_interval,
					minute_interval,
					hour_interval,
				)
			)
			if any_kw:
				raise ValueError("pass either `expr` or structured fields, not both")

		now = datetime.now(timezone.utc)
		# validates by parsing; croniter raises on bad expr
		first = _next_after(expr, now)
		job = CronJob(
			id=id or f"cron:{self._resolve(task)}:{len(self.jobs)}",
			task_name=self._resolve(task),
			args=args,
			queue=queue,
			headers=headers,
			max_attempts=max_attempts,
			expr=expr,
			next_run=first,
		)
		self.jobs.append(job)
		return job

	async def run(self, *, install_signals: bool = True) -> None:
		"""
		run the scheduler loop until cancelled or signalled.

		`install_signals=False` is for embedding inside another runtime (e.g.
		the orchestrator) that already owns SIGINT/SIGTERM handling and will
		cancel us via its own task-group scope.
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
			if delay > 0:
				await anyio.sleep(min(delay, 60.0))
				continue
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
