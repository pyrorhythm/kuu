from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from croniter import croniter

from ..message import Payload

if TYPE_CHECKING:
	pass


def _next_after(expr: str, after: datetime) -> datetime:
	if after.tzinfo is None:
		after = after.replace(tzinfo=timezone.utc)
	it = croniter(expr, after, second_at_beginning=True)
	return it.get_next(datetime)


@dataclass
class BaseJob(ABC):
	id: str
	task_name: str
	args: Payload = field(default_factory=Payload)
	queue: str | None = None
	headers: dict[str, str] | None = None
	max_attempts: int | None = None
	next_run: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

	@abstractmethod
	def schedule_next(self, now: datetime) -> None: ...


@dataclass
class IntervalJob(BaseJob):
	every: timedelta = timedelta(seconds=1)

	def schedule_next(self, now: datetime) -> None:
		nxt = self.next_run + self.every
		if nxt <= now:
			missed = (now - self.next_run) // self.every
			nxt = self.next_run + self.every * (missed + 1)
		self.next_run = nxt


@dataclass
class CronJob(BaseJob):
	expr: str = "0 * * * * *"

	def schedule_next(self, now: datetime) -> None:
		self.next_run = _next_after(self.expr, now)
