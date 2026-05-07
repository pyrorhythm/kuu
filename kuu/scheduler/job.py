from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from kuu._util import utcnow

from ..message import Payload
from .schedule import Schedule

if TYPE_CHECKING:
	pass


@dataclass
class BaseJob(ABC):
	id: str
	task_name: str
	args: Payload = field(default_factory=Payload)
	queue: str | None = None
	headers: dict[str, str] | None = None
	max_attempts: int | None = None
	next_run: datetime = field(default_factory=utcnow)

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
class ScheduleJob(BaseJob):
	schedule: Schedule = field(kw_only=True)

	def schedule_next(self, now: datetime) -> None:
		self.next_run = self.schedule.next_after(now)
