from datetime import datetime, timedelta

from kuu.scheduler.schedule.abc import Schedule
from kuu.scheduler.schedule.enums import Weekday

from ..exc import ScheduleError


class on(Schedule):
	"""Constrains firing to specific weekdays.

	On its own this is rarely useful; compose it with a time-based schedule::

		on(Tue, Fri, Sun) & every(hours=4, starting=time(3, 28, 6))
		on(Mon, Wed, Fri) & at(time(9, 0))
	"""

	def __init__(self, *weekdays: Weekday) -> None:
		if not weekdays:
			raise ValueError("On requires at least one weekday")
		self.weekdays: frozenset[int] = frozenset(int(w) for w in weekdays)

	def matches(self, dt: datetime) -> bool:
		return dt.weekday() in self.weekdays

	def next_after(self, now: datetime) -> datetime:
		if now.weekday() in self.weekdays:
			return now + timedelta(seconds=1)
		for d in range(1, 8):
			if (now.weekday() + d) % 7 in self.weekdays:
				next_day = now + timedelta(days=d)
				return next_day.replace(hour=0, minute=0, second=0, microsecond=0)
		raise ScheduleError(self, "unreachable - weekdays is non-empty")
