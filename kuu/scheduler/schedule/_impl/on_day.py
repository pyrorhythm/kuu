from datetime import datetime, timedelta

from kuu.scheduler.schedule.abc import Schedule
from kuu.scheduler.schedule.exc import ScheduleError


class on_day(Schedule):
	"""Constrains firing to specific days of the month (1-31).

	Example::

		on_day(1, 15) & at(time(9, 0))   # 1st and 15th of each month at 09:00
	"""

	def __init__(self, *days: int) -> None:
		if not days:
			raise ValueError("on_day requires at least one day")
		if not all(1 <= day <= 31 for day in days):
			raise ValueError("day number must be in range from 1 to 31")

		self.days: frozenset[int] = frozenset(days)

	def matches(self, dt: datetime) -> bool:
		return dt.day in self.days

	def next_after(self, now: datetime) -> datetime:
		if now.day in self.days:
			return now + timedelta(seconds=1)
		dt = now + timedelta(days=1)
		for _ in range(32):
			if dt.day in self.days:
				return dt.replace(hour=0, minute=0, second=0, microsecond=0)
			dt += timedelta(days=1)
		raise ScheduleError(self, "unreachable - no matching day found in the next 32 days")
