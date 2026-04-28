from datetime import datetime, timedelta

from kuu.scheduler.schedule.abc import Schedule
from kuu.scheduler.schedule.enums import Month

from ..exc import ScheduleError


class in_month(Schedule):
	"""Constrains firing to specific months (1 = January … 12 = December).

	Example::

		in_month(1, 7) & on_day(1) & at(time(0))   # Jan 1st and Jul 1st at midnight
	"""

	def __init__(self, *months: Month) -> None:
		if not months:
			raise ValueError("in_month requires at least one month")
		self.months: frozenset[int] = frozenset(months)

	def matches(self, dt: datetime) -> bool:
		return dt.month in self.months

	def next_after(self, now: datetime) -> datetime:
		if now.month in self.months:
			return now + timedelta(seconds=1)
		dt = (now.replace(day=1) + timedelta(days=32)).replace(day=1)
		for _ in range(13):
			if dt.month in self.months:
				return dt.replace(hour=0, minute=0, second=0, microsecond=0)
			dt = (dt + timedelta(days=32)).replace(day=1)
		raise ScheduleError(self, "unreachable - no matching month found")
