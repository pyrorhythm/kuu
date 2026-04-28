from datetime import datetime, time, timedelta

from kuu.scheduler.schedule.abc import Schedule


class at(Schedule):
	"""Fires every day at a specific wall-clock time.

	Example::

		at(time(9, 30))                  # daily at 09:30:00
		at(time(3, 28, 6))               # daily at 03:28:06
	"""

	def __init__(self, t: time) -> None:
		self.t = t.replace(tzinfo=None, microsecond=0)

	def matches(self, dt: datetime) -> bool:
		return dt.replace(microsecond=0).time().replace(tzinfo=None) == self.t

	def next_after(self, now: datetime) -> datetime:
		cand = now.replace(
			hour=self.t.hour,
			minute=self.t.minute,
			second=self.t.second,
			microsecond=0,
		)
		if cand > now:
			return cand
		return cand + timedelta(days=1)
