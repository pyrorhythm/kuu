from datetime import datetime, time, timedelta

from kuu.scheduler.schedule.abc import Schedule


class between(Schedule):
	"""Constrains firing to a time window within each day (inclusive on both ends).

	Example::

		every(minutes=30) & between(time(9), time(17))
		# fires every 30 min but only between 09:00 and 17:00
	"""

	def __init__(self, start: time, end: time) -> None:
		self.start = start.replace(tzinfo=None, microsecond=0)
		self.end = end.replace(tzinfo=None, microsecond=0)

	def matches(self, dt: datetime) -> bool:
		t = dt.time().replace(tzinfo=None, microsecond=0)
		return self.start <= t <= self.end

	def next_after(self, now: datetime) -> datetime:
		t = now.time().replace(tzinfo=None, microsecond=0)
		if t < self.start:
			return now.replace(
				hour=self.start.hour,
				minute=self.start.minute,
				second=self.start.second,
				microsecond=0,
			)
		if t <= self.end:
			return now + timedelta(seconds=1)
		tomorrow = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
		return tomorrow.replace(
			hour=self.start.hour,
			minute=self.start.minute,
			second=self.start.second,
		)
