from datetime import datetime, time, timedelta

from kuu.scheduler.schedule.abc import Schedule


class every(Schedule):
	"""Fires at a fixed interval within each calendar day, starting from *starting*.

	The interval is anchored to *starting* and tiled across the full day, so all
	occurrences are evenly spaced regardless of what time you first call it.

	Example::

		every(hours=4, starting=time(3, 28, 6))
		# fires at 03:28:06; 07:28:06; 11:28:06 ...

		every(minutes=30)
		# fires at 00:00; 00:30; 01:00 ...
	"""

	def __init__(
		self,
		*,
		hours: int = 0,
		minutes: int = 0,
		seconds: int = 0,
		starting: time = time(0, 0, 0),
	) -> None:
		self.interval = timedelta(hours=hours, minutes=minutes, seconds=seconds)
		if self.interval.total_seconds() <= 0:
			raise ValueError("every requires a positive interval (hours, minutes, or seconds)")
		self.starting = starting.replace(tzinfo=None, microsecond=0)

	def _fire_times_on_date(self, dt: datetime) -> list[datetime]:
		midnight = dt.replace(hour=0, minute=0, second=0, microsecond=0)
		day_end = midnight + timedelta(days=1)
		anchor = midnight.replace(
			hour=self.starting.hour,
			minute=self.starting.minute,
			second=self.starting.second,
		)
		interval_secs = self.interval.total_seconds()
		elapsed = (anchor - midnight).total_seconds()
		k = int(elapsed / interval_secs)
		first = anchor - timedelta(seconds=k * interval_secs)
		if first < midnight:
			first += self.interval

		result: list[datetime] = []
		t = first
		while t < day_end:
			result.append(t)
			t += self.interval
		return result

	def matches(self, dt: datetime) -> bool:
		dt_sec = dt.replace(microsecond=0)
		return any(t == dt_sec for t in self._fire_times_on_date(dt))

	def next_after(self, now: datetime) -> datetime:
		for t in self._fire_times_on_date(now):
			if t > now:
				return t
		tomorrow = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
		return self._fire_times_on_date(tomorrow)[0]
