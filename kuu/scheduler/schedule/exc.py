from typing import Any

from kuu.scheduler.schedule.abc import Schedule


class ScheduleError(BaseException):
	def __init__(self, sched: Schedule, *args: Any, **kwargs: Any):
		super().__init__(*args, **kwargs)
		self.sched = sched
