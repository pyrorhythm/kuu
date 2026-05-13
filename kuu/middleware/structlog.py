from __future__ import annotations

import time
from typing import Any

import structlog

from ..context import Context
from .base import Next


class StructlogMiddleware:
	"""Log task start, success, and failure via a structlog logger.

	Only active during the ``process`` phase.

	- `logger`: a ``structlog.BoundLogger`` instance (required).
		The caller is responsible for configuring structlog globally
		(``structlog.configure(...)``) before passing the logger.
	"""

	def __init__(self, logger: structlog.BoundLogger):
		self.log = logger

	async def __call__(self, ctx: Context, call_next: Next) -> Any:
		if ctx.phase != "process":
			return await call_next()
		msg = ctx.message
		started = time.perf_counter()
		self.log.info(
			"task.start",
			task_name=msg.task,
			sched_id=str(msg.id),
			queue=msg.queue,
			attempt=msg.attempt,
		)
		try:
			result = await call_next()
		except Exception as e:
			self.log.warning(
				"task.fail",
				task_name=msg.task,
				sched_id=str(msg.id),
				duration=time.perf_counter() - started,
				exc_type=type(e).__name__,
			)
			raise
		self.log.info(
			"task.ok",
			task_name=msg.task,
			sched_id=str(msg.id),
			duration=time.perf_counter() - started,
		)
		return result
