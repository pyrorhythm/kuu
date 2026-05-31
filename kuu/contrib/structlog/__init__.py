from __future__ import annotations

from typing import Any

import structlog

from kuu.context import Context
from kuu.middleware.base import Next
from kuu.middleware.task_log import TaskLogSink, run_process_task_logging

__all__ = ("StructlogMiddleware",)


class _StructlogTaskLogSink(TaskLogSink):
	def __init__(self, logger: structlog.BoundLogger) -> None:
		self._log = logger

	def task_start(self, *, task_name: str, sched_id: str, queue: str, attempt: int) -> None:
		self._log.info(
			"task.start",
			task_name=task_name,
			sched_id=sched_id,
			queue=queue,
			attempt=attempt,
		)

	def task_ok(self, *, task_name: str, sched_id: str, duration: float) -> None:
		self._log.info(
			"task.ok",
			task_name=task_name,
			sched_id=sched_id,
			duration=duration,
		)

	def task_fail(self, *, task_name: str, sched_id: str, duration: float, exc_type: str) -> None:
		self._log.warning(
			"task.fail",
			task_name=task_name,
			sched_id=sched_id,
			duration=duration,
			exc_type=exc_type,
		)


class StructlogMiddleware:
	"""Log task start, success, and failure via a structlog logger.

	Only active during the ``process`` phase.

	- `logger`: a ``structlog.BoundLogger`` instance (required).
		The caller is responsible for configuring structlog globally
		(``structlog.configure(...)``) before passing the logger.
	"""

	def __init__(self, logger: structlog.BoundLogger):
		self._sink = _StructlogTaskLogSink(logger)

	async def __call__(self, ctx: Context, call_next: Next) -> Any:
		return await run_process_task_logging(ctx, call_next, self._sink)
