from __future__ import annotations

import logging
from typing import Any

from ..context import Context
from .base import Next
from .task_log import TaskLogSink, run_process_task_logging


class _StdlibTaskLogSink(TaskLogSink):
	def __init__(self, logger: logging.Logger) -> None:
		self._log = logger

	def task_start(self, *, task_name: str, sched_id: str, queue: str, attempt: int) -> None:
		self._log.info(
			"event=task.start name=%s sched_id=%s queue=%s attempt=%d",
			task_name,
			sched_id,
			queue,
			attempt,
		)

	def task_ok(self, *, task_name: str, sched_id: str, duration: float) -> None:
		self._log.info(
			"event=task.ok name=%s sched_id=%s duration=%.3fs",
			task_name,
			sched_id,
			duration,
		)

	def task_fail(self, *, task_name: str, sched_id: str, duration: float, exc_type: str) -> None:
		self._log.warning(
			"event=task.fail name=%s sched_id=%s duration=%.3fs exc=%s",
			task_name,
			sched_id,
			duration,
			exc_type,
		)


class LoggingMiddleware:
	"""
	Log a line at task start, success, and failure.

	- `logger`: logger to write to. Defaults to `logging.getLogger("kuu.task")`.
	"""

	def __init__(self, logger: logging.Logger | None = None):
		self._sink = _StdlibTaskLogSink(logger or logging.getLogger("kuu.task"))

	async def __call__(self, ctx: Context, call_next: Next) -> Any:
		return await run_process_task_logging(ctx, call_next, self._sink)
