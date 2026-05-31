from __future__ import annotations

import time
from typing import Any, Protocol

from ..context import Context
from .base import Next


class TaskLogSink(Protocol):
	"""Receiver for task lifecycle log events.

	Implement this protocol to back a logging middleware with any frontend
	(stdlib :mod:`logging`, ``structlog``, a metrics emitter, ...). Drive it
	with :func:`run_process_task_logging` from a middleware ``__call__``.

	``LoggingMiddleware`` and ``kuu.contrib.structlog.StructlogMiddleware`` are
	the bundled implementations.
	"""

	def task_start(self, *, task_name: str, sched_id: str, queue: str, attempt: int) -> None: ...

	def task_ok(self, *, task_name: str, sched_id: str, duration: float) -> None: ...

	def task_fail(
		self, *, task_name: str, sched_id: str, duration: float, exc_type: str
	) -> None: ...


async def run_process_task_logging(
	ctx: Context,
	call_next: Next,
	sink: TaskLogSink,
) -> Any:
	"""Drive a :class:`TaskLogSink` around the ``process`` phase of a task.

	Emits ``task_start`` before the task runs, then ``task_ok`` on success or
	``task_fail`` on exception (re-raising). The ``enqueue`` phase is passed
	through untouched.
	"""
	if ctx.phase != "process":
		return await call_next()
	msg = ctx.message
	started = time.perf_counter()
	sink.task_start(
		task_name=msg.task,
		sched_id=str(msg.id),
		queue=msg.queue,
		attempt=msg.attempt,
	)
	try:
		result = await call_next()
	except Exception as e:
		sink.task_fail(
			task_name=msg.task,
			sched_id=str(msg.id),
			duration=time.perf_counter() - started,
			exc_type=type(e).__name__,
		)
		raise
	sink.task_ok(
		task_name=msg.task,
		sched_id=str(msg.id),
		duration=time.perf_counter() - started,
	)
	return result
