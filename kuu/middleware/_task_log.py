from __future__ import annotations

import time
from typing import Any, Protocol

from ..context import Context
from .base import Next


class TaskLogSink(Protocol):
	def task_start(self, *, task_name: str, sched_id: str, queue: str, attempt: int) -> None: ...

	def task_ok(
		self, *, task_name: str, sched_id: str, duration: float
	) -> None: ...

	def task_fail(
		self, *, task_name: str, sched_id: str, duration: float, exc_type: str
	) -> None: ...


async def run_process_task_logging(
	ctx: Context,
	call_next: Next,
	sink: TaskLogSink,
) -> Any:
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
