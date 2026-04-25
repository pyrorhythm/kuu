from __future__ import annotations

import logging
import time
from typing import Any

from ..context import Context
from .base import Next


class LoggingMiddleware:
	def __init__(self, logger: logging.Logger | None = None):
		self.log = logger or logging.getLogger("kuu.task")

	async def __call__(self, ctx: Context, call_next: Next) -> Any:
		if ctx.phase != "process":
			return await call_next()
		msg = ctx.message
		started = time.perf_counter()
		self.log.info(
			"task.start name=%s id=%s queue=%s attempt=%d", msg.task, msg.id, msg.queue, msg.attempt
		)
		try:
			result = await call_next()
		except BaseException as e:
			self.log.warning(
				"task.fail name=%s id=%s duration=%.3fs exc=%s",
				msg.task,
				msg.id,
				time.perf_counter() - started,
				type(e).__name__,
			)
			raise
		self.log.info(
			"task.ok name=%s id=%s duration=%.3fs", msg.task, msg.id, time.perf_counter() - started
		)
		return result
