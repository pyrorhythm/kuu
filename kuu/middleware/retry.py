from __future__ import annotations

import random
from typing import Any

from ..context import Context
from ..exceptions import RejectErr, RetryErr
from .base import Next


class RetryMiddleware:
	"""
	Compute retry delays for failed tasks

	Catches RetryErr and unexpected exceptions; then calculates exponential backoff with jitter for the next attempt

	Args:
		base: initial backoff duration in seconds
		cap: maximum backoff duration in seconds
		jitter: randomization factor applied to backoff
	"""

	def __init__(self, base: float = 0.5, cap: float = 60.0, jitter: float = 0.2):
		self.base = base
		self.cap = cap
		self.jitter = jitter

	def _backoff(self, attempt: int) -> float:
		d = min(self.cap, self.base * (2**attempt))
		return d * (1.0 + random.uniform(-self.jitter, self.jitter))

	async def __call__(self, ctx: Context, call_next: Next) -> Any:
		if ctx.phase != "process":
			return await call_next()
		try:
			return await call_next()
		except RejectErr:
			raise
		except RetryErr as r:
			delay = r.delay if r.delay is not None else self._backoff(ctx.message.attempt)
			ctx.state["retry_delay"] = delay
			raise
		except Exception:
			if ctx.message.attempt + 1 >= ctx.message.max_attempts:
				raise
			ctx.state["retry_delay"] = self._backoff(ctx.message.attempt)
			raise RetryErr(delay=ctx.state["retry_delay"], reason="exception")
