from __future__ import annotations

import random
from typing import Any

from ..context import Context
from ..exceptions import RejectErr, RetryErr
from .base import Next


class RetryMiddleware:
	"""
	Retry failed tasks with exponential backoff.

	Catches `RetryErr` and, when `retry_on` is set, matching exception types.
	Computes the delay for the next attempt as `min(cap, base * 2 ** attempt)`
	with `jitter`-bounded randomization, and stores it on
	`ctx.state["retry_delay"]` for the worker to use when nack-ing.

	- `base`: initial backoff in seconds.
	- `cap`: upper bound on the backoff in seconds.
	- `jitter`: fractional randomization applied to each backoff.
	- `retry_on`: optional tuple of exception types to auto-retry (in addition
	  to explicit `RetryErr`). When `None`, only explicit `RetryErr` triggers
	  a retry; all other exceptions propagate to the worker and fail the task.
	"""

	def __init__(
		self,
		base: float = 0.5,
		cap: float = 60.0,
		jitter: float = 0.2,
		retry_on: tuple[type[Exception], ...] | None = None,
	):
		self.base = base
		self.cap = cap
		self.jitter = jitter
		self.retry_on = retry_on

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
		except Exception as e:
			if self.retry_on and isinstance(e, self.retry_on):
				if ctx.message.attempt + 1 >= ctx.message.max_attempts:
					raise
				ctx.state["retry_delay"] = self._backoff(ctx.message.attempt)
				raise RetryErr(delay=ctx.state["retry_delay"], reason=type(e).__name__)
			raise
