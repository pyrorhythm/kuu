from __future__ import annotations

from typing import Any

import anyio

from ..context import Context
from .base import Next


class TimeoutMiddleware:
	def __init__(self, seconds: float):
		self.seconds = seconds

	async def __call__(self, ctx: Context, call_next: Next) -> Any:
		if ctx.phase != "process":
			return await call_next()
		t = getattr(ctx.task, "timeout", None) or self.seconds
		with anyio.fail_after(t):
			return await call_next()
