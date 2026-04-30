from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from ..context import Context

Next = Callable[[], Awaitable[Any]]


class Middleware(Protocol):
	"""
	Wraps the enqueue and process pipelines.

	Implementations call `await call_next()` to invoke the next middleware
	(or the terminal handler if last in the chain). They may inspect or
	mutate `ctx` before/after, swallow exceptions, retry, or short-circuit
	by returning without calling `call_next`.
	"""

	async def __call__(self, ctx: Context, call_next: Next) -> Any: ...


async def run_chain(
	ctx: Context,
	chain: list[Middleware],
	terminal: Callable[[Context], Awaitable[Any]],
) -> Any:
	"""Drive `chain` around `terminal`, returning whatever the chain returns."""
	handlers = [*chain, _TerminalWrapper(terminal)]

	async def _dispatch(i: int = 0) -> Any:
		if i >= len(handlers):
			return None

		async def _call_next() -> Any:
			return await _dispatch(i + 1)

		return await handlers[i](ctx, _call_next)

	return await _dispatch(0)


class _TerminalWrapper:
	"""Adapter to make a terminal callable conform to Middleware protocol."""

	__slots__ = ("_terminal",)

	def __init__(self, terminal: Callable[[Context], Awaitable[Any]]) -> None:
		self._terminal = terminal

	async def __call__(self, ctx: Context, call_next: Next) -> Any:
		return await self._terminal(ctx)
