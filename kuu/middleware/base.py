from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from ..context import Context

Next = Callable[[], Awaitable[Any]]


class Middleware(Protocol):
	async def __call__(self, ctx: Context, call_next: Next) -> Any: ...


async def run_chain(
	ctx: Context,
	chain: list[Middleware],
	terminal: Callable[[Context], Awaitable[Any]],
) -> Any:
	"""
	Execute middleware chain and terminal handler.

	Args:
		ctx: task context
		chain: ordered list of middleware to execute
		terminal: final handler invoked after all middleware

	Returns:
		Result from terminal handler or middleware
	"""
	idx = -1

	async def _next() -> Any:
		nonlocal idx
		idx += 1
		if idx >= len(chain):
			return await terminal(ctx)
		return await chain[idx](ctx, _next)

	return await _next()
