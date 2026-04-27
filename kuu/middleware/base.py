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
    idx = -1

    async def _next() -> Any:
        nonlocal idx
        idx += 1
        if idx >= len(chain):
            return await terminal(ctx)
        return await chain[idx](ctx, _next)

    return await _next()
