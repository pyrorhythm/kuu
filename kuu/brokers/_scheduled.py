from __future__ import annotations

from collections.abc import Awaitable, Callable


async def run_scheduled_pump_loop(
	tick: Callable[[], Awaitable[bool | None]],
	idle: Callable[[], Awaitable[None]],
) -> None:
	"""Run ``tick`` then ``idle`` forever — shared loop shape for in-process scheduled pumps.

	A truthy ``tick`` return means it hit its own batch ceiling and more is due right
	now, so ``idle`` is skipped and the backlog drains at round-trip speed.
	"""
	while True:
		if not await tick():
			await idle()
