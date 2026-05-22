from __future__ import annotations

from collections.abc import Awaitable, Callable


async def run_scheduled_pump_loop(
	tick: Callable[[], Awaitable[None]],
	idle: Callable[[], Awaitable[None]],
) -> None:
	"""Run ``tick`` then ``idle`` forever — shared loop shape for in-process scheduled pumps."""
	while True:
		await tick()
		await idle()
