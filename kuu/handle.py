from __future__ import annotations

import typing
from typing import TYPE_CHECKING
from uuid import UUID

import anyio

from kuu.exceptions import NotConnected, TaskError
from kuu.results.base import result_key

if TYPE_CHECKING:
	from kuu.app import Kuu
	from kuu.message import Message


class TaskHandle[Res]:
	"""
	Handle to an enqueued task, used to fetch the result later.

	Returned by `task.q(...)`. The type parameter `Res` is inferred from the
	wrapped task's return annotation, so `await handle.result()` is typed.
	"""

	__slots__ = ("message", "app")

	def __init__(self, message: Message, app: Kuu) -> None:
		self.message = message
		self.app = app

	@property
	def id(self) -> UUID:
		return self.message.id

	@property
	def key(self) -> str:
		return result_key(self.message)

	async def _poll_once(self) -> tuple[bool, Res]:
		if self.app.results is None:
			raise NotConnected("no result backend configured on Kuu(results=...)")
		r = await self.app.results.get(self.key)
		if r is None:
			return False, typing.cast(Res, None)
		if r.status == "error":
			raise TaskError(r.error or "task failed")
		return True, typing.cast(Res, self.app.results.decode(r))

	async def result(self, timeout: float | None = None, poll: float = 0.2) -> Res:
		"""
		Block until the task result is available, then return it.

		- `timeout`: max seconds to wait. `None` waits forever.
		- `poll`: seconds between backend polls.

		Raises `NotConnected` when no result backend is configured,
		`TaskError` when the task finished with an error status, and
		`TimeoutError` when `timeout` elapses before a result arrives.
		"""

		async def _loop() -> Res:
			while True:
				done, value = await self._poll_once()
				if done:
					return value
				await anyio.sleep(poll)

		if timeout is None:
			return await _loop()
		with anyio.fail_after(timeout):
			return await _loop()
