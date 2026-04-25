from __future__ import annotations

import typing
from typing import TYPE_CHECKING
from uuid import UUID

import anyio

from kuu.exceptions import NotConnected, TaskError

if TYPE_CHECKING:
	from kuu.app import Kuu
	from kuu.message import Message


class TaskHandle[Res]:
	__slots__ = ("message", "app")

	def __init__(self, message: Message, app: Kuu) -> None:
		self.message = message
		self.app = app

	@property
	def id(self) -> UUID:
		return self.message.id

	@property
	def key(self) -> str:
		return self.message.headers.get("idempotency_key") or str(self.message.id)

	async def _poll_once(self) -> tuple[bool, Res]:
		if self.app.results is None:
			raise NotConnected("no result backend configured on Kuu(results=...)")
		r = await self.app.results.get(self.key)
		if r is None:
			return False, typing.cast("Res", None)
		if r.status == "error":
			raise TaskError(r.error or "task failed")
		return True, typing.cast("Res", self.app.results.decode(r))

	async def result(self, timeout: float | None = None, poll: float = 0.2) -> Res:
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
