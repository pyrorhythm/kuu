from __future__ import annotations

from typing import Any

from ..context import Context
from ..exceptions import RetryErr
from ..message import Message
from ..results.base import Result, ResultBackend
from .base import Next


class ResultsMiddleware:
	def __init__(
		self,
		backend: ResultBackend,
		ttl: float = 86400,
		key_header: str = "idempotency_key",
		replay: bool = True,
		store_errors: bool = True,
	):
		self.backend = backend
		self.ttl = ttl
		self.key_header = key_header
		self.replay = replay
		self.store_errors = store_errors

	def _key(self, msg: Message) -> str:
		return msg.headers.get(self.key_header) or str(msg.id)

	async def __call__(self, ctx: Context, call_next: Next) -> Any:
		if ctx.phase != "process":
			return await call_next()
		ser = ctx.app.serializer
		key = self._key(ctx.message)
		if self.replay:
			cached = await self.backend.get(key)
			if cached is not None and cached.status == "ok":
				return ser.unmarshal(cached.value) if cached.value else None
		try:
			result = await call_next()
		except RetryErr:
			raise
		except BaseException as e:
			if self.store_errors and ctx.message.attempt + 1 >= ctx.message.max_attempts:
				await self.backend.set(
					key,
					Result(status="error", error=f"{type(e).__name__}: {e}"),
					ttl=self.ttl,
				)
			raise
		value = ser.marshal(result) if result is not None else None
		await self.backend.set(key, Result(status="ok", value=value), ttl=self.ttl)
		return result
