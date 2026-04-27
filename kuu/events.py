from __future__ import annotations

import inspect
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable
from typing import Any

log = logging.getLogger("kuu.events")

Handler = Callable[..., Awaitable[None] | None]


class Signal:
	__slots__ = ("name", "_handlers")

	def __init__(self, name: str):
		self.name = name
		self._handlers: list[Handler] = []

	def connect(self, handler: Handler) -> Handler:
		self._handlers.append(handler)
		return handler

	def disconnect(self, handler: Handler) -> None:
		try:
			self._handlers.remove(handler)
		except ValueError:
			pass

	async def send(self, *args: Any, **kw: Any) -> None:
		for h in self._handlers:
			try:
				r = h(*args, **kw)
				if inspect.isawaitable(r):
					await r
			except Exception:
				log.exception("event handler %s failed on %s", h, self.name)


class Events:
	def __init__(self) -> None:
		self._signals: dict[str, Signal] = defaultdict(lambda: None)  # type: ignore[arg-type]
		for n in (
			"task_enqueued",
			"task_received",
			"task_started",
			"task_succeeded",
			"task_failed",
			"task_retried",
			"task_dead",
			"worker_heartbeat",
		):
			self._signals[n] = Signal(n)

	def __getattr__(self, name: str) -> Signal:
		sig = self._signals.get(name)
		if sig is None:
			raise AttributeError(name)
		return sig
