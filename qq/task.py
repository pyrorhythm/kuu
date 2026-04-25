from __future__ import annotations

import inspect
import typing
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from uuid import UUID

import anyio
import orjson
from pydantic import BaseModel

from qq.exceptions import NotConnected, TaskError
from qq.message import Message

if TYPE_CHECKING:
	from qq.app import Q


class TaskHandle[Res]:
	__slots__ = ("message", "app", "_result_model")

	def __init__(
		self,
		message: Message,
		app: Q,
		result_model: type[BaseModel] | None = None,
	) -> None:
		self.message = message
		self.app = app
		self._result_model = result_model

	@property
	def id(self) -> UUID:
		return self.message.id

	@property
	def key(self) -> str:
		return self.message.headers.get("idempotency_key") or str(self.message.id)

	def _decode(self, raw: bytes | None) -> Res:
		data = orjson.loads(raw) if raw else None
		if self._result_model is not None and data is not None:
			return typing.cast("Res", self._result_model.model_validate(data))
		return typing.cast("Res", data)

	async def _poll_once(self) -> tuple[bool, Res]:
		if self.app.results is None:
			raise NotConnected("no result backend configured on Q(results=...)")
		r = await self.app.results.get(self.key)
		if r is None:
			return False, typing.cast("Res", None)
		if r.status == "error":
			raise TaskError(r.error or "task failed")
		return True, self._decode(r.value)

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


@dataclass
class Task[**P, Res]:
	name: str
	queue: str
	fn: Callable[P, Res]
	args_model: type[BaseModel] | None
	result_model: type[BaseModel] | None = None
	max_attempts: int = 5
	timeout: float | None = None
	_bound_app: Q | None = field(default=None, repr=False)

	def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Res:
		return self.fn(*args, **kwargs)

	async def q(self, *args: P.args, **kwargs: P.kwargs) -> TaskHandle[Res]:
		if self._bound_app is None:
			raise RuntimeError(f"task {self.name!r} not bound to an app")
		return await self._bound_app._enqueue_task(self)(*args, **kwargs)


def infer_args_model(fn: Callable[..., Any]) -> type[BaseModel] | None:
	try:
		hints = typing.get_type_hints(fn)
	except Exception:
		hints = {}
	sig = inspect.signature(fn)
	for name, p in sig.parameters.items():
		ann = hints.get(name, p.annotation)
		if isinstance(ann, type) and issubclass(ann, BaseModel):
			return ann
	return None


def infer_result_model(fn: Callable[..., Any]) -> type[BaseModel] | None:
	try:
		hints = typing.get_type_hints(fn)
	except Exception:
		return None
	ann = hints.get("return")
	if isinstance(ann, type) and issubclass(ann, BaseModel):
		return ann
	return None


class Registry:
	def __init__(self) -> None:
		self._by_name: dict[str, Task[Any, Any]] = {}

	def add(self, task: Task[Any, Any]) -> None:
		if task.name in self._by_name:
			raise ValueError(f"duplicate task name: {task.name}")
		self._by_name[task.name] = task

	def get(self, name: str) -> Task[Any, Any] | None:
		return self._by_name.get(name)

	def names(self) -> list[str]:
		return list(self._by_name)

	def queues(self) -> set[str]:
		return {t.queue for t in self._by_name.values()}
