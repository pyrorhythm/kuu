from __future__ import annotations

import inspect
import sys
from dataclasses import field
from typing import TYPE_CHECKING, Any, Awaitable, Mapping, cast

if TYPE_CHECKING:
	from kuu._types import _Fn
	from kuu.app import Kuu
	from kuu.handle import TaskHandle


async def if_async[T](arg: T | Awaitable[T]) -> T:
	return await cast(Awaitable[T], arg) if inspect.iscoroutine(arg) else cast(T, arg)


class Task[**P, Res]:
	name: str
	task_queue: str
	original_func: _Fn[P, Res]
	max_attempts: int = 5
	timeout: float | None = None
	blocking: bool = False
	_bound_app: Kuu | None = field(default=None, repr=False)

	def __init__(
		self,
		manager: Kuu,
		original_func: _Fn[P, Res],
		task_name: str,
		task_queue: str,
		task_labels: Mapping[str, Any],
		max_attempts: int = 5,
		timeout: float | None = None,
		blocking: bool = False,
	) -> None:
		self._bound_app = manager

		self.original_func = original_func

		self.task_name = task_name
		self.task_labels = task_labels
		self.task_queue = task_queue

		self.max_attempts = max_attempts
		self.timeout = timeout
		self.blocking = blocking

		if blocking and inspect.iscoroutinefunction(original_func):
			raise TypeError(
				f"task {task_name!r}: blocking=True is for sync functions; "
				"async functions should not be offloaded to a thread"
			)

		new_name = f"{self.original_func.__name__}_qq"
		self.original_func.__name__ = new_name
		if hasattr(self.original_func, "__qualname__"):
			original_qualname = self.original_func.__qualname__.rsplit(".")
			original_qualname[-1] = new_name
			new_qualname = ".".join(original_qualname)
			self.original_func.__qualname__ = new_qualname
		setattr(sys.modules[original_func.__module__], new_name, original_func)

	async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Res:
		return await if_async(self.original_func(*args, **kwargs))

	async def q(self, *args: P.args, **kwargs: P.kwargs) -> TaskHandle[Res]:
		if self._bound_app is None:
			raise RuntimeError(f"task {self.name!r} not bound to an app")
		return await self._bound_app._enqueue_task(self)(*args, **kwargs)
