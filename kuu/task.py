from __future__ import annotations

import inspect
from typing import Any, Awaitable, Mapping, TYPE_CHECKING, cast

if TYPE_CHECKING:
	from kuu._types import _FnAny
	from kuu.app import Kuu
	from kuu.handle import TaskHandle


async def if_async[T](arg: T | Awaitable[T]) -> T:
	return await cast(Awaitable[T], arg) if inspect.iscoroutine(arg) else cast(T, arg)


class Task[**P, Res]:
	task_name: str
	task_queue: str
	original_func: _FnAny[P, Res]
	max_attempts: int = 5
	timeout: float | None = None
	blocking: bool = False
	_bound_app: Kuu | None = None

	def __init__(
			self,
			manager: Kuu,
			original_func: _FnAny[P, Res],
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

	async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Res:
		return await if_async(self.original_func(*args, **kwargs))

	async def q(self, *args: P.args, **kwargs: P.kwargs) -> TaskHandle[Res]:
		"""
		Enqueue the task and return a `TaskHandle` to poll for the result.

		Raises `RuntimeError` if the task is not bound to an app (only
		happens when constructing `Task` manually instead of via `@app.task`).
		"""
		if self._bound_app is None:
			raise RuntimeError(f"task {self.task_name!r} not bound to an app")
		return await self._bound_app._enqueue_task(self)(*args, **kwargs)
