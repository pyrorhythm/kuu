from __future__ import annotations

import inspect
from datetime import datetime, timezone
from typing import Any, overload

from kuu._import import object_fqn
from kuu._types import _Fn, _FnAsync, _Wrap
from kuu.brokers.base import Broker
from kuu.context import Context
from kuu.events import Events
from kuu.handle import TaskHandle
from kuu.message import Message, Payload
from kuu.middleware.base import Middleware, run_chain
from kuu.registry import Registry
from kuu.results.base import ResultBackend
from kuu.scheduler.scheduler import Scheduler
from kuu.task import Task


class Kuu:
	def __init__(
		self,
		broker: Broker,
		default_queue: str = "default",
		middleware: list[Middleware] | None = None,
		results: ResultBackend | None = None,
	) -> None:
		"""
		The Kuu app.

		- `broker`: transport for enqueue and consume.
		- `default_queue`: fallback queue when `@app.task(queue=...)` is omitted.
		- `middleware`: optional middleware chain applied around every task.
		- `results`: optional result backend. Result-persistence policy
		  (`ttl`, `replay`, `store_errors`) lives on the backend itself.
		"""

		self.broker = broker
		self.results = results
		self.default_queue = default_queue
		self.middleware: list[Middleware] = list(middleware or [])
		self.registry = Registry()
		self.events = Events()
		self.schedule = Scheduler(self)

	@overload
	def task[**P, R](
		self,
		name: str | None = ...,
		/,
		queue: str | None = ...,
		max_attempts: int = ...,
		timeout: float | None = ...,
		blocking: bool = ...,
		**labels: Any,
	) -> _Wrap[P, R]: ...

	@overload
	def task[**P, R](self, func: _Fn[P, R] = ..., /) -> Task[P, R]: ...

	def task[**P, R](
		self,
		name_or_func: str | _Fn[P, R] | None = None,
		/,
		queue: str | None = None,
		max_attempts: int = 5,
		timeout: float | None = None,
		blocking: bool = False,
		**labels: Any,
	) -> _Wrap[P, R] | Task[P, R]:
		"""
		Register a function as a task

		Supports direct registration and decorator usage
		        both **with** call parens () and **without**

		>>> @app.task
		... async def ....

		or

		>>> @app.task(queue='any-q', ...)
		... async def ....


		Args:
		    queue: override the default queue for this task; defaults
		        to None
		    max_attempts: maximum retry attempts; defaults to 5
		    timeout: maximum execution time in seconds; None means no
		        limit
		    blocking: whether the task runs in blocking mode;
		        defaults to False
		    **labels: extra metadata attached to the task

		Returns:
		    Task instance when called with a function, or a decorator
		    wrapper
		"""

		def _get_wrap(
			_name: str | None = None,
		):
			def _wrap(func: _Fn[P, R]) -> Task[P, R]:
				t: Task[P, R] = Task(
					manager=self,
					original_func=func,
					task_name=_name or object_fqn(func),
					task_queue=queue or self.default_queue,
					task_labels=labels,
					max_attempts=max_attempts,
					timeout=timeout,
					blocking=blocking,
				)
				self.registry.add(t)
				return t

			return _wrap

		if inspect.isfunction(name_or_func):
			func = name_or_func
			return _get_wrap()(func)

		name = name_or_func
		return _get_wrap(name)

	def _build_message(
		self,
		task_name: str,
		task: Task | None,
		args: Payload,
		queue: str | None,
		not_before: datetime | None,
		headers: dict[str, str] | None,
		max_attempts: int | None,
	) -> Message:
		return Message(
			task=task_name,
			queue=queue or (task.task_queue if task else self.default_queue),
			payload=args,
			headers=headers or {},
			max_attempts=(
				max_attempts if max_attempts is not None else (task.max_attempts if task else 5)
			),
			not_before=not_before,
		)

	async def _dispatch(
		self,
		msg: Message,
		task: Task[Any, Any] | None,
		not_before: datetime | None,
	) -> None:
		ctx = Context(app=self, message=msg, phase="enqueue", task=task)

		async def _terminal(_c: Context) -> None:
			if not_before is not None and not_before > datetime.now(timezone.utc):
				await self.broker.schedule(msg, not_before)
			else:
				await self.broker.enqueue(msg)
			await self.events.task_enqueued.send(msg)

		await run_chain(ctx, self.middleware, _terminal)

	def _enqueue_task[**P, Res](
		self,
		task: Task[P, Res],
		queue: str | None = None,
		not_before: datetime | None = None,
		headers: dict[str, str] | None = None,
		max_attempts: int | None = None,
	) -> _FnAsync[P, TaskHandle[Res]]:
		async def _(*args: P.args, **kwargs: P.kwargs) -> TaskHandle[Res]:
			payload = Payload(args=args, kwargs=kwargs)
			msg = self._build_message(
				task.task_name,
				task,
				payload,
				queue=queue,
				not_before=not_before,
				headers=headers,
				max_attempts=max_attempts,
			)
			await self._dispatch(msg, task, not_before)
			return TaskHandle[Res](message=msg, app=self)

		return _

	async def enqueue_by_name(
		self,
		task: str,
		args: Payload = Payload(),
		queue: str | None = None,
		not_before: datetime | None = None,
		headers: dict[str, str] | None = None,
		max_attempts: int | None = None,
	) -> TaskHandle[Any]:
		"""
		Enqueue a task by its registered name

		Args:
		    task: registered task name
		    args: positional and keyword arguments for the task. types should be correct,
		                        or else typeerror on worker would be raised
		    queue: override the queue. Defaults to None
		    not_before: earliest time the task may run. Defaults to
		        None
		    headers: custom message headers. Defaults to None
		    max_attempts: override max attempts. Defaults to None

		Returns:
		    Handle for the enqueued task
		"""

		t = self.registry.get(task)
		msg = self._build_message(
			task,
			t,
			args,
			queue=queue,
			not_before=not_before,
			headers=headers,
			max_attempts=max_attempts,
		)
		await self._dispatch(msg, t, not_before)
		return TaskHandle[Any](message=msg, app=self)
