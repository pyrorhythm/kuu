from __future__ import annotations

import inspect
from datetime import datetime, timezone
from typing import Any, overload

from pydantic_core import ArgsKwargs

from ._import import object_fqn
from ._types import _Fn, _FnAsync, _Wrap
from .brokers.base import Broker
from .context import Context
from .events import Events
from .handle import TaskHandle
from .message import Message
from .middleware.base import Middleware, run_chain
from .registry import Registry
from .results.base import ResultBackend
from .serializers.base import Serializer
from .serializers.json import JSONSerializer
from .task import Task


class Q:
	def __init__(
		self,
		broker: Broker,
		default_queue: str = "default",
		middleware: list[Middleware] | None = None,
		results: ResultBackend | None = None,
		serializer: Serializer = JSONSerializer(),
	):
		self.broker = broker
		self.results = results
		self.serializer = serializer
		self.default_queue = default_queue
		self.middleware: list[Middleware] = list(middleware or [])
		self.registry = Registry()
		self.events = Events()

	@overload
	def task[**P, R](
		self,
		name: str | None = ...,
		/,
		queue: str | None = ...,
		max_attempts: int = ...,
		timeout: float | None = ...,
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
		**labels: Any,
	) -> _Wrap[P, R] | Task[P, R]:

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
		args: ArgsKwargs,
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
		args: Any,
		not_before: datetime | None,
	) -> None:
		ctx = Context(app=self, message=msg, phase="enqueue", task=task, args=args)

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
			payload = ArgsKwargs(args, kwargs)
			msg = self._build_message(
				task.name,
				task,
				payload,
				queue=queue,
				not_before=not_before,
				headers=headers,
				max_attempts=max_attempts,
			)
			await self._dispatch(msg, task, payload, not_before)
			return TaskHandle[Res](message=msg, app=self)

		return _

	async def enqueue_by_name(
		self,
		task: str,
		args: ArgsKwargs = ArgsKwargs(()),
		queue: str | None = None,
		not_before: datetime | None = None,
		headers: dict[str, str] | None = None,
		max_attempts: int | None = None,
	) -> TaskHandle[Any]:
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
		await self._dispatch(msg, t, args, not_before)
		return TaskHandle[Any](message=msg, app=self)
