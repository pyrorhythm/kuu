from __future__ import annotations

import inspect
from datetime import datetime, timedelta
from typing import Any, overload

from kuu._import import object_fqn
from kuu._types import _FnAny, _FnAsync, _Wrap
from kuu._util import utcnow
from kuu.brokers.base import Broker
from kuu.context import Context
from kuu.events import Events
from kuu.handle import TaskHandle
from kuu.message import Message, Payload
from kuu.middleware.base import Middleware, run_chain
from kuu.registry import Registry
from kuu.results.base import ResultBackend
from kuu.scheduler.schedule import Schedule
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
		"""Core app, which behaves as a main entrypoint for tasks

		:param broker: t for enqueue and consume.
		:param default_queue: fallback queue when `@app.task(queue=...)` is omitted.
		:param middleware: optional middleware chain applied around every task.
		:param results: optional result backend. Result-persistence policy
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
	def task[**P, R](self, func: _FnAny[P, R], /) -> Task[P, R]: ...

	def task[**P, R](
		self,
		name_or_func: str | _FnAny[P, R] | None = None,
		/,
		queue: str | None = None,
		max_attempts: int = 5,
		timeout: float | None = None,
		blocking: bool = False,
		**labels: Any,
	) -> _Wrap[P, R] | Task[P, R]:
		"""Register function as a task

		Accepts the bare function (`@app.task`) or a parametrized decorator
		(`@app.task(queue=..., max_attempts=...)`).

		:param queue: destination queue; defaults to `Kuu.default_queue`.
		:param max_attempts: retry budget before the task is declared dead.
		:param timeout: per-run wall-clock limit in seconds; `None` means no limit.
		:param blocking: when `True`, offloads a sync function to a thread.
		:param labels: arbitrary metadata attached to the task.
		"""

		def _get_wrap(
			_name: str | None = None,
		):
			def _wrap(_func: _FnAny[P, R]) -> Task[P, R]:
				t: Task[P, R] = Task(
					manager=self,
					original_func=_func,
					task_name=_name or object_fqn(_func),
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
		elif name_or_func is None or isinstance(name_or_func, str):
			return _get_wrap(name_or_func)

		raise TypeError(type(name_or_func))

	def every[**P, R](
		self,
		interval: timedelta,
		args: Payload = Payload(),
		*,
		sched_id: str | None = None,
		queue: str | None = None,
		headers: dict[str, str] | None = None,
		max_attempts: int | None = None,
	) -> _Wrap[P, R]:
		"""Register function as a scheduled task with specified interval `timedelta`.

		:param interval: timedelta, in which task would be scheduled
		:param args: args for the task to be executed with
		:param sched_id: optional, specific sched_id for scheduler
		:param queue: destination queue; defaults to `Kuu.default_queue`
		:param headers: arbitrary metadata attached to the scheduled task
		:param max_attempts: retry budget before the task is declared dead
		"""

		def wrap(fn: _FnAny[P, R]) -> Task[P, R]:
			if not isinstance(fn, Task):
				fn = self.task(fn)
			self.schedule.add_every(
				interval,
				fn,
				args,
				id=sched_id,
				queue=queue,
				headers=headers,
				max_attempts=max_attempts,
			)
			return fn

		return wrap

	def sched[**P, R](
		self,
		sched: Schedule,
		args: Payload = Payload(),
		*,
		sched_id: str | None = None,
		queue: str | None = None,
		headers: dict[str, str] | None = None,
		max_attempts: int | None = None,
	) -> _Wrap[P, R]:
		"""Register function as a task with `Schedule`

		:param sched: schedule to run task with
		:param args: args for the task to be executed with
		:param sched_id: optional, specific sched_id for scheduler
		:param queue: destination queue; defaults to `Kuu.default_queue`
		:param headers: arbitrary metadata attached to the scheduled task
		:param max_attempts: retry budget before the task is declared dead
		"""

		def wrap(fn: _FnAny[P, R]) -> Task[P, R]:
			if not isinstance(fn, Task):
				fn = self.task(fn)
			self.schedule.add_schedule(
				sched,
				fn,
				args,
				id=sched_id,
				queue=queue,
				headers=headers,
				max_attempts=max_attempts,
			)
			return fn

		return wrap

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
			if not_before is not None and not_before > utcnow():
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
		"""Enqueue a task by its registered dotted name.

		:param task: registered task name (e.g. `"myapp.tasks:charge"`).
		:param args: positional and keyword arguments passed to the task.
		:param queue: destination queue override.
		:param not_before: earliest UTC time the task may run.
		:param headers: custom message headers.
		:param max_attempts: retry budget override.
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
