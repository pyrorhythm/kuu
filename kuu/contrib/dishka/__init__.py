from __future__ import annotations

__all__ = (
	"CONTAINER_STATE_KEY",
	"AnyContainer",
	"FromDishka",
	"KuuDishkaMiddleware",
	"current_container",
	"from_contextvar",
	"inject",
	"setup_dishka",
)

import functools
import inspect
from collections.abc import Callable
from contextvars import ContextVar
from inspect import Parameter
from typing import Any, overload

from dishka import AsyncContainer, Container
from dishka.entities.component import DEFAULT_COMPONENT, Component
from dishka.entities.key import DependencyKey
from dishka.integrations.base import default_parse_dependency, wrap_injection

from kuu import Kuu
from kuu.context import Context
from kuu.middleware.base import Next

type AnyContainer = AsyncContainer | Container
type ContainerGetter = Callable[..., AnyContainer]


class _FromDishkaMarker:
	__slots__ = ("component",)

	def __init__(self, component: Component = DEFAULT_COMPONENT) -> None:
		self.component = component

	def __repr__(self) -> str:  # pragma: no cover - cosmetic
		return f"FromDishka(component={self.component!r})"


def FromDishka(component: Component = DEFAULT_COMPONENT) -> Any:
	return _FromDishkaMarker(component)


def _parse_dependency(parameter: Parameter, hint: Any) -> DependencyKey | None:
	default = parameter.default
	if isinstance(default, _FromDishkaMarker):
		return DependencyKey(hint, default.component)
	return default_parse_dependency(parameter, hint)


CONTAINER_STATE_KEY = "dishka_container"

_request_container: ContextVar[AnyContainer | None] = ContextVar(
	"kuu_dishka_request_container",
	default=None,
)


def current_container() -> AnyContainer:
	container = _request_container.get()
	if container is None:
		raise LookupError(
			"No Dishka REQUEST container in the current context. "
			"Add KuuDishkaMiddleware to the app's middleware chain "
			"(e.g. via setup_dishka(app, container))."
		)
	return container


def from_contextvar(var: ContextVar[Any]) -> ContainerGetter:
	def _getter(*_: Any, **__: Any) -> AnyContainer:
		container = var.get()
		if container is None:
			raise LookupError(
				f"ContextVar {var.name!r} holds no Dishka container; "
				"it must be set before the task runs."
			)
		return container

	return _getter


_container_getter: ContainerGetter = from_contextvar(_request_container)


@overload
def inject[**P, R](func: Callable[P, R], /) -> Callable[P, R]: ...


@overload
def inject[**P, R](
	*,
	container_getter: ContainerGetter | None = ...,
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def inject[**P, R](
	func: Callable[P, R] | None = None,
	*,
	container_getter: ContainerGetter | None = None,
) -> Callable[P, R] | Callable[[Callable[P, R]], Callable[P, R]]:
	if func is None:
		return functools.partial(inject, container_getter=container_getter)
	# pyrefly: ignore [no-matching-overload]
	return wrap_injection(
		func=func,
		container_getter=container_getter or _container_getter,
		is_async=inspect.iscoroutinefunction(func),
		remove_depends=True,
		parse_dependency=_parse_dependency,
	)


class KuuDishkaMiddleware:
	def __init__(
		self,
		container: AsyncContainer | None = None,
		*,
		sync_container: Container | None = None,
		context_var: ContextVar[Any] | None = None,
	) -> None:
		if container is None and sync_container is None:
			raise ValueError(
				"KuuDishkaMiddleware needs at least one of `container` "
				"(AsyncContainer) or `sync_container` (Container)."
			)
		self._async_app = container
		self._sync_app = sync_container
		self._var = context_var if context_var is not None else _request_container

	async def __call__(self, ctx: Context, call_next: Next) -> Any:
		if ctx.phase != "process":
			return await call_next()

		if ctx.task and ctx.task.blocking:
			if self._sync_app is None:
				raise LookupError(
					f"task {getattr(ctx.task, 'task_name', '?')!r} needs a sync "
					"Dishka container (it is blocking=True), but KuuDishkaMiddleware "
					"was created without `sync_container=`."
				)
			return await self._with_sync(ctx, call_next)
		if self._async_app is None:
			raise LookupError(
				f"task {getattr(ctx.task, 'task_name', '?')!r} needs an async "
				"Dishka container, but KuuDishkaMiddleware was created without "
				"`container=`."
			)
		return await self._with_async(ctx, call_next)

	async def _with_async(self, ctx: Context, call_next: Next) -> Any:
		assert self._async_app is not None
		async with self._async_app() as request:
			token = self._var.set(request)
			ctx.state[CONTAINER_STATE_KEY] = request
			try:
				return await call_next()
			finally:
				self._var.reset(token)

	async def _with_sync(self, ctx: Context, call_next: Next) -> Any:
		assert self._sync_app is not None
		with self._sync_app() as request:
			token = self._var.set(request)
			ctx.state[CONTAINER_STATE_KEY] = request
			try:
				return await call_next()
			finally:
				self._var.reset(token)


def setup_dishka(
	app: Kuu,
	container: AsyncContainer | None = None,
	*,
	sync_container: Container | None = None,
) -> KuuDishkaMiddleware:
	mw = KuuDishkaMiddleware(container, sync_container=sync_container)
	app.middleware.insert(0, mw)
	return mw
