from __future__ import annotations

import functools
import inspect
import typing as t
from types import NoneType
from typing import Any, Callable, Concatenate, Coroutine, Protocol, TYPE_CHECKING

from adaptix import Retort
from adaptix.load_error import LoadError

if TYPE_CHECKING:
	from kuu.task import Task

import logging

from kuu.message import Payload

log = logging.getLogger("kuu.worker")

_retort = Retort(strict_coercion=True)


def _coerce(value: Any, ann: Any) -> Any:
	if ann is inspect.Parameter.empty or ann is None or _is_incomplete(ann):
		return _coerce_runtime(value)
	if isinstance(ann, type) and isinstance(value, ann):
		return value

	origin = t.get_origin(ann)
	args = t.get_args(ann)

	if origin is list:
		inner = args[0] if args else Any
		if not isinstance(value, list):
			return value
		return [_coerce(v, inner) for v in value]

	if origin is dict:
		val_ann = args[1] if len(args) > 1 else Any
		if not isinstance(value, dict):
			return value
		return {k: _coerce(v, val_ann) for k, v in value.items()}

	if origin is set:
		inner = args[0] if args else Any
		if not isinstance(value, (set, list, tuple)):
			return value
		return {_coerce(v, inner) for v in value}

	if origin is tuple:
		if not args:
			return value
		if not isinstance(value, (tuple, list)):
			return value
		if len(args) == 2 and args[1] is ...:
			return tuple(_coerce(v, args[0]) for v in value)
		return tuple(_coerce(v, a) for v, a in zip(value, args))

	try:
		return _retort.load(value, ann)
	except LoadError:
		return value


def _coerce_runtime(value: Any) -> Any:
	if isinstance(value, dict):
		return {k: _coerce_runtime(v) for k, v in value.items()}
	if isinstance(value, list):
		return [_coerce_runtime(v) for v in value]
	return value


def _is_incomplete(ann: Any) -> bool:
	if ann is Any or ann is None:
		return True
	o = t.get_origin(ann)
	if o is None and ann in (Any, NoneType):
		return True
	if isinstance(ann, str):
		return True
	return False


def _coerce_payload(func: Any, payload: Payload) -> Payload:
	try:
		sig = inspect.signature(func)
		hints = t.get_type_hints(func, include_extras=True)
	except Exception:
		return payload

	bound = sig.bind(*payload.args, **payload.kwargs)
	bound.apply_defaults()

	new_kwargs = dict(payload.kwargs)
	new_args = list(payload.args)

	positional_idx = 0
	for p in sig.parameters.values():
		if p.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
			continue
		name = p.name
		if name not in bound.arguments:
			continue

		ann = hints.get(name, inspect.Parameter.empty)
		coerced = _coerce(bound.arguments[name], ann)

		if p.kind in (
				inspect.Parameter.POSITIONAL_ONLY,
				inspect.Parameter.POSITIONAL_OR_KEYWORD,
		):
			if name in new_kwargs:
				new_kwargs[name] = coerced
			elif positional_idx < len(new_args):
				new_args[positional_idx] = coerced
			positional_idx += 1
		elif p.kind == inspect.Parameter.KEYWORD_ONLY and name in new_kwargs:
			new_kwargs[name] = coerced

	return Payload(args=tuple(new_args), kwargs=new_kwargs)


class Connectable(Protocol):
	async def connect(self) -> None: ...


type _FnAny[**P, R] = Callable[P, R] | Callable[P, Coroutine[Any, Any, R]]
type _FnAsync[**P, R] = Callable[P, Coroutine[Any, Any, R]]
type _FnSingle[P, R] = Callable[[P], R]
type _Wrap[**P, R] = _FnSingle[_FnAny[P, R], Task[P, R]]


def _ensure_connected[T: Connectable, **P, R](
		fn: Callable[Concatenate[T, P], Coroutine[Any, Any, R]],
) -> Callable[Concatenate[T, P], Coroutine[Any, Any, R]]:
	@functools.wraps(fn)
	async def wrapper(self: T, *args: P.args, **kwargs: P.kwargs) -> R:
		await self.connect()
		return await fn(self, *args, **kwargs)

	return wrapper
