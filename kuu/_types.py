from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Any, Callable, Concatenate, Coroutine, Protocol

if TYPE_CHECKING:
	from kuu.task import Task


import inspect
import logging
import types
import typing
from typing import TYPE_CHECKING

from pydantic import BaseModel

from kuu.message import Payload

if TYPE_CHECKING:
	pass

log = logging.getLogger("kuu.worker")


def _unwrap_model(ann: Any) -> type[BaseModel] | None:
	"""Extract a concrete BaseModel subclass from a type annotation.

	Handles plain ``Model``, ``Model | None``, ``list[Model]``,
	and ``dict[K, Model]``. Returns ``None`` when the annotation does
	not involve a BaseModel.
	"""
	if isinstance(ann, type) and issubclass(ann, BaseModel):
		return ann

	origin = typing.get_origin(ann)
	# X | None  (Python 3.12+ union syntax)
	if origin is types.UnionType:
		for arg in typing.get_args(ann):
			if isinstance(arg, type) and issubclass(arg, BaseModel):
				return arg
		return None

	# list[Model]
	if origin is list:
		args = typing.get_args(ann)
		inner = args[0] if args else None
		if isinstance(inner, type) and issubclass(inner, BaseModel):
			return inner
		# list[Model | None] etc.
		return _unwrap_model(inner) if inner is not None else None

	# dict[K, Model]
	if origin is dict:
		args = typing.get_args(ann)
		val = args[1] if len(args) > 1 else None
		if isinstance(val, type) and issubclass(val, BaseModel):
			return val
		return _unwrap_model(val) if val is not None else None

	return None


def _coerce_value(value: Any, ann: Any, model: type[BaseModel]) -> Any:
	"""Coerce a single runtime value according to its annotation.

	- ``Model`` or ``Model | None``: dict → model, None stays None, model stays model.
	- ``list[Model]``: list of dicts → list of models.
	- ``dict[K, Model]``: dict whose values are dicts → dict with model values.
	"""
	origin = typing.get_origin(ann)

	# X | None
	if origin is types.UnionType:
		if value is None:
			return None
		if isinstance(value, model):
			return value
		if isinstance(value, dict):
			return model.model_validate(value)
		return value

	# list[Model]
	if origin is list:
		if not isinstance(value, list):
			return value
		return [model.model_validate(v) if isinstance(v, dict) else v for v in value]

	# dict[K, Model]
	if origin is dict:
		if not isinstance(value, dict):
			return value
		return {k: model.model_validate(v) if isinstance(v, dict) else v for k, v in value.items()}

	# Plain Model
	if isinstance(value, model):
		return value
	if isinstance(value, dict):
		return model.model_validate(value)
	return value


def _coerce_payload(func: Any, payload: Payload) -> Payload:
	"""Reconstruct BaseModel parameters that arrived as plain dicts after JSON round-trip.

	When a message traverses a JSON-based broker, Pydantic model arguments
	are flattened to dicts by ``model_dump(mode="json")``. The worker
	needs to coerce them back so the task function receives proper model
	instances instead of raw dicts.

	Handles plain models, ``Model | None``, ``list[Model]``, and ``dict[K, Model]``.
	"""
	try:
		sig = inspect.signature(func)
		hints = typing.get_type_hints(func)
	except Exception:
		return payload

	bound = sig.bind_partial(*payload.args, **payload.kwargs)
	bound.apply_defaults()

	new_kwargs = dict(payload.kwargs)
	new_args = list(payload.args)
	params = list(sig.parameters.values())

	for i, (name, value) in enumerate(bound.arguments.items()):
		ann = hints.get(name)
		if ann is None:
			continue

		model = _unwrap_model(ann)
		if model is None:
			continue

		coerced = _coerce_value(value, ann, model)

		p = params[i] if i < len(params) else None
		if p is not None and p.kind in (
			inspect.Parameter.POSITIONAL_ONLY,
			inspect.Parameter.POSITIONAL_OR_KEYWORD,
		):
			if name in new_kwargs:
				new_kwargs[name] = coerced
			elif i < len(new_args):
				new_args[i] = coerced
		elif name in new_kwargs:
			new_kwargs[name] = coerced

	return Payload(args=tuple(new_args), kwargs=new_kwargs)


class Connectable(Protocol):
	async def connect(self) -> None: ...


type _FnAsync[**P, R] = Callable[P, Coroutine[Any, Any, R]]
type _Fn[**P, R] = Callable[P, R] | Callable[P, Coroutine[Any, Any, R]]
type _FnSingle[P, R] = Callable[[P], R]
type _Wrap[**P, R] = _FnSingle[_Fn[P, R], Task[P, R]]


def _ensure_connected[T: Connectable, **P, R](
	fn: Callable[Concatenate[T, P], Coroutine[Any, Any, R]],
) -> Callable[Concatenate[T, P], Coroutine[Any, Any, R]]:
	@functools.wraps(fn)
	async def wrapper(self: T, *args: P.args, **kwargs: P.kwargs) -> R:
		await self.connect()
		return await fn(self, *args, **kwargs)

	return wrapper
