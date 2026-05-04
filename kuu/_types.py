from __future__ import annotations

import dataclasses
import functools
import inspect
import types
import typing
from types import NoneType
from typing import (
	TYPE_CHECKING,
	Any,
	Callable,
	Concatenate,
	Coroutine,
	Protocol,
	TypeIs,
)

from pydantic import BaseModel

if TYPE_CHECKING:
	from kuu.task import Task

import logging

from kuu.message import Payload

log = logging.getLogger("kuu.worker")


_Incomplete = (Any, NoneType)


def _origin(ann: Any) -> Any:
	o = typing.get_origin(ann)
	if o is types.UnionType:
		return o
	# typing.Union
	if o is typing.Union:
		return o
	return o


def _is_incomplete(ann: Any) -> bool:
	if ann is None or ann is Any:
		return True
	o = typing.get_origin(ann)
	if o is None and ann in _Incomplete:
		return True
	if isinstance(ann, str):
		return True
	return False


def _resolve_field_types(cls: type[Any]) -> dict[str, Any]:
	try:
		hints = typing.get_type_hints(cls, include_extras=True)
	except Exception:
		hints = {}

	if _is_pydantic_model(cls):
		return {name: hints.get(name, field.annotation) for name, field in cls.model_fields.items()}

	if dataclasses.is_dataclass(cls):
		return {f.name: hints.get(f.name, f.type) for f in dataclasses.fields(cls)}

	return {}


def _model_construct(cls: type, data: dict[str, Any]) -> Any:
	if dataclasses.is_dataclass(cls):
		field_types = _resolve_field_types(cls)
		coerced = _coerce_dict(data, field_types)
		return cls(**coerced)

	if _is_pydantic_model(cls):
		field_types = _resolve_field_types(cls)
		coerced = _coerce_dict(data, field_types)
		return cls.model_validate(coerced) if _needs_validation(cls) else cls(**coerced)

	return cls(**data)


def _needs_validation(cls: type) -> bool:
	if not _is_pydantic_model(cls):
		return False
	return (
		bool(cls.__pydantic_decorators__.validators)
		if hasattr(cls, "__pydantic_decorators__")
		else True
	)


def _is_pydantic_model(cls: type) -> TypeIs[type[BaseModel]]:
	return hasattr(cls, "model_validate") and hasattr(cls, "model_fields")


def _is_structlike(cls: type) -> bool:
	return _is_pydantic_model(cls) or dataclasses.is_dataclass(cls)


def _coerce(value: Any, ann: Any) -> Any:
	"""Recursively coerce *value* to match *ann* (the type annotation).

	Algorithm:
	  1. Resolve the annotation — strip Optional unions, get origin.
	  2. If concrete struct-like type → construct from dict / recurse fields.
	  3. If ``list[Inner]`` → recurse each element against Inner.
	  4. If ``dict[K, V]`` → recurse each value against V.
	  5. If ``X | None`` → recurse value against X (or passthrough None).
	  6. If annotation incomplete → try runtime deduction.
	  7. Otherwise return value unchanged.
	"""
	if ann is inspect.Parameter.empty or ann is None:
		return _coerce_runtime(value)

	if _is_incomplete(ann):
		return _coerce_runtime(value)

	# Unpack union (X | None, Union[X, None], etc.)
	non_none, has_none = _flatten_union(ann)
	if has_none:
		if value is None:
			return None
		if len(non_none) == 1:
			return _coerce(value, non_none[0])
		# multi-type union: try each until one fits
		for branch in non_none:
			try:
				return _coerce(value, branch)
			except (TypeError, ValueError, KeyError):
				continue
		return value

	origin = typing.get_origin(ann)

	# list[X]
	if origin is list:
		inner = typing.get_args(ann)[0] if typing.get_args(ann) else Any
		if not isinstance(value, list):
			return value
		return [_coerce(v, inner) for v in value]

	# dict[K, V]
	if origin is dict:
		args = typing.get_args(ann)
		val_ann = args[1] if len(args) > 1 else Any
		if not isinstance(value, dict):
			return value
		return {k: _coerce(v, val_ann) for k, v in value.items()}

	# set[X]
	if origin is set:
		inner = typing.get_args(ann)[0] if typing.get_args(ann) else Any
		if not isinstance(value, (set, list, tuple)):
			return value
		return {_coerce(v, inner) for v in value}

	# tuple[X, ...] or tuple[X, Y, Z]
	if origin is tuple:
		args = typing.get_args(ann)
		if not args:
			return value
		if not isinstance(value, (tuple, list)):
			return value
		if len(args) == 2 and args[1] is ...:
			return tuple(_coerce(v, args[0]) for v in value)
		return tuple(_coerce(v, a) for v, a in zip(value, args))

	# Concrete struct-like type (pydantic model, dataclass, etc.)
	if isinstance(ann, type) and _is_structlike(ann):
		if isinstance(value, ann):
			return value
		if isinstance(value, dict):
			return _model_construct(ann, value)
		return value

	# Primitives with simple coercion
	if isinstance(ann, type):
		if isinstance(value, ann):
			return value
		# int/float/str/bool from compatible types
		if ann is int and isinstance(value, (bool, int)):
			return value  # booleans are ints in Python, preserve
		if ann is float and isinstance(value, (int, float)) and not isinstance(value, bool):
			return float(value)
		if ann is str and isinstance(value, str):
			return value

	return value


def _coerce_runtime(value: Any) -> Any:
	"""Best-effort coercion when no annotation is available.

	Recursively descends dicts-looking-like-models and lists.
	"""
	if isinstance(value, dict):
		return {k: _coerce_runtime(v) for k, v in value.items()}
	if isinstance(value, list):
		return [_coerce_runtime(v) for v in value]
	return value


def _coerce_dict(data: dict[str, Any], field_types: dict[str, Any]) -> dict[str, Any]:
	"""Given a dict of field-name→annotation, coerce each matching key."""
	out: dict[str, Any] = {}
	for k, v in data.items():
		if k in field_types:
			out[k] = _coerce(v, field_types[k])
		else:
			out[k] = v
	return out


def _flatten_union(ann: Any) -> tuple[list[Any], bool]:
	"""Return (non-None branches, has_None branch)."""
	origin = typing.get_origin(ann)
	if origin is types.UnionType or origin is typing.Union:
		args = typing.get_args(ann)
		non_none = [a for a in args if a is not type(None)]
		has_none = any(a is type(None) for a in args)
		return non_none, has_none
	return [], False


# ── payload coercion ──────────────────────────────────────────────────────


def _coerce_payload(func: Any, payload: Payload) -> Payload:
	"""Coerce every argument in *payload* according to *func*'s type annotations.

	Annotations are the source of truth. When an annotation is missing or
	incomplete (``Any``, ``None``, unresolved forward ref), the function
	attempts runtime type deduction.

	Returns a new ``Payload`` with coerced values.
	"""
	try:
		sig = inspect.signature(func)
		hints = typing.get_type_hints(func, include_extras=True)
	except Exception:
		return payload

	bound = sig.bind(*payload.args, **payload.kwargs)
	bound.apply_defaults()

	new_kwargs = dict(payload.kwargs)
	new_args = list(payload.args)
	params = list(sig.parameters.values())

	for i, (name, value) in enumerate(bound.arguments.items()):
		ann = hints.get(name, inspect.Parameter.empty)
		if ann is inspect.Parameter.empty:
			# No annotation at all — try runtime deduction
			ann = None

		coerced = _coerce(value, ann)

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
