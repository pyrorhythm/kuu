from __future__ import annotations

import logging
from typing import Any, Literal, overload

import orjson
from pydantic import BaseModel

from .base import Serializer


_msgspec_json: Any = None

try:
	from msgspec import json

	logging.getLogger(__name__).info("using msgspec for JSON serialization")

	_msgspec_json = json
except ImportError:
	logging.getLogger(__name__).info("using orjson for JSON serialization")


class JSONSerializer(Serializer):
	__slots__ = ("_msgspec",)

	def __init__(self) -> None:
		self._msgspec = _msgspec_json

	def marshal(self, data: Any) -> bytes:
		if data is None:
			return b""
		if isinstance(data, BaseModel):
			if self._msgspec is not None:
				return self._msgspec.encode(data.model_dump(mode="json"))
			return orjson.dumps(data.model_dump(mode="json"))
		if self._msgspec is not None:
			return self._msgspec.encode(data)
		return orjson.dumps(data)

	@overload
	def unmarshal[T](self, data: bytes, into: type[T]) -> T: ...
	@overload
	def unmarshal[T](self, data: bytes, into: Literal[None] = None) -> Any: ...
	def unmarshal[T](self, data: bytes, into: type[T] | None = None) -> T | Any:
		if not data:
			return None

		target = into if into is not None else self._primary_type
		if isinstance(target, type) and issubclass(target, BaseModel):
			return target.model_validate_json(data)
		if self._msgspec is None:
			return orjson.loads(data) if target is None else orjson.loads(data)
		return (
			self._msgspec.decode(data, type=target)
			if target is not None
			else self._msgspec.decode(data)
		)
