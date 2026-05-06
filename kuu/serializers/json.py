from __future__ import annotations

from typing import Any, Literal, overload

from msgspec import json

from .base import Serializer


class JSONSerializer(Serializer):
	__slots__ = ()

	def marshal(self, data: Any) -> bytes:
		if data is None:
			return b""
		return json.encode(data)

	@overload
	def unmarshal[T](self, data: bytes, into: type[T]) -> T: ...
	@overload
	def unmarshal[T](self, data: bytes, into: Literal[None] = None) -> Any: ...
	def unmarshal[T](self, data: bytes, into: type[T] | None = None) -> T | Any:
		if not data:
			return None

		target = into if into is not None else self._primary_type
		if target is not None:
			return json.decode(data, type=target)
		return json.decode(data)
