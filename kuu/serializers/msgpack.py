from __future__ import annotations

from typing import Any, Literal, overload

from msgspec import msgpack

from .base import Serializer


class MsgpackSerializer(Serializer):
	def marshal(self, data: Any) -> bytes:
		if data is None:
			return b""
		return msgpack.encode(data)

	@overload
	def unmarshal[T](self, data: bytes, into: type[T]) -> T: ...
	@overload
	def unmarshal[T](self, data: bytes, into: Literal[None] = None) -> Any: ...
	def unmarshal[T](self, data: bytes, into: type[T] | None = None) -> T | Any:
		if not data:
			return None

		target = into if into is not None else self._primary_type
		return msgpack.decode(data, type=target)
