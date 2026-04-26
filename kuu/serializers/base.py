from __future__ import annotations

from typing import Any, Literal, Protocol, overload


class Serializer(Protocol):
	"""
	Protocol for data serializers

	Converts between python objects and bytes
	"""

	_primary_type: type | None = None

	@classmethod
	def with_type(cls, t: type) -> Serializer:
		"""
		Create serializer instance bound to primary type

		Args:
			t: target type for deserialization

		Returns:
			Configured serializer instance
		"""
		inst = cls()
		inst._primary_type = t
		return inst

	def marshal(self, data: Any) -> bytes: ...

	@overload
	def unmarshal[T](self, data: bytes, into: type[T]) -> T: ...
	@overload
	def unmarshal[T](self, data: bytes, into: Literal[None] = None) -> Any: ...
	def unmarshal[T](self, data: bytes, into: type[T] | None = None) -> T | Any: ...
