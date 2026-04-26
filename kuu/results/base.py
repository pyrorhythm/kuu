from __future__ import annotations

from typing import Any, Protocol

from .._import import get_type_fqn, get_type_from_fqn
from ..message import Message
from ..result import Result
from ..serializers import (
	JSONSerializer,
	Serializer,
)

IDEMPOTENCY_HEADER = "idempotency_key"


def result_key(msg: Message) -> str:
	return msg.headers.get(IDEMPOTENCY_HEADER) or str(msg.id)


class ResultBackend(Protocol):
	"""
	Protocol for result backends

	Defines serialization; type marshalling; TTL; replay; and error storage
	"""

	serializer: Serializer
	marshal_types: bool
	ttl: float | None
	replay: bool
	store_errors: bool

	def __init__(
		self,
		*,
		serializer: Serializer,
		marshal_types: bool = True,
		ttl: float | None = 86400,
		replay: bool = True,
		store_errors: bool = True,
	) -> None:
		"""
		Configure backend options

		Args:
			serializer: serializer to encode/decode result payloads
			marshal_types: when true; persist python type FQN alongside payload so decode can reconstruct original type
			ttl: how long persisted entries live in seconds; None means no expiry
			replay: when true; worker checks backend before running task and short-circuits to cached value on hit
			store_errors: when true; worker persists terminal failures so callers can observe them via TaskHandle
		"""
		self.serializer = serializer
		self.marshal_types = marshal_types
		self.ttl = ttl
		self.replay = replay
		self.store_errors = store_errors

	def encode(self, value: Any) -> tuple[bytes, str | None]:
		"""
		Encode value into serialized payload and optional type FQN

		Args:
			value: object to encode

		Returns:
			Tuple of payload bytes and optional fully-qualified type name
		"""
		payload = self.serializer.marshal(value) if value is not None else b""
		type_fqn = get_type_fqn(value) if (self.marshal_types and value is not None) else None
		return payload, type_fqn

	def decode(self, result: Result) -> Any:
		"""
		Decode result payload back into python object

		Args:
			result: result containing payload and optional type info

		Returns:
			Reconstructed python object; or None if value is empty
		"""
		if not result.value:
			return None
		into = get_type_from_fqn(result.type) if self.marshal_types else None
		return self.serializer.unmarshal(result.value, into)

	async def connect(self) -> None:
		"""Initialize connection to results backend"""

	async def close(self) -> None:
		"""Close connection to results backend"""

	async def get(self, key: str) -> Result | None:
		"""
		Fetch result by key

		Args:
			key: result cache key

		Returns:
			Deserialized result; or None if key absent
		"""

	async def set(self, key: str, result: Result, ttl: float | None = None) -> None:
		"""
		Store serialized result with optional expiry

		Args:
			key: result cache key
			result: result object to serialize and store
			ttl: expiry in seconds
		"""

	async def setnx(self, key: str, result: Result, ttl: float | None = None) -> bool:
		"""
		Store serialized result only if key is absent

		Args:
			key: result cache key
			result: result object to serialize and store
			ttl: expiry in seconds

		Returns:
			True if key was set; False otherwise
		"""


__all__ = [
	"Result",
	"ResultBackend",
	"Serializer",
	"JSONSerializer",
	"result_key",
	"IDEMPOTENCY_HEADER",
]
