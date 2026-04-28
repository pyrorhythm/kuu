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
	Protocol for result backends.

	Defines serialization, type marshalling, TTL, replay and error storage.
	Subclasses should accept `serializer`, `marshal_types`, `ttl`, `replay`,
	and `store_errors` keyword arguments.
	"""

	serializer: Serializer
	marshal_types: bool
	ttl: float | None
	replay: bool
	store_errors: bool

	def __init__(
		self,
		serializer: Serializer,
		marshal_types: bool,
		ttl: float | None,
		replay: bool,
		store_errors: bool,
	) -> None:
		self.serializer = serializer
		self.marshal_types = marshal_types
		self.ttl = ttl
		self.replay = replay
		self.store_errors = store_errors

	def encode(self, value: Any) -> tuple[bytes, str | None]:
		"""
		Encode `value` into a serialized payload and an optional type FQN.

		Returns `(payload, type_fqn)`. `type_fqn` is `None` when `value`
		is `None` or when `marshal_types` is disabled.
		"""
		payload = self.serializer.marshal(value) if value is not None else b""
		type_fqn = get_type_fqn(value) if (self.marshal_types and value is not None) else None
		return payload, type_fqn

	def decode(self, result: Result) -> Any:
		"""
		Decode a stored `Result` back into the original python object.

		Returns `None` if the stored value is empty.
		"""
		if not result.value:
			return None
		into = get_type_from_fqn(result.type) if self.marshal_types else None
		return self.serializer.unmarshal(result.value, into)

	async def connect(self) -> None:
		"""Open the backend connection."""

	async def close(self) -> None:
		"""Close the backend connection."""

	async def get(self, key: str) -> Result | None:
		"""Fetch a result by `key`. Returns `None` if absent."""

	async def set(self, key: str, result: Result, ttl: float | None = None) -> None:
		"""Store `result` at `key`, optionally expiring after `ttl` seconds."""

	async def setnx(self, key: str, result: Result, ttl: float | None = None) -> bool:
		"""
		Store `result` at `key` only if the key is absent.

		Returns `True` on a successful write, `False` if the key already existed.
		"""


__all__ = [
	"Result",
	"ResultBackend",
	"Serializer",
	"JSONSerializer",
	"result_key",
	"IDEMPOTENCY_HEADER",
]
