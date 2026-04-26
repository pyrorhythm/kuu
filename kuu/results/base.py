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
		Args:
		    serializer: serializer to encode/decode result payloads.
		    marshal_types: when true, persist the python type fqn alongside the
		        payload so `decode` can reconstruct the original type.
		    ttl: how long persisted entries live, in seconds; `None` means no
		        expiry. Applied at write time by `set`/`setnx`.
		    replay: when true, the worker checks the backend before running a
		        task and short-circuits to the cached value on a hit.
		    store_errors: when true, the worker persists terminal failures
		        (final attempt) so callers can observe them via `TaskHandle`.
		"""
		self.serializer = serializer
		self.marshal_types = marshal_types
		self.ttl = ttl
		self.replay = replay
		self.store_errors = store_errors

	def encode(self, value: Any) -> tuple[bytes, str | None]:
		payload = self.serializer.marshal(value) if value is not None else b""
		type_fqn = get_type_fqn(value) if (self.marshal_types and value is not None) else None
		return payload, type_fqn

	def decode(self, result: Result) -> Any:
		if not result.value:
			return None
		into = get_type_from_fqn(result.type) if self.marshal_types else None
		return self.serializer.unmarshal(result.value, into)

	async def connect(self) -> None: ...
	async def close(self) -> None: ...
	async def get(self, key: str) -> Result | None: ...
	async def set(self, key: str, result: Result, ttl: float | None = None) -> None: ...
	async def setnx(self, key: str, result: Result, ttl: float | None = None) -> bool: ...


__all__ = [
	"Result",
	"ResultBackend",
	"Serializer",
	"JSONSerializer",
	"result_key",
	"IDEMPOTENCY_HEADER",
]
