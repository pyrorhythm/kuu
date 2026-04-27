from __future__ import annotations

import os
import pickle
import warnings
from typing import Any

from .base import Serializer


class SecurityWarning(Warning): ...


pickle_allowed = os.getenv("QQ_ALLOW_PICKLE", "").strip().lower() == "yes"


_WARN_MSG = (
	"PickleSerializer is not secure and should only be used in trusted environments. "
	"Pickle can execute arbitrary code during deserialization. "
	"Consider using OrjsonSerializer or MsgpackSerializer instead.\n\n"
	"This warning could be turned off by setting envvar QQ_ALLOW_PICKLE=yes."
)


class PickleSerializer(Serializer):
	"""
	Pickle serializer for trusted environments only.

	Pickle can execute arbitrary code during deserialization. Each
	`marshal`/`unmarshal` call emits a `SecurityWarning` unless
	`QQ_ALLOW_PICKLE=yes` is set in the environment.
	"""

	@staticmethod
	def marshal(data: Any) -> bytes:
		if not pickle_allowed:
			warnings.warn(_WARN_MSG, SecurityWarning, stacklevel=2)
		return pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)

	@staticmethod
	def unmarshal(data: bytes, into: type | None = None) -> object:  # type:ignore
		if not pickle_allowed:
			warnings.warn(_WARN_MSG, SecurityWarning, stacklevel=2)
		if not data:
			return None
		return pickle.loads(data)
