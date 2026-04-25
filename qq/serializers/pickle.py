from __future__ import annotations

import os
import pickle
import warnings
from typing import Any


class SecurityWarning(Warning):
    pass


pickle_allowed = os.getenv("QQ_ALLOW_PICKLE", "").strip().lower() == "yes"


_WARN_MSG = (
    "PickleSerializer is not secure and should only be used in trusted environments. "
    "Pickle can execute arbitrary code during deserialization. "
    "Consider using OrjsonSerializer or MsgpackSerializer instead.\n\n"
    "This warning could be turned off by setting envvar QQ_ALLOW_PICKLE=yes."
)


class PickleSerializer:
    @staticmethod
    def serialize(data: Any) -> bytes:
        if not pickle_allowed:
            warnings.warn(_WARN_MSG, SecurityWarning, stacklevel=2)
        return pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def deserialize(data: bytes, into: type | None = None) -> object:
        if not pickle_allowed:
            warnings.warn(_WARN_MSG, SecurityWarning, stacklevel=2)
        if not data:
            return None
        return pickle.loads(data)
