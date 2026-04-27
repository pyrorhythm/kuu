from __future__ import annotations

import logging
from typing import Any, Literal, overload

import orjson
from pydantic import BaseModel

from .base import Serializer

msgspec_json = None

try:
    from msgspec import json

    logging.getLogger(__name__).info("using msgspec for JSON serialization")

    msgspec_json = json
except ImportError:
    logging.getLogger(__name__).info("using orjson for JSON serialization")
    pass


class JSONSerializer(Serializer):
    def marshal(self, data: Any) -> bytes:
        if data is None:
            return b""
        if isinstance(data, BaseModel):
            if msgspec_json is not None:
                return msgspec_json.encode(data.model_dump(mode="json"))
            return orjson.dumps(data.model_dump(mode="json"))
        if msgspec_json is not None:
            return msgspec_json.encode(data)
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
        if msgspec_json is None:
            return orjson.loads(data) if target is None else orjson.loads(data)
        return (
            msgspec_json.decode(data, type=target)
            if target is not None
            else msgspec_json.decode(data)
        )
