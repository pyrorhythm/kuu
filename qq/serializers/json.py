from __future__ import annotations

from typing import Any, Literal, Self, overload

import orjson
from pydantic import BaseModel


class OrjsonSerializer:
    _primary_type: type | None = None

    @classmethod
    def with_type(cls, t: type) -> Self:
        inst = cls()
        inst._primary_type = t
        return inst

    def serialize(self, data: Any) -> bytes:
        if data is None:
            return b""
        if isinstance(data, BaseModel):
            return orjson.dumps(data.model_dump(mode="json"))
        return orjson.dumps(data)

    @overload
    def deserialize[T](self, data: bytes, into: type[T]) -> T: ...
    @overload
    def deserialize[T](self, data: bytes, into: Literal[None] = None) -> object: ...

    def deserialize[T](self, data: bytes, into: type[T] | None = None) -> T | object:
        target = into if into is not None else self._primary_type
        if not data:
            return None
        raw = orjson.loads(data)
        if target is None:
            return raw
        if isinstance(target, type) and issubclass(target, BaseModel):
            return target.model_validate(raw)
        return raw
