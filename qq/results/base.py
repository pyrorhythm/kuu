from __future__ import annotations

from typing import Annotated, Literal, Protocol, Self

import orjson
from pydantic import BaseModel, ConfigDict, Field


class Result(BaseModel):
    model_config = ConfigDict(frozen=True)

    status: Literal["ok", "error"]
    value: Annotated[bytes | None, Field(default=None)]
    error: Annotated[str | None, Field(default=None)]

    def marshal(self) -> bytes:
        return orjson.dumps(self.model_dump(mode="json"))

    @classmethod
    def unmarshal(cls, data: bytes | str | bytearray) -> Self:
        return cls.model_validate(orjson.loads(data))


class ResultBackend(Protocol):
    async def connect(self) -> None: ...
    async def close(self) -> None: ...

    async def get(self, key: str) -> Result | None: ...
    async def set(self, key: str, result: Result, ttl: float | None = None) -> None: ...
    async def setnx(self, key: str, result: Result, ttl: float | None = None) -> bool: ...
