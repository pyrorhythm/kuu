from __future__ import annotations

from typing import Any, Literal, Mapping, overload

import orjson
from pydantic import BaseModel

from .message import Message


def dump_args(args: BaseModel | dict[str, Any] | None) -> bytes:
    if args is None:
        return b""
    if isinstance(args, BaseModel):
        return orjson.dumps(args.model_dump(mode="json"))
    return orjson.dumps(args)


@overload
def load_args[_BM: BaseModel](
    raw: bytes,
    model: Literal[None] = None,
) -> Mapping[str, Any] | None: ...


@overload
def load_args[_BM: BaseModel](
    raw: bytes,
    model: type[_BM],
) -> _BM | None: ...


def load_args[_BM: BaseModel](
    raw: bytes | None, model: type[_BM] | None = None
) -> Mapping[str, Any] | _BM | None:
    if not raw:
        return None
    data = orjson.loads(raw)
    if model is None:
        return data
    return model.model_validate(data)


def dump_message(msg: Message) -> bytes:
    return orjson.dumps(msg.model_dump(mode="json"))


def load_message(raw: bytes) -> Message:
    return Message.model_validate(orjson.loads(raw))
