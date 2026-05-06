from __future__ import annotations

from typing import Literal

from msgspec import Struct


class Result(Struct, frozen=True):
	status: Literal["ok", "error"]
	value: bytes | None = None
	error: str | None = None
	type: str | None = None
