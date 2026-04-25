from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated, Self
from uuid import UUID, uuid4

import orjson
from pydantic import (
	AwareDatetime,
	BaseModel,
	ConfigDict,
	Field,
	FutureDatetime,
)


class Message(BaseModel):
	model_config = ConfigDict(frozen=True)

	id: Annotated[UUID, Field(default_factory=uuid4)]
	task: Annotated[str, Field(min_length=1)]
	queue: Annotated[str, Field(min_length=1)]
	payload: bytes
	args_type: Annotated[str | None, Field(default=None)]
	result_type: Annotated[str | None, Field(default=None)]
	headers: Annotated[dict[str, str], Field(default_factory=dict)]
	attempt: Annotated[int, Field(default=0, ge=0)]
	max_attempts: Annotated[int, Field(default=5, gt=0)]
	not_before: Annotated[FutureDatetime | None, Field(default=None)]
	enqueued_at: Annotated[AwareDatetime, Field(default_factory=lambda: datetime.now(timezone.utc))]

	def marshal(self) -> bytes:
		return orjson.dumps(self.model_dump(mode="json"))

	@classmethod
	def unmarshal(self, data: str | bytes | bytearray) -> Self:
		return self.model_validate(orjson.loads(data))
