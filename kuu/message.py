from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated, Any
from uuid import UUID, uuid4

from pydantic import (
	AwareDatetime,
	BaseModel,
	ConfigDict,
	Field,
	FutureDatetime,
)


class Payload(BaseModel):
	model_config = ConfigDict(frozen=True)

	args: tuple[Any, ...] = ()
	kwargs: dict[str, Any] = Field(default_factory=dict)


class Message(BaseModel):
	model_config = ConfigDict(frozen=True)

	id: Annotated[UUID, Field(default_factory=uuid4)]
	task: Annotated[str, Field(min_length=1)]
	queue: Annotated[str, Field(min_length=1)]
	payload: Payload = Field(default_factory=Payload)
	headers: Annotated[dict[str, str], Field(default_factory=dict)]
	attempt: Annotated[int, Field(default=0, ge=0)]
	max_attempts: Annotated[int, Field(default=5, gt=0)]
	not_before: Annotated[FutureDatetime | None, Field(default=None)]
	enqueued_at: Annotated[AwareDatetime, Field(default_factory=lambda: datetime.now(timezone.utc))]
