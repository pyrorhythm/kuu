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
	"""
	Task message sent through the broker

	Attributes:
	        id: unique message identifier
	        task: registered name of the task to run
	        queue: queue the message belongs to
	        payload: task arguments
	        headers: custom string key-value pairs
	        attempt: current retry attempt; starting at 0
	        max_attempts: maximum allowed attempts before failure
	        not_before: earliest UTC datetime the task may execute
	        enqueued_at: UTC datetime when the message was enqueued
	"""

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
