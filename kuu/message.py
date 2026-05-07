from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any
from uuid import UUID, uuid4

from msgspec import Meta, Struct, field

from kuu._util import utcnow


class Payload(Struct, frozen=True):
	args: tuple[Any, ...] = field(default_factory=tuple)
	kwargs: dict[str, Any] = field(default_factory=dict)


class Message(Struct, frozen=True):
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

	task: Annotated[str, Meta(min_length=1)]
	queue: Annotated[str, Meta(min_length=1)]
	id: UUID = field(default_factory=uuid4)
	payload: Payload = field(default_factory=Payload)
	headers: dict[str, str] = field(default_factory=dict)
	attempt: Annotated[int, Meta(ge=0)] = 0
	max_attempts: Annotated[int, Meta(ge=1)] = 5
	not_before: Annotated[datetime, Meta(tz=True)] | None = None
	enqueued_at: Annotated[datetime, Meta(tz=True)] = field(default_factory=utcnow)
