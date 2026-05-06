from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any, Literal, Protocol

from msgspec import Struct, field

PROTOCOL_VERSION = 1

EventKind = Literal["enqueued", "succeeded", "failed", "retried", "dead"]
ByeReason = Literal["sigterm", "sigint", "crash", "manual"]


class BrokerInfo(Struct, frozen=True):
	type: str
	key: str


class WorkerSnapshot(Struct, frozen=True):
	pid: int
	alive: bool
	current_task: str | None = None


class JobSnapshot(Struct, frozen=True):
	id: str
	task: str
	next_run: float


class QueueSnapshot(Struct, frozen=True):
	in_flight: int
	depth: int | None = None


class TaskParam(Struct, frozen=True):
	name: str
	annotation: str | None = None
	default: Any = None
	required: bool = True


class TaskInfo(Struct, frozen=True):
	name: str
	queue: str
	max_attempts: int = 5
	timeout: float | None = None
	params: list[TaskParam] = field(default_factory=list)
	has_varargs: bool = False


class Hello(Struct, frozen=True, tag="hello"):
	preset: str
	host: str
	pid: int
	version: str
	started_at: float
	broker: BrokerInfo
	scheduler_enabled: bool
	processes: int
	concurrency: int = 1
	tasks: list[TaskInfo] = field(default_factory=list)


class Event(Struct, frozen=True, tag="event"):
	kind: EventKind
	task: str
	queue: str
	worker_pid: int
	elapsed: float | None = None
	message_id: str | None = None
	attempt: int | None = None
	args_repr: str | None = None
	kwargs_repr: str | None = None
	exc_type: str | None = None
	exc_message: str | None = None
	traceback: str | None = None


class LogRecord(Struct, frozen=True):
	message_id: str
	attempt: int
	level: int
	logger: str
	message: str
	ts: float


class LogBatch(Struct, frozen=True, tag="log_batch"):
	records: list[LogRecord] = field(default_factory=list)


class State(Struct, frozen=True, tag="state"):
	workers: list[WorkerSnapshot] = field(default_factory=list)
	jobs: list[JobSnapshot] = field(default_factory=list)
	queues: dict[str, QueueSnapshot] = field(default_factory=dict)


class Bye(Struct, frozen=True, tag="bye"):
	reason: ByeReason


Body = Hello | Event | State | Bye | LogBatch


class Envelope(Struct, frozen=True):
	v: int
	instance: str
	ts: float
	body: Body


class EventsSink(Protocol):
	def emit(self, envelope: Envelope) -> None: ...


class EventsSource(Protocol):
	def __aiter__(self) -> AsyncIterator[Envelope]: ...


class InstanceInfo(Struct, frozen=True):
	instance_id: str
	hello: Hello
	last_state: State | None
	last_seen: float


class InstanceRegistry(Protocol):
	def ingest(self, envelope: Envelope) -> None: ...

	def get(self, instance_id: str) -> InstanceInfo | None: ...

	def all(self) -> list[InstanceInfo]: ...


__all__ = [
	"PROTOCOL_VERSION",
	"EventKind",
	"ByeReason",
	"BrokerInfo",
	"WorkerSnapshot",
	"JobSnapshot",
	"QueueSnapshot",
	"TaskParam",
	"TaskInfo",
	"Hello",
	"Event",
	"LogBatch",
	"LogRecord",
	"State",
	"Bye",
	"Body",
	"Envelope",
	"EventsSink",
	"EventsSource",
	"InstanceInfo",
	"InstanceRegistry",
]
