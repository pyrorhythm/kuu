from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Literal, Protocol

PROTOCOL_VERSION = 1


EventKind = Literal["enqueued", "succeeded", "failed", "retried", "dead"]
ByeReason = Literal["sigterm", "sigint", "crash", "manual"]


@dataclass(frozen=True, slots=True)
class BrokerInfo:
	"""broker identity as advertised by a producer

	``key`` is a stable fingerprint computed via
	:func:`kuu.observability.broker_key`; two producers with the same
	connection identity share the same key, allowing the dashboard to
	dedup shared buses
	"""

	type: str
	key: str


@dataclass(frozen=True, slots=True)
class WorkerSnapshot:
	pid: int
	alive: bool
	current_task: str | None = None


@dataclass(frozen=True, slots=True)
class JobSnapshot:
	id: str
	task: str
	next_run: float


@dataclass(frozen=True, slots=True)
class QueueSnapshot:
	"""per-queue state

	``depth`` is ``None`` when the supervisor cannot cheaply query the
	broker; ``in_flight`` is always known locally (count of messages
	currently being processed by this instance's workers)
	"""

	in_flight: int
	depth: int | None = None


# === body sum type


@dataclass(frozen=True, slots=True)
class Body:
	"""sealed base for envelope bodies: use ``match`` to dispatch"""


@dataclass(frozen=True, slots=True)
class Hello(Body):
	"""first envelope from any producer; identifies the instance.

	re-sent on reconnect with the same ``instance_id`` of the originating
	envelope, so the consumer treats it as the same logical instance
	"""

	preset: str
	host: str
	pid: int
	version: str
	started_at: float
	broker: BrokerInfo
	scheduler_enabled: bool
	processes: int


@dataclass(frozen=True, slots=True)
class Event(Body):
	"""single task lifecycle event"""

	kind: EventKind
	task: str
	queue: str
	worker_pid: int
	elapsed: float | None = None


@dataclass(frozen=True, slots=True)
class State(Body):
	"""periodic snapshot; sent every ``state_interval`` seconds

	a consumer treats an instance as dead after
	``2 * state_interval`` seconds without a fresh ``State``
	"""

	workers: list[WorkerSnapshot] = field(default_factory=list)
	jobs: list[JobSnapshot] = field(default_factory=list)
	queues: dict[str, QueueSnapshot] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Bye(Body):
	"""graceful shutdown notification"""

	reason: ByeReason


# === envelope


@dataclass(frozen=True, slots=True)
class Envelope:
	"""wire-level frame

	the body type is the tag; ``match envelope.body`` to dispatch
	"""

	v: int
	instance: str
	ts: float
	body: Body


# === consumer / producer interfaces


class EventsSink(Protocol):
	def emit(self, envelope: Envelope) -> None: ...


class EventsSource(Protocol):
	def __aiter__(self) -> AsyncIterator[Envelope]: ...


@dataclass(frozen=True, slots=True)
class InstanceInfo:
	instance_id: str
	hello: Hello
	last_state: State | None
	last_seen: float


class InstanceRegistry(Protocol):
	def ingest(self, envelope: Envelope) -> None:
		"""Update the roster from any envelope."""

	def get(self, instance_id: str) -> InstanceInfo | None: ...

	def all(self) -> list[InstanceInfo]: ...
