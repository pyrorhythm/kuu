from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Self, overload

from asyncpg.protocol import Record
from msgspec import Struct, convert, field, to_builtins

from kuu.result import RemoteFailure, sanitize_remote_failure

from kuu._util import utcnow

_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")

RunStatus = Literal["enqueued", "started", "succeeded", "failed", "retried", "dead", "cancelled"]
LogicalRunStatus = Literal[
	"enqueued",
	"running",
	"cancel_requested",
	"succeeded",
	"failed",
	"cancelled",
	"unknown",
]

_FINISH_STATUS: dict[str, RunStatus] = {
	"succeeded": "succeeded",
	"failed": "failed",
	"retried": "retried",
	"dead": "dead",
	"cancelled": "cancelled",
}


def parse_pg_dsn(dsn: str) -> str:
	if dsn.startswith("postgres://"):
		return "postgresql://" + dsn[len("postgres://") :]
	return dsn


@overload
def to_naive(dt: datetime) -> datetime: ...
@overload
def to_naive(dt: None) -> None: ...


def to_naive(dt: datetime | None) -> datetime | None:
	if dt:
		return dt.astimezone(tz=timezone.utc).replace(tzinfo=None)


def validate_table_name(name: str) -> str:
	if not _TABLE_RE.match(name):
		raise ValueError(f"invalid table/schema name {name!r}; must match {_TABLE_RE.pattern}")
	return name


class PendingRun(Struct, frozen=True):
	message_id: str = ""
	attempt: int = 0
	task: str = ""
	queue: str = ""
	instance_id: str = ""
	worker_pid: int = 0
	args: Any = None
	kwargs: Any = None
	headers: Any = None
	input_complete: bool = False
	result_preview: Any = None
	started_at: datetime | None = None
	finished_at: datetime | None = None
	time_elapsed: timedelta | None = None
	status: RunStatus = "succeeded"
	exc_type: str | None = None
	exc_message: str | None = None
	traceback: str | None = None
	failure: RemoteFailure | None = None

	def to_row(self) -> RunRow:
		return RunRow(
			message_id=self.message_id,
			attempt=self.attempt,
			task=self.task,
			queue=self.queue,
			instance_id=self.instance_id,
			worker_pid=self.worker_pid,
			args=self.args,
			kwargs=self.kwargs,
			headers=self.headers,
			input_complete=self.input_complete,
			result_preview=self.result_preview,
			started_at=self.started_at,
			finished_at=self.finished_at,
			time_elapsed=self.time_elapsed,
			status=self.status,
			exc_type=self.exc_type,
			exc_message=self.exc_message,
			traceback=self.traceback,
			failure=self.failure,
		)

	def finish(
		self,
		*,
		kind: str,
		finish_ts: datetime,
		args: Any = None,
		kwargs: Any = None,
		headers: Any = None,
		input_complete: bool = False,
		result_preview: Any = None,
		exc_type: str | None = None,
		exc_message: str | None = None,
		traceback: str | None = None,
		failure: RemoteFailure | None = None,
	) -> PendingRun:
		elapsed = None
		if self.started_at is not None:
			elapsed = finish_ts - self.started_at
		return PendingRun(
			message_id=self.message_id,
			attempt=self.attempt,
			task=self.task,
			queue=self.queue,
			instance_id=self.instance_id,
			worker_pid=self.worker_pid,
			args=args if args is not None else self.args,
			kwargs=kwargs if kwargs is not None else self.kwargs,
			headers=headers if headers is not None else self.headers,
			input_complete=input_complete or self.input_complete,
			result_preview=(result_preview if result_preview is not None else self.result_preview),
			started_at=self.started_at,
			finished_at=finish_ts,
			time_elapsed=elapsed,
			status=_FINISH_STATUS.get(kind, "failed"),
			exc_type=exc_type,
			exc_message=exc_message,
			traceback=traceback,
			failure=failure,
		)


class RunRow(Struct, frozen=True):
	id: int | None = None
	message_id: str = ""
	attempt: int = 0
	task: str = ""
	queue: str = ""
	instance_id: str = ""
	worker_pid: int = 0
	args: Any = None
	kwargs: Any = None
	headers: Any = None
	input_complete: bool = False
	result_preview: Any = None
	started_at: datetime | None = None
	finished_at: datetime | None = None
	time_elapsed: timedelta | None = None
	status: RunStatus = "succeeded"
	exc_type: str | None = None
	exc_message: str | None = None
	traceback: str | None = None
	failure: RemoteFailure | None = None
	state_override: Literal["unknown", "lost"] | None = None

	def astuple(self) -> tuple:
		return (
			self.message_id,
			self.attempt,
			self.task,
			self.queue,
			self.instance_id,
			self.worker_pid,
			self.args,
			self.kwargs,
			self.headers,
			self.input_complete,
			self.result_preview,
			to_naive(self.started_at),
			to_naive(self.finished_at),
			self.time_elapsed,
			self.status,
			self.exc_type,
			self.exc_message,
			self.traceback,
			self.failure,
			self.state_override,
		)

	def asdict(self) -> dict:
		return {
			"id": self.id,
			"message_id": self.message_id,
			"attempt": self.attempt,
			"task": self.task,
			"queue": self.queue,
			"instance_id": self.instance_id,
			"worker_pid": self.worker_pid,
			"args": self.args,
			"kwargs": self.kwargs,
			"headers": self.headers,
			"input_complete": self.input_complete,
			"result_preview": self.result_preview,
			"started_at": self.started_at.isoformat() if self.started_at else None,
			"finished_at": self.finished_at.isoformat() if self.finished_at else None,
			"time_elapsed": self.time_elapsed.total_seconds() if self.time_elapsed else None,
			"status": self.state_override or self.status,
			"recorded_status": self.status,
			"exc_type": self.exc_type,
			"exc_message": self.exc_message,
			"traceback": self.traceback,
			"failure": to_builtins(self.failure) if self.failure is not None else None,
		}

	@classmethod
	def fromrecord(cls, row: Record) -> Self:
		rd = dict(row)
		failure = rd.get("failure")
		if isinstance(failure, dict):
			rd["failure"] = sanitize_remote_failure(convert(failure, type=RemoteFailure))
		rd.update(
			started_at=started.replace(tzinfo=timezone.utc)
			if (started := rd.get("started_at"))
			else None,
			finished_at=finished.replace(tzinfo=timezone.utc)
			if (finished := rd.get("finished_at"))
			else None,
		)

		return RunRow(**rd)


class DashboardStats(Struct, frozen=True):
	totals: dict[str, int]


class LogicalRunRow(Struct, frozen=True):
	message_id: str
	task: str
	queue: str
	instance_id: str
	status: LogicalRunStatus
	created_at: datetime
	updated_at: datetime
	replay_of: str | None = None
	attempt_count: int = 1
	dead_lettered: bool = False

	def astuple(self) -> tuple:
		return (
			self.message_id,
			self.task,
			self.queue,
			self.instance_id,
			self.status,
			to_naive(self.created_at),
			to_naive(self.updated_at),
			self.replay_of,
			self.attempt_count,
			self.dead_lettered,
		)

	def asdict(self) -> dict[str, Any]:
		return {
			"message_id": self.message_id,
			"task": self.task,
			"queue": self.queue,
			"instance_id": self.instance_id,
			"status": self.status,
			"created_at": self.created_at.isoformat(),
			"updated_at": self.updated_at.isoformat(),
			"replay_of": self.replay_of,
			"attempt_count": self.attempt_count,
			"dead_lettered": self.dead_lettered,
		}

	@classmethod
	def fromrecord(cls, row: Record) -> Self:
		rd = dict(row)
		for name in ("created_at", "updated_at"):
			if value := rd.get(name):
				rd[name] = value.replace(tzinfo=timezone.utc)
		return cls(**rd)


class LogRow(Struct, frozen=True):
	id: int | None = None
	message_id: str = ""
	attempt: int = 0
	ts: datetime = field(default_factory=utcnow)
	kind: str = "log"
	seq: int = 0
	level: int = 0
	logger: str = ""
	message: str = ""
	fields: dict[str, Any] = field(default_factory=dict)
	current: float | None = None
	total: float | None = None
	dropped: int = 0

	def astuple(self) -> tuple:
		return (
			self.message_id,
			self.attempt,
			to_naive(self.ts),
			self.kind,
			self.seq,
			self.level,
			self.logger,
			self.message,
			self.fields,
			self.current,
			self.total,
			self.dropped,
		)

	def asdict(self) -> dict:
		return {
			"cursor": self.id,
			"message_id": self.message_id,
			"attempt": self.attempt,
			"ts": self.ts.isoformat() if self.ts else None,
			"kind": self.kind,
			"seq": self.seq,
			"level": self.level,
			"logger": self.logger,
			"message": self.message,
			"fields": self.fields,
			"current": self.current,
			"total": self.total,
			"dropped": self.dropped,
		}

	@classmethod
	def fromrecord(cls, row: Record) -> Self:
		rd = dict(row)
		rd.update(
			ts=ts.replace(tzinfo=timezone.utc) if (ts := rd.get("ts")) else None,
			fields=rd.get("fields") or {},
		)
		return LogRow(**rd)
