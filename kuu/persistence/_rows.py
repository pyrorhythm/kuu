from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Self, overload

from asyncpg.protocol import Record
from msgspec import Struct, field

from kuu._util import utcnow

_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")

RunStatus = Literal["enqueued", "started", "succeeded", "failed", "retried", "dead"]


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
	started_at: datetime | None = None
	finished_at: datetime | None = None
	time_elapsed: timedelta | None = None
	status: RunStatus = "succeeded"
	exc_type: str | None = None
	exc_message: str | None = None
	traceback: str | None = None

	def astuple(self) -> tuple:
		return (
			self.message_id,
			self.attempt,
			self.task,
			self.queue,
			self.instance_id,
			self.worker_pid,
			self.args.decode()
			if isinstance(self.args, bytes)
			else str(self.args)
			if self.args
			else None,
			self.kwargs.decode()
			if isinstance(self.kwargs, bytes)
			else str(self.kwargs)
			if self.kwargs
			else None,
			to_naive(self.started_at),
			to_naive(self.finished_at),
			self.time_elapsed,
			self.status,
			self.exc_type,
			self.exc_message,
			self.traceback,
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
			"started_at": self.started_at.isoformat() if self.started_at else None,
			"finished_at": self.finished_at.isoformat() if self.finished_at else None,
			"time_elapsed": self.time_elapsed.total_seconds() if self.time_elapsed else None,
			"status": self.status,
			"exc_type": self.exc_type,
			"exc_message": self.exc_message,
			"traceback": self.traceback,
		}

	@classmethod
	def fromrecord(cls, row: Record) -> Self:
		rd = dict(row)
		rd.update(
			started_at=started.replace(tzinfo=timezone.utc)
			if (started := rd.get("started_at"))
			else None,
			finished_at=finished.replace(tzinfo=timezone.utc)
			if (finished := rd.get("finished_at"))
			else None,
		)

		return RunRow(**rd)


class LogRow(Struct, frozen=True):
	message_id: str = ""
	attempt: int = 0
	ts: datetime = field(default_factory=utcnow)
	level: int = 0
	logger: str = ""
	message: str = ""

	def astuple(self) -> tuple:
		return (
			self.message_id,
			self.attempt,
			to_naive(self.ts),
			self.level,
			self.logger,
			self.message,
		)

	def asdict(self) -> dict:
		return {
			"message_id": self.message_id,
			"attempt": self.attempt,
			"ts": self.ts.isoformat() if self.ts else None,
			"level": self.level,
			"logger": self.logger,
			"message": self.message,
		}

	@classmethod
	def fromrecord(cls, row: Record) -> Self:
		rd = dict(row)
		rd.update(
			ts=ts.replace(tzinfo=timezone.utc) if (ts := rd.get("ts")) else None,
		)

		return LogRow(**rd)
