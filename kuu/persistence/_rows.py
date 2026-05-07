from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, overload

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


class LogRow(Struct, frozen=True):
	message_id: str = ""
	attempt: int = 0
	ts: datetime = field(default_factory=utcnow)
	level: int = 0
	logger: str = ""
	message: str = ""
