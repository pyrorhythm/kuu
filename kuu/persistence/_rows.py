from __future__ import annotations

import re
from typing import Literal

from msgspec import Struct

_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")

RunStatus = Literal["enqueued", "started", "succeeded", "failed", "retried", "dead"]


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
	args_repr: str | None = None
	kwargs_repr: str | None = None
	started_at: float | None = None
	finished_at: float | None = None
	time_elapsed: float | None = None
	status: RunStatus = "succeeded"
	exc_type: str | None = None
	exc_message: str | None = None
	traceback: str | None = None


class LogRow(Struct, frozen=True):
	message_id: str = ""
	attempt: int = 0
	ts: float = 0.0
	level: int = 0
	logger: str = ""
	message: str = ""
