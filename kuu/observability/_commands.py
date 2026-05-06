from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class Cmd: ...


@dataclass(frozen=True, slots=True)
class EnqueueCmd(Cmd):
	"""enqueue a task on the target supervisor's broker"""

	request_id: str
	task: str
	args: list[Any] = field(default_factory=list)
	kwargs: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TriggerJobCmd(Cmd):
	"""enqueue a scheduled job once, by id"""

	request_id: str
	job_id: str


@dataclass(frozen=True, slots=True)
class RemoveJobCmd(Cmd):
	"""remove a scheduled job by id from the target supervisor's scheduler"""

	request_id: str
	job_id: str


@dataclass(frozen=True, slots=True)
class CmdResponse:
	"""response paired to a command by ``request_id``"""

	request_id: str
	ok: bool
	error: str | None = None


__all__ = [
	"Cmd",
	"EnqueueCmd",
	"TriggerJobCmd",
	"RemoveJobCmd",
	"CmdResponse",
]
