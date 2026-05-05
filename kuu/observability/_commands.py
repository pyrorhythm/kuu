"""control-plane -> supervisor RPC

separate from the observability envelope stream which is one-way; commands
travel via dedicated mp.Queue pairs (one inbound queue per supervisor,
one shared response queue back to the parent)

dispatch via ``match`` on the :class:`Command` sum type; responses are
correlated via ``request_id``
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class Cmd:
	"""sealed base for cross-process RPC commands"""


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
