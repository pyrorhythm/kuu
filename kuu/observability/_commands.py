from __future__ import annotations

from typing import Any

from msgspec import Struct, field


class Cmd(Struct, frozen=True): ...


class EnqueueCmd(Cmd, frozen=True, tag="enqueue"):
	request_id: str
	task: str
	args: list[Any] = field(default_factory=list)
	kwargs: dict[str, Any] = field(default_factory=dict)


class TriggerJobCmd(Cmd, frozen=True, tag="trigger_job"):
	request_id: str
	job_id: str


class RemoveJobCmd(Cmd, frozen=True, tag="remove_job"):
	request_id: str
	job_id: str


class CmdResponse(Struct, frozen=True):
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
