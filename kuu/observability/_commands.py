from __future__ import annotations

from typing import Any, TypeAlias

from msgspec import Struct, field


class Cmd(Struct, frozen=True): ...


class EnqueueCmd(Cmd, frozen=True, tag="enqueue"):
	request_id: str
	task: str
	args: list[Any] = field(default_factory=list)
	kwargs: dict[str, Any] = field(default_factory=dict)
	queue: str | None = None


class ReplayCmd(Cmd, frozen=True, tag="replay"):
	request_id: str
	replay_of: str
	task: str
	args: list[Any] = field(default_factory=list)
	kwargs: dict[str, Any] = field(default_factory=dict)
	queue: str | None = None


class RetryCmd(Cmd, frozen=True, tag="retry"):
	request_id: str
	run_id: str
	attempt: int
	task: str
	args: list[Any] = field(default_factory=list)
	kwargs: dict[str, Any] = field(default_factory=dict)
	queue: str | None = None


class CancelCmd(Cmd, frozen=True, tag="cancel"):
	request_id: str
	run_id: str


class TriggerJobCmd(Cmd, frozen=True, tag="trigger_job"):
	request_id: str
	job_id: str


class RemoveJobCmd(Cmd, frozen=True, tag="remove_job"):
	request_id: str
	job_id: str


Command: TypeAlias = EnqueueCmd | ReplayCmd | RetryCmd | CancelCmd | TriggerJobCmd | RemoveJobCmd


class CmdResponse(Struct, frozen=True):
	request_id: str
	ok: bool
	error: str | None = None
	run_id: str | None = None


class CommandFrame(Struct, frozen=True):
	v: int
	command: Command


class CommandResponseFrame(Struct, frozen=True):
	v: int
	response: CmdResponse


__all__ = [
	"Cmd",
	"Command",
	"EnqueueCmd",
	"ReplayCmd",
	"RetryCmd",
	"CancelCmd",
	"TriggerJobCmd",
	"RemoveJobCmd",
	"CmdResponse",
	"CommandFrame",
	"CommandResponseFrame",
]
