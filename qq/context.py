from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
	from qq.app import Q
	from qq.message import Message
	from qq.task import Task


type Phase = Literal["enqueue", "process"]


@dataclass
class Context:
	app: Q
	message: Message
	phase: Phase
	task: Task | None = None
	state: dict[str, Any] = field(default_factory=dict)
