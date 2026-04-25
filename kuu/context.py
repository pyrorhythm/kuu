from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
	from kuu.app import Kuu
	from kuu.message import Message
	from kuu.task import Task


type Phase = Literal["enqueue", "process"]


@dataclass
class Context:
	app: Kuu
	message: Message
	phase: Phase
	task: Task | None = None
	state: dict[str, Any] = field(default_factory=dict)
