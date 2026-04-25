from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pydantic_core import ArgsKwargs

from .message import Message

if TYPE_CHECKING:
	from .app import Q
	from .brokers.base import Delivery
	from .task import Task


@dataclass
class Context:
	app: Q
	message: Message
	phase: str
	task: Task | None = None
	delivery: Delivery | None = None
	args: ArgsKwargs = ArgsKwargs(())
	result: Any = None
	exc: BaseException | None = None
	state: dict[str, Any] = field(default_factory=dict)
